"""Главный скрипт для сбора и структурирования данных из Confluence.

Этапы (streaming-пайплайн, чтобы не упираться в память на больших порталах):
  1) Получаем ID страниц (все или только заданные)
  2) Асинхронно скачиваем страницы с ограничением конкурентности
  3) Парсим HTML и формируем логические блоки
  4) Формируем чанки для семантического поиска
  5) Сохраняем результаты через ResultSink (по умолчанию JSON)

Почему так:
- Раньше было: "скачать ВСЁ" -> "обработать ВСЁ" -> "записать".
  Это хорошо для маленьких порталов, но на больших быстро превращается в OOM.
- Теперь это конвейер: fetch -> parse -> chunk -> write.

Важно для будущего:
- Сохранение вынесено в слой sink (storage/*).
  Позже можно без боли заменить JsonStreamSink на DbSink,
  не переписывая основную логику пайплайна.
"""

from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from config.settings import settings
from utils.logger import logger
from utils.timer import timer
from confluence import ConfluenceAPIClient, ConfluencePage, Chunk, ContentBlock
from parser import parse_page_content
from chunking import create_chunks_from_page
from storage import JsonStreamSink, SqliteSink, CompositeSink, ResultSink


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------

def _get_tqdm():
    """Возвращает tqdm если включён прогресс-бар, иначе None."""
    if not settings.SHOW_PROGRESS_BAR:
        return None
    try:
        from tqdm import tqdm
        return tqdm
    except ImportError:
        logger.warning("tqdm not installed — progress bar disabled")
        return None


# ---------------------------------------------------------------------------
# Обработка одной страницы (sync)
# ---------------------------------------------------------------------------

def process_page(page: ConfluencePage) -> tuple[list[Chunk], list[ContentBlock]]:
    """Парсинг + чанкование одной страницы.

    Вынесено в sync-функцию, чтобы вызывать её в asyncio.to_thread.
    """
    with timer.measure(f"page_{page.id}"):
        blocks, headings = parse_page_content(page.body_html, page.id)

        if not blocks:
            logger.warning(f"No content blocks in page {page.id} ({page.title})")
            return [], []

        chunks = create_chunks_from_page(
            blocks=blocks,
            headings=headings,
            page_id=page.id,
            page_title=page.title,
            space_key=page.space_key,
            page_version=page.version,
            last_modified=page.last_modified,
            page_url=page.url,
        )

        return chunks, blocks


# ---------------------------------------------------------------------------
# Модель результата для очереди writer'а
# ---------------------------------------------------------------------------

@dataclass
class PageResult:
    page: ConfluencePage
    chunks: list[Chunk]
    blocks: list[ContentBlock]
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Pipeline tasks
# ---------------------------------------------------------------------------

async def produce_page_ids(
    client: ConfluenceAPIClient,
    id_queue: asyncio.Queue[Optional[str]],
    fetch_workers: int,
    stats: dict,
) -> None:
    """Производитель ID страниц.

    Если CONFLUENCE_PAGE_ID пустой — обходим весь портал, иначе берём список из env.

    Важно:
    - Мы не копим все ID в памяти, если это не нужно.
    - После завершения отправляем sentinel=None для каждого fetch-worker.
    """
    try:
        if settings.CONFLUENCE_PAGE_IDS:
            logger.info(f"Using explicit page IDs: {len(settings.CONFLUENCE_PAGE_IDS)}")
            for pid in settings.CONFLUENCE_PAGE_IDS:
                await id_queue.put(pid)
                stats["page_ids"] += 1
        else:
            logger.info("No specific pages requested — streaming all page IDs…")
            async for pid in client.iter_all_page_ids():
                await id_queue.put(pid)
                stats["page_ids"] += 1
    finally:
        # Сообщаем fetch-воркерам, что ID больше не будет.
        for _ in range(fetch_workers):
            await id_queue.put(None)


async def fetch_worker(
    name: str,
    client: ConfluenceAPIClient,
    id_queue: asyncio.Queue[Optional[str]],
    page_queue: asyncio.Queue[ConfluencePage],
    stats: dict,
) -> None:
    """Скачивает страницы по ID и кладёт их в очередь страниц."""
    while True:
        pid = await id_queue.get()
        try:
            if pid is None:
                return

            page = await client.get_page(pid)
            if page is None:
                stats["pages_fetch_failed"] += 1
                continue

            await page_queue.put(page)
            stats["pages_fetched"] += 1
        except Exception as exc:
            stats["pages_fetch_failed"] += 1
            logger.error(f"[{name}] Error fetching page {pid}: {exc!r}")
        finally:
            id_queue.task_done()


async def process_worker(
    name: str,
    page_queue: asyncio.Queue[Optional[ConfluencePage]],
    result_queue: asyncio.Queue[PageResult],
    stats: dict,
) -> None:
    """Парсит HTML и формирует чанки.

    Это CPU-bound стадия, поэтому внутри используем asyncio.to_thread.
    Количество параллельных process_worker контролируется настройкой
    MAX_CONCURRENT_PARSING.
    """
    while True:
        page = await page_queue.get()
        try:
            if page is None:
                return

            try:
                chunks, blocks = await asyncio.to_thread(process_page, page)
                await result_queue.put(PageResult(page=page, chunks=chunks, blocks=blocks))
            except Exception as exc:
                stats["pages_process_failed"] += 1
                logger.error(f"[{name}] Error processing page {page.id}: {exc!r}")
                await result_queue.put(PageResult(page=page, chunks=[], blocks=[], error=str(exc)))

        finally:
            page_queue.task_done()


async def writer_task(
    sink: ResultSink,
    result_queue: asyncio.Queue[Optional[PageResult]],
    stats: dict,
    pbar,
) -> None:
    """Единственный писатель результата.

    Почему writer один:
    - Для JSON-файла (как и для многих БД сценариев) удобно иметь единую точку записи.
    - Если завтра sink будет DB writer — можно оставить один writer или сделать батчи.
    """
    while True:
        item = await result_queue.get()
        try:
            if item is None:
                return

            stats["pages_processed"] += 1

            if item.error:
                stats["pages_with_errors"] += 1
            else:
                await sink.write_page(item.page, item.blocks, item.chunks)
                stats["chunks_written"] += len(item.chunks)
                if settings.INCLUDE_BLOCKS_IN_OUTPUT and settings.OUTPUT_WRITE_JSON:
                    stats["blocks_written"] += len(item.blocks)

            # Обновление прогресс-бара — только тут, чтобы отражать завершённую страницу.
            if pbar is not None:
                pbar.update(1)
                # Стараемся не спамить огромными строками.
                title = item.page.title[:30]
                pbar.set_postfix(
                    pages=stats["pages_processed"],
                    chunks=stats["chunks_written"],
                    last=title,
                )

        except Exception as exc:
            stats["pages_write_failed"] += 1
            logger.error(f"Writer error: {exc!r}")
        finally:
            result_queue.task_done()


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

async def main() -> None:
    print("\n" + "=" * 80)
    print("CONFLUENCE SEMANTIC SEARCH — DATA PREPARATION")
    print("=" * 80 + "\n")

    # Валидация настроек
    try:
        settings.validate()
        settings.display()
    except ValueError as exc:
        logger.error(f"Configuration error: {exc}")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Output sinks
    # ------------------------------------------------------------------
    #
    # По умолчанию результат пишется в JSON (как и раньше).
    # Дополнительно можно включить запись чанков в SQLite.
    #
    # Управляется через env:
    #   OUTPUT_WRITE_JSON=true|false      (по умолчанию: true)
    #   OUTPUT_WRITE_SQLITE=true|false    (по умолчанию: false)
    #
    # Если включены оба — используем CompositeSink и пишем одновременно.
    enabled_sinks: list[ResultSink] = []

    if settings.OUTPUT_WRITE_JSON:
        enabled_sinks.append(
            JsonStreamSink(
                output_dir=settings.OUTPUT_DIR,
                include_blocks=settings.INCLUDE_BLOCKS_IN_OUTPUT,
            )
        )

    if settings.OUTPUT_WRITE_SQLITE:
        enabled_sinks.append(
            SqliteSink(
                output_dir=settings.OUTPUT_DIR,
                db_filename=settings.SQLITE_DB_FILENAME,
                table_name=settings.SQLITE_TABLE_NAME,
                payload_field=settings.SQLITE_PAYLOAD_FIELD,
            )
        )

    if not enabled_sinks:
        # Требование: если выключено и JSON, и SQLite — предупреждение и остановка.
        logger.warning(
            "No output enabled: both OUTPUT_WRITE_JSON and OUTPUT_WRITE_SQLITE are false. "
            "Nothing to save — stopping."
        )
        sys.exit(1)

    sink: ResultSink = enabled_sinks[0] if len(enabled_sinks) == 1 else CompositeSink(enabled_sinks)

    await sink.open()

    # Статистика пайплайна (инкременты без await -> безопасно в asyncio).
    stats = {
        "page_ids": 0,
        "pages_fetched": 0,
        "pages_fetch_failed": 0,
        "pages_processed": 0,
        "pages_process_failed": 0,
        "pages_with_errors": 0,
        "pages_write_failed": 0,
        "chunks_written": 0,
        "blocks_written": 0,
    }

    # Очереди пайплайна
    id_queue: asyncio.Queue[Optional[str]] = asyncio.Queue(maxsize=settings.PAGE_ID_QUEUE_SIZE)
    page_queue: asyncio.Queue[Optional[ConfluencePage]] = asyncio.Queue(maxsize=settings.PAGE_QUEUE_SIZE)
    result_queue: asyncio.Queue[Optional[PageResult]] = asyncio.Queue(maxsize=settings.RESULT_QUEUE_SIZE)

    tqdm_cls = _get_tqdm()
    pbar = tqdm_cls(desc="Processing pages", unit="page") if tqdm_cls else None

    started_at = datetime.now()

    try:
        async with ConfluenceAPIClient(
            base_url=settings.CONFLUENCE_BASE_URL,
            auth_token=settings.CONFLUENCE_AUTH_TOKEN,
            max_concurrent=settings.MAX_CONCURRENT_REQUESTS,
            timeout=settings.REQUEST_TIMEOUT,
            max_retries=settings.MAX_RETRIES,
        ) as client:

            # 1) producer
            fetch_workers = max(1, settings.MAX_CONCURRENT_REQUESTS)
            processing_workers = max(1, settings.MAX_CONCURRENT_PARSING)

            producer = asyncio.create_task(
                produce_page_ids(client, id_queue, fetch_workers, stats)
            )

            # 2) fetchers
            fetchers = [
                asyncio.create_task(
                    fetch_worker(f"fetch-{i}", client, id_queue, page_queue, stats)
                )
                for i in range(fetch_workers)
            ]

            # 3) processors
            processors = [
                asyncio.create_task(
                    process_worker(f"proc-{i}", page_queue, result_queue, stats)
                )
                for i in range(processing_workers)
            ]

            # 4) writer
            writer = asyncio.create_task(writer_task(sink, result_queue, stats, pbar))

            # Дожидаемся producer, затем полного "высасывания" очереди ID.
            await producer
            await id_queue.join()

            # Fetch-воркеры завершаются сами после получения sentinel.
            await asyncio.gather(*fetchers)

            # Сообщаем процессорам, что страниц больше не будет.
            for _ in range(processing_workers):
                await page_queue.put(None)

            await page_queue.join()
            await asyncio.gather(*processors)

            # Сообщаем writer'у, что результатов больше не будет.
            await result_queue.put(None)
            await result_queue.join()
            await writer

    finally:
        if pbar is not None:
            pbar.close()

        finished_at = datetime.now()
        elapsed_ms = (finished_at - started_at).total_seconds() * 1000

        # Метаданные пишем в конце, когда известны totals.
        metadata = {
            "generated_at": finished_at.isoformat(),
            "started_at": started_at.isoformat(),
            "elapsed_ms": round(elapsed_ms, 4),
            "confluence_url": settings.CONFLUENCE_BASE_URL,
            "chunk_size": settings.CHUNK_SIZE,
            "chunk_overlap": settings.CHUNK_OVERLAP,
            "chunking_strategy": settings.CHUNKING_STRATEGY,
            "max_heading_levels": settings.MAX_HEADING_LEVELS,
            "include_blocks": settings.INCLUDE_BLOCKS_IN_OUTPUT,
            "stats": stats,
        }

        await sink.close(metadata)

    # Итог
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    print(f"  Page IDs discovered  : {stats['page_ids']}")
    print(f"  Pages fetched        : {stats['pages_fetched']}")
    print(f"  Pages processed      : {stats['pages_processed']}")
    print(f"  Pages with errors    : {stats['pages_with_errors']}")
    print(f"  Total chunks         : {stats['chunks_written']}")
    if settings.INCLUDE_BLOCKS_IN_OUTPUT and settings.OUTPUT_WRITE_JSON:
        print(f"  Total blocks         : {stats['blocks_written']}")
    print(f"  Output target(s)     : {sink.output_path}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(0)
    except Exception as exc:
        logger.error(f"Unexpected error: {exc!r}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
