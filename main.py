"""
Главный скрипт для сбора и структурирования данных из Confluence.

Этапы:
  1. Получение страниц из Confluence через REST API
  2. Парсинг HTML-контента и извлечение логических блоков
  3. Формирование чанков для семантического поиска
  4. Сохранение результатов в JSON-файл
"""

import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

from config.settings import settings
from utils.logger import logger
from utils.timer import timer
from confluence import fetch_confluence_pages, ConfluencePage, Chunk, ContentBlock
from parser import parse_page_content
from chunking import create_chunks_from_page

# Пытаемся использовать orjson (быстрее), но можем работать и без него
try:
    import orjson

    def _dump_json(obj) -> bytes:
        return orjson.dumps(obj, option=orjson.OPT_INDENT_2)

except ImportError:
    def _dump_json(obj) -> bytes:
        return json.dumps(obj, ensure_ascii=False, indent=2).encode('utf-8')


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
# Обработка одной страницы
# ---------------------------------------------------------------------------

def process_page(page: ConfluencePage) -> Tuple[List[Chunk], List[ContentBlock]]:
    """Парсинг + чанкование одной страницы."""
    with timer.measure(f"page_{page.id}"):
        blocks, headings = parse_page_content(page.body_html, page.id)

        if not blocks:
            logger.warning(f"No content blocks in page {page.id} ({page.title})")
            return [], []

        logger.info(
            f"Page '{page.title}' ({page.id}): "
            f"{len(blocks)} blocks, {len(headings)} headings"
        )

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

        logger.info(f"Page '{page.title}': {len(chunks)} chunks")
        return chunks, blocks


# ---------------------------------------------------------------------------
# Сохранение результатов
# ---------------------------------------------------------------------------

def save_results(
    all_chunks: List[Chunk],
    all_blocks: List[ContentBlock],
    output_dir: str,
) -> str:
    """Сохранение в JSON-файл с таймстампом в имени."""
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = out_path / f"confluence_chunks_{timestamp}.json"

    data = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'total_chunks': len(all_chunks),
            'total_blocks': len(all_blocks) if settings.INCLUDE_BLOCKS_IN_OUTPUT else None,
            'confluence_url': settings.CONFLUENCE_BASE_URL,
            'chunk_size': settings.CHUNK_SIZE,
            'chunk_overlap': settings.CHUNK_OVERLAP,
            'chunking_strategy': settings.CHUNKING_STRATEGY,
        },
        'chunks': [c.to_dict() for c in all_chunks],
    }

    if settings.INCLUDE_BLOCKS_IN_OUTPUT:
        data['blocks'] = [b.to_dict() for b in all_blocks]
        logger.info(f"Including {len(all_blocks)} blocks in output")

    filepath.write_bytes(_dump_json(data))
    logger.info(f"Results saved to: {filepath}")
    return str(filepath)


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------

async def main():
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

    # ---- Получение страниц ----
    logger.info("Starting data collection from Confluence…")

    try:
        with timer.measure("fetch_all_pages"):
            pages = await fetch_confluence_pages(
                page_ids=settings.CONFLUENCE_PAGE_IDS or None
            )
    except Exception as exc:
        logger.error(f"Failed to fetch pages: {exc!r}")
        sys.exit(1)

    if not pages:
        logger.error("No pages fetched from Confluence")
        sys.exit(1)

    logger.info(f"Fetched {len(pages)} pages")

    # ---- Обработка страниц ----
    logger.info("Processing pages and creating chunks…")

    all_chunks: List[Chunk] = []
    all_blocks: List[ContentBlock] = []

    tqdm_cls = _get_tqdm()
    iterator = tqdm_cls(pages, desc="Processing pages") if tqdm_cls else pages

    with timer.measure("process_all_pages"):
        for page in iterator:
            try:
                chunks, blocks = process_page(page)
                all_chunks.extend(chunks)
                if settings.INCLUDE_BLOCKS_IN_OUTPUT:
                    all_blocks.extend(blocks)

                # Обновляем прогресс-бар
                if tqdm_cls and hasattr(iterator, 'set_postfix'):
                    iterator.set_postfix(
                        chunks=len(all_chunks),
                        page=page.title[:30],
                    )
            except Exception as exc:
                logger.error(f"Error processing page {page.id}: {exc!r}")
                continue

    if not all_chunks:
        logger.error("No chunks created from any page")
        sys.exit(1)

    logger.info(f"Created {len(all_chunks)} chunks from {len(pages)} pages")

    # ---- Сохранение ----
    with timer.measure("save_results"):
        output_file = save_results(all_chunks, all_blocks, settings.OUTPUT_DIR)

    # ---- Итог ----
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    print(f"  Pages processed      : {len(pages)}")
    print(f"  Total chunks         : {len(all_chunks)}")
    print(f"  Avg chunks/page      : {len(all_chunks) / len(pages):.1f}")
    if settings.INCLUDE_BLOCKS_IN_OUTPUT:
        print(f"  Total blocks         : {len(all_blocks)}")
    print(f"  Output file          : {output_file}")
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
