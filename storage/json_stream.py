"""Потоковая запись результата в один JSON-файл.

Почему потоковая:
- На большом портале нельзя собирать все chunks/blocks в памяти.
- Но требования (п.4) просят результат "по умолчанию" в виде JSON-файла.

Решение:
- Пишем валидный JSON-объект, но массивы (chunks/blocks) заполняем постепенно.
- Метаданные записываем в конце, когда известны итоги.

Формат файла:
{
  "chunks": [...],
  "pages": [...],    # метаданные по страницам (всегда)
  "blocks": [...],   # опционально
  "metadata": {...}
}

Примечание:
- Чтобы не блокировать event loop, все операции записи сделаны синхронно,
  но вызываются из отдельного writer-task (см. main.py). Поэтому lock не обязателен.
  Однако, на всякий случай, класс потокобезопасен через asyncio.Lock.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from confluence.models import Chunk, ContentBlock, ConfluencePage
from utils.logger import logger

from .base import ResultSink

# Пытаемся использовать orjson (быстрее). Если его нет — работаем на stdlib json.
try:
    import orjson

    def _dumps(obj: Any) -> bytes:
        # Компактный JSON лучше для больших файлов.
        return orjson.dumps(obj)

except ImportError:  # pragma: no cover
    def _dumps(obj: Any) -> bytes:
        return json.dumps(obj, ensure_ascii=False).encode("utf-8")


class JsonStreamSink(ResultSink):
    """Сохраняет результат в один JSON-файл с потоковой записью массивов."""

    def __init__(
        self,
        output_dir: str,
        *,
        include_blocks: bool = False,
        file_prefix: str = "confluence_chunks",
    ):
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._filepath = self._output_dir / f"{file_prefix}_{timestamp}.json"

        self._include_blocks = include_blocks

        self._fh = None  # type: ignore[assignment]
        self._lock = None  # type: ignore[assignment]

        self._first_chunk = True
        self._first_block = True
        self._first_page = True

        self.total_chunks = 0
        self.total_blocks = 0
        self.total_pages = 0

    # ------------------------------------------------------------------
    @property
    def output_path(self) -> str:
        return str(self._filepath)

    # ------------------------------------------------------------------
    async def open(self) -> None:
        import asyncio

        self._fh = self._filepath.open("wb")
        self._lock = asyncio.Lock()

        # Пишем начало JSON-объекта и открываем массив чанков.
        self._fh.write(b'{"chunks":[\n')

        # blocks добавим позже (после закрытия chunks), чтобы файл оставался валидным.
        logger.info(f"Streaming output -> {self._filepath}")

    # ------------------------------------------------------------------
    async def write_page(
        self,
        page: ConfluencePage,
        blocks: List[ContentBlock],
        chunks: List[Chunk],
    ) -> None:
        """Запись результатов одной страницы.

        Замечание:
        - Пишем chunk'и сразу, без буферизации.
        - blocks пишем только если include_blocks=True.
        """
        if self._fh is None or self._lock is None:
            raise RuntimeError("JsonStreamSink is not opened")

        # Один write_page = одна страница обработана.
        # Сохраняем мета-информацию по странице отдельным списком,
        # чтобы:
        #  - не выковыривать её из чанков при выдаче/отладке
        #  - позже без боли переложить в отдельную таблицу pages (при записи в БД)
        self.total_pages += 1
        await self._write_page_info(page)

        # Пишем chunks.
        for ch in chunks:
            await self._write_chunk(ch)

        # Пишем blocks, если включено.
        if self._include_blocks:
            for b in blocks:
                await self._write_block(b)

    # ------------------------------------------------------------------
    async def close(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        if self._fh is None or self._lock is None:
            return

        # Закрываем массив chunks
        self._fh.write(b"\n]")

        # pages — всегда (но пишем из sidecar-файла, чтобы не держать всё в памяти)
        self._fh.write(b',\n"pages":[\n')
        pages_sidecar = self._filepath.with_suffix(".pages.jsonl")
        first = True
        if pages_sidecar.exists():
            with pages_sidecar.open("rb") as pf:
                for raw_line in pf:
                    line = raw_line.strip()
                    if not line:
                        continue
                    if not first:
                        self._fh.write(b",\n")
                    self._fh.write(line)
                    first = False
        self._fh.write(b"\n]")

        # Sidecar больше не нужен.
        try:
            if pages_sidecar.exists():
                pages_sidecar.unlink()
        except Exception:
            # Если не удалили — не критично, просто оставим.
            pass

        # blocks — опционально
        if self._include_blocks:
            self._fh.write(b',\n"blocks":[\n')

            sidecar = self._filepath.with_suffix(".blocks.jsonl")
            first = True
            if sidecar.exists():
                # Переписываем JSONL -> JSON array без загрузки всех блоков в память.
                with sidecar.open("rb") as bf:
                    for raw_line in bf:
                        line = raw_line.strip()
                        if not line:
                            continue
                        if not first:
                            self._fh.write(b",\n")
                        self._fh.write(line)
                        first = False

            self._fh.write(b"\n]")

            # Sidecar больше не нужен.
            try:
                if sidecar.exists():
                    sidecar.unlink()
            except Exception:
                # Если не удалили — не критично, просто оставим.
                pass

        # metadata — всегда
        meta = metadata or {}
        self._fh.write(b',\n"metadata":')
        self._fh.write(_dumps(meta))
        self._fh.write(b"\n}")

        self._fh.close()
        logger.info(f"Output file closed: {self._filepath}")

    # ------------------------------------------------------------------
    async def _write_chunk(self, chunk: Chunk) -> None:
        assert self._fh is not None and self._lock is not None

        data = chunk.to_dict()

        async with self._lock:
            if not self._first_chunk:
                self._fh.write(b",\n")
            self._fh.write(_dumps(data))
            self._first_chunk = False

        self.total_chunks += 1

    async def _write_block(self, block: ContentBlock) -> None:
        assert self._fh is not None and self._lock is not None

        # "blocks" массив физически будет записан ТОЛЬКО при close() выше.
        # Поэтому здесь мы буферим блоки в отдельный временный файл.
        #
        # Но требования по умолчанию blocks не требуют, и включается это флагом.
        # Чтобы не усложнять основной сценарий — используем sidecar-файл.
        # Он тоже JSONL и очень легко перегоняется в БД.

        sidecar = self._filepath.with_suffix(".blocks.jsonl")
        # Пишем в append режиме, отдельным потоком. Это безопаснее и проще.
        line = _dumps(block.to_dict()) + b"\n"

        async with self._lock:
            with sidecar.open("ab") as bf:
                bf.write(line)

        self.total_blocks += 1

    async def _write_page_info(self, page: ConfluencePage) -> None:
        """Записывает мета-информацию о странице в sidecar JSONL.

        Почему JSONL:
        - потоково
        - легко перегоняется в БД (INSERT ... или COPY)
        - не требует держать всё в памяти
        """
        assert self._fh is not None and self._lock is not None

        sidecar = self._filepath.with_suffix(".pages.jsonl")
        line = _dumps(page.to_page_info()) + b"\n"

        async with self._lock:
            with sidecar.open("ab") as pf:
                pf.write(line)

        self._first_page = False
