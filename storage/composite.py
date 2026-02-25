"""Композитный sink: пишет результат одновременно в несколько хранилищ.

Нужен, чтобы:
- сохранить текущий дефолт (JSON),
- но при этом иметь возможность дополнительно писать в SQLite,
  не меняя архитектуру пайплайна.

Поведение:
- open() вызывает open() у всех sink'ов
- write_page() пишет в каждый sink; если один sink падает —
  стараемся всё равно дописать в остальные, затем пробрасываем ошибку наверх
  (writer_task отметит страницу как failed)
- close() пытается закрыть все sink'и, собирая ошибки
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from confluence.models import Chunk, ContentBlock, ConfluencePage
from utils.logger import logger

from .base import ResultSink


class CompositeSink(ResultSink):
    """Объединяет несколько sink'ов в один."""

    def __init__(self, sinks: List[ResultSink]) -> None:
        self._sinks = [s for s in (sinks or []) if s is not None]
        if not self._sinks:
            raise ValueError("CompositeSink requires at least one sink")

    # ------------------------------------------------------------------
    @property
    def output_path(self) -> str:
        # Для логов/консоли: перечисляем все пути.
        return " | ".join(s.output_path for s in self._sinks)

    # ------------------------------------------------------------------
    async def open(self) -> None:
        for s in self._sinks:
            await s.open()

    # ------------------------------------------------------------------
    async def write_page(
        self,
        page: ConfluencePage,
        blocks: List[ContentBlock],
        chunks: List[Chunk],
    ) -> None:
        errors: List[Exception] = []

        for s in self._sinks:
            try:
                await s.write_page(page, blocks, chunks)
            except Exception as exc:
                errors.append(exc)
                logger.error(f"CompositeSink: sink '{type(s).__name__}' failed: {exc!r}")

        if errors:
            # Пробрасываем первую ошибку наверх (writer_task её залогирует и посчитает).
            raise errors[0]

    # ------------------------------------------------------------------
    async def close(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        errors: List[Exception] = []
        for s in self._sinks:
            try:
                await s.close(metadata)
            except Exception as exc:
                errors.append(exc)
                logger.error(f"CompositeSink: close() for '{type(s).__name__}' failed: {exc!r}")
        if errors:
            raise errors[0]
