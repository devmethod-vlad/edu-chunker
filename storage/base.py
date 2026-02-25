"""Базовые интерфейсы для сохранения результатов.

Важно: этот проект сейчас пишет результат в JSON, но дальше (по твоему плану)
скрипт будет перестраиваться на запись данных в БД.

Чтобы потом не переписывать весь main.py, сохранение вынесено в интерфейс ResultSink.

Как использовать:
    sink: ResultSink = JsonStreamSink(...)
    await sink.open()

    ... в процессе обработки страниц ...
    await sink.write_page(page, blocks, chunks)

    ... в конце ...
    await sink.close(metadata)

Если завтра вместо JSON нужна БД:
- создаёшь новый класс DbSink(ResultSink)

Уже сейчас:
- реализован SqliteSink(ResultSink) — запись чанков в SQLite
- реализован CompositeSink(ResultSink) — запись сразу в несколько sink'ов
- реализуешь open/write_page/close (например, через asyncpg/SQLAlchemy)
- main.py не меняется.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from confluence.models import ConfluencePage, ContentBlock, Chunk


class ResultSink(ABC):
    """Абстракция для сохранения результатов обработки.

    Принцип: main.py отдаёт на вход целиком результат одной страницы.

    Почему именно "одна страница":
    - это естественная транзакционная единица (и для файлов, и для БД)
    - можно легко делать повторную обработку одной страницы
    - проще делать ретраи/идемпотентность в будущем
    """

    @abstractmethod
    async def open(self) -> None:
        """Инициализация ресурсов (файл, соединение с БД, и т.д.)."""

    @abstractmethod
    async def write_page(
        self,
        page: ConfluencePage,
        blocks: List[ContentBlock],
        chunks: List[Chunk],
    ) -> None:
        """Сохранить результаты обработки одной страницы."""

    @abstractmethod
    async def close(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Финализация и закрытие ресурсов."""

    @property
    @abstractmethod
    def output_path(self) -> str:
        """Путь к результирующему файлу/идентификатору (для логов)."""
