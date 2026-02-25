"""Сохранение чанков в SQLite.

Задача:
- При больших объёмах данных JSON-файл становится тяжёлым для дальнейшей обработки.
- SQLite — "простая БД в одном файле", которую легко переносить между проектами
  без поднятия отдельной инфраструктуры.

Логика сохранения (упрощённая по требованиям):
- В таблицу пишем одну строку на один chunk.
- PRIMARY KEY = chunk_id (поле Chunk.chunk_id)
- Всё остальное складываем в JSON-поле payload (название поля задаётся через env).

Почему именно так:
- chunk_id уже уникален и стабилен для повторных прогонов
- payload в JSON оставляет полную информацию "как в JSON-выгрузке",
  но в более удобном контейнере (SQL)
- дальше можно добавить отдельные колонки/таблицы без ломки пайплайна

Важно:
- Этот sink НЕ пишет blocks/pages — только chunks.
  (В JSON выгрузке pages/blocks остаются доступными при включённом JSON-выводе.)
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from confluence.models import Chunk, ContentBlock, ConfluencePage
from utils.logger import logger

from .base import ResultSink


class SqliteSink(ResultSink):
    """Запись чанков в SQLite-файл.

    Ожидаемый формат таблицы:
        CREATE TABLE IF NOT EXISTS <table_name> (
            chunk_id TEXT PRIMARY KEY,
            <payload_field> TEXT NOT NULL
        )

    По умолчанию используется INSERT OR REPLACE, чтобы:
    - повторный прогон был идемпотентным
    - при изменениях в содержимом chunk'ов запись обновлялась
    """

    def __init__(
        self,
        output_dir: str,
        *,
        db_filename: str = "confluence_chunks.sqlite3",
        table_name: str = "chunks",
        payload_field: str = "payload",
    ) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)

        # Файл БД лежит рядом с JSON, в OUTPUT_DIR (по требованиям)
        self._db_path = self._output_dir / db_filename

        self._table_name = table_name.strip() or "chunks"
        self._payload_field = payload_field.strip() or "payload"

        self._conn: Optional[sqlite3.Connection] = None
        self._lock = None  # type: ignore[assignment]

        self.total_chunks = 0
        self.total_pages = 0

    # ------------------------------------------------------------------
    @property
    def output_path(self) -> str:
        return str(self._db_path)

    # ------------------------------------------------------------------
    async def open(self) -> None:
        import asyncio

        # SQLite соединение создаём один раз и используем в writer-task.
        # Здесь всё синхронно — writer-task и так является "единственной точкой записи".
        self._conn = sqlite3.connect(str(self._db_path))
        self._conn.row_factory = sqlite3.Row

        # Небольшие прагмы для более безопасной/быстрой записи.
        # WAL улучшает параллельное чтение, но нам главное — стабильность.
        try:
            self._conn.execute("PRAGMA journal_mode=WAL;")
        except Exception:
            # В редких окружениях (read-only FS и т.п.) WAL может не включиться.
            pass

        try:
            self._conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass

        self._lock = asyncio.Lock()

        self._ensure_schema()

        logger.info(f"SQLite output -> {self._db_path} (table: {self._table_name})")

    # ------------------------------------------------------------------
    async def write_page(
        self,
        page: ConfluencePage,
        blocks: List[ContentBlock],
        chunks: List[Chunk],
    ) -> None:
        if self._conn is None or self._lock is None:
            raise RuntimeError("SqliteSink is not opened")

        # Пишем "страница обработана" (для статистики). Сама страница не сохраняется.
        self.total_pages += 1

        if not chunks:
            return

        # Готовим данные для вставки одной транзакцией на страницу —
        # это намного быстрее, чем коммит на каждый chunk.
        rows = []
        for ch in chunks:
            payload_obj: Dict[str, Any] = ch.to_dict()
            # В требованиях: chunk_id используем как id записи,
            # а остальное кладём в payload.
            payload_obj.pop("chunk_id", None)

            rows.append((ch.chunk_id, json.dumps(payload_obj, ensure_ascii=False)))

        insert_sql = (
            f"INSERT OR REPLACE INTO {self._table_name} (chunk_id, {self._payload_field}) "
            f"VALUES (?, ?);"
        )

        async with self._lock:
            cur = self._conn.cursor()
            cur.executemany(insert_sql, rows)
            self._conn.commit()

        self.total_chunks += len(rows)

    # ------------------------------------------------------------------
    async def close(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        # metadata пока не сохраняем — но оставляем параметр, чтобы интерфейс не ломался.
        if self._conn is None:
            return
        try:
            self._conn.commit()
        except Exception:
            pass
        try:
            self._conn.close()
        except Exception:
            pass
        self._conn = None
        logger.info(f"SQLite database closed: {self._db_path}")

    # ------------------------------------------------------------------
    def _ensure_schema(self) -> None:
        assert self._conn is not None

        # Важно: имена таблицы/колонки приходят из env, поэтому:
        # - делаем минимальную "санитизацию" (без пробелов)
        # - не используем их как значения параметров (SQLite так не умеет),
        #   поэтому единственный безопасный способ — ограничивать их на уровне валидации.
        # В settings.validate() для этих полей добавлена проверка на допустимые символы.
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {self._table_name} ("
            f"chunk_id TEXT PRIMARY KEY, "
            f"{self._payload_field} TEXT NOT NULL"
            f");"
        )
        self._conn.execute(create_sql)
        self._conn.commit()
