"""Модуль сохранения результатов.

Зачем он нужен:
- Сейчас скрипт по умолчанию пишет результат в JSON-файл.
- В дальнейшем результат может сохраняться в БД (часть полей) —
  поэтому логика сохранения вынесена в отдельный слой (sink).

Основная идея: main.py ничего не знает про формат хранения.
Он передаёт в sink результаты обработки страниц: blocks + chunks.

По умолчанию используется JsonStreamSink — потоковая запись в один JSON.

Новое:
- SqliteSink — запись чанков в SQLite (один файл в OUTPUT_DIR)
- CompositeSink — запись сразу в несколько sink'ов (например JSON + SQLite)
"""

from .base import ResultSink
from .json_stream import JsonStreamSink
from .sqlite_sink import SqliteSink
from .composite import CompositeSink

__all__ = [
    "ResultSink",
    "JsonStreamSink",
    "SqliteSink",
    "CompositeSink",
]
