"""
Модуль настроек приложения.
Загружает и валидирует переменные окружения из .env файла.
"""

import os
from pathlib import Path
from typing import List, Optional
from dotenv import load_dotenv


# Загружаем переменные из .env файла (ищем рядом с корнем проекта)
_env_path = Path(__file__).parent.parent / '.env'
load_dotenv(_env_path)


def _get_bool(key: str, default: str = 'false') -> bool:
    """Безопасное чтение булевой переменной окружения."""
    return os.getenv(key, default).strip().lower() == 'true'


def _get_int(key: str, default: str = '0') -> int:
    """Безопасное чтение целочисленной переменной окружения."""
    return int(os.getenv(key, default).strip())


def _get_list(key: str, default: str = '') -> List[str]:
    """Чтение списка из переменной окружения (значения через запятую)."""
    raw = os.getenv(key, default).strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(',') if item.strip()]


def _is_safe_identifier(value: str) -> bool:
    """Проверяет, что строка может безопасно использоваться как SQL-идентификатор.

    Здесь не пытаемся поддержать весь SQL синтаксис SQLite, а намеренно
    ограничиваемся простыми именами, чтобы:
    - не допустить SQL-инъекций через env
    - сделать поведение предсказуемым

    Разрешено:
    - латинские буквы
    - цифры (не в первом символе)
    - underscore _
    """
    v = (value or "").strip()
    if not v:
        return False
    import re
    return re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", v) is not None


class Settings:
    """
    Класс для хранения и валидации настроек приложения.

    Все значения вычисляются в __init__, чтобы избежать проблем
    с мутабельными значениями по умолчанию на уровне класса.
    """

    def __init__(self):
        # --- Confluence ---
        self.CONFLUENCE_BASE_URL: str = os.getenv('CONFLUENCE_BASE_URL', '').strip().rstrip('/')
        self.CONFLUENCE_AUTH_TOKEN: Optional[str] = os.getenv('CONFLUENCE_AUTH_TOKEN', '').strip() or None

        # Требования называют переменную CONFLUENCE_PAGE_ID (ед. число)
        self.CONFLUENCE_PAGE_IDS: List[str] = _get_list('CONFLUENCE_PAGE_ID')

        # --- Chunking ---
        self.CHUNK_SIZE: int = _get_int('CHUNK_SIZE', '512')
        self.CHUNK_OVERLAP: int = _get_int('CHUNK_OVERLAP', '0')
        self.CHUNKING_STRATEGY: str = os.getenv('CHUNKING_STRATEGY', 'simple').strip()
        self.MAX_HEADING_LEVELS: int = _get_int('MAX_HEADING_LEVELS', '2')

        # Разделение на предложения. Используется при упаковке блоков в чанки,
        # чтобы не резать предложения на границах.
        # Варианты: 'regex' (без зависимостей), 'razdel', 'spacy'
        self.SENTENCE_SPLITTER: str = os.getenv('SENTENCE_SPLITTER', 'regex').strip()

        # --- HTML parsing ---
        self.BLOCK_TAGS: List[str] = _get_list(
            'BLOCK_TAGS',
            'p,div,blockquote,pre,ul,ol,table,h1,h2,h3,h4,h5,h6,section,article,figure,figcaption'
        )
        self.EXCLUDED_TAGS: List[str] = _get_list(
            'EXCLUDED_TAGS',
            'code,script,style,hr,nav,header,footer'
        )
        # CSS-классы элементов для исключения (совпадение или начало с)
        self.EXCLUDED_CLASSES: List[str] = _get_list('EXCLUDED_CLASSES')
        # HTML id элементов для исключения (совпадение или начало с)
        self.EXCLUDED_IDS: List[str] = _get_list('EXCLUDED_IDS')
        self.INCLUDE_PAGE_TAG: bool = _get_bool('INCLUDE_PAGE_TAG', 'true')
        self.INCLUDE_SECTION_TAG: bool = _get_bool('INCLUDE_SECTION_TAG', 'true')

        # --- Performance & logging ---
        self.SHOW_PROGRESS_BAR: bool = _get_bool('SHOW_PROGRESS_BAR', 'false')
        self.SHOW_PERFORMANCE_METRICS: bool = _get_bool('SHOW_PERFORMANCE_METRICS', 'false')
        self.MAX_CONCURRENT_REQUESTS: int = _get_int('MAX_CONCURRENT_REQUESTS', '10')
        # Ограничение параллельной обработки (parse + chunk) страницы.
        # Это CPU-bound стадия (BeautifulSoup + нормализация + чанкование),
        # поэтому разумно держать значение близко к числу CPU.
        # По умолчанию: min(8, cpu_count) но не меньше 2.
        default_parsing = str(max(2, min(8, (os.cpu_count() or 4))))
        self.MAX_CONCURRENT_PARSING: int = _get_int('MAX_CONCURRENT_PARSING', default_parsing)

        # Размеры очередей пайплайна. Нужны, чтобы скрипт не разгонялся
        # по памяти на больших порталах.
        #
        # Если оставить по умолчанию — будет достаточно для большинства случаев.
        # В будущем, при записи в БД, эти значения продолжат работать так же.
        self.PAGE_ID_QUEUE_SIZE: int = _get_int('PAGE_ID_QUEUE_SIZE', str(self.MAX_CONCURRENT_REQUESTS * 50))
        self.PAGE_QUEUE_SIZE: int = _get_int('PAGE_QUEUE_SIZE', str(self.MAX_CONCURRENT_REQUESTS * 10))
        self.RESULT_QUEUE_SIZE: int = _get_int('RESULT_QUEUE_SIZE', str(self.MAX_CONCURRENT_PARSING * 10))
        self.REQUEST_TIMEOUT: int = _get_int('REQUEST_TIMEOUT', '30')
        self.MAX_RETRIES: int = _get_int('MAX_RETRIES', '3')

        # --- Output ---
        self.OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', 'output').strip()
        self.INCLUDE_BLOCKS_IN_OUTPUT: bool = _get_bool('INCLUDE_BLOCKS_IN_OUTPUT', 'false')

        # Включить ли сохранение результата в JSON (по умолчанию: true).
        # Это текущий "классический" режим работы скрипта.
        self.OUTPUT_WRITE_JSON: bool = _get_bool('OUTPUT_WRITE_JSON', 'true')

        # Включить ли сохранение чанков в SQLite (по умолчанию: false).
        # SQLite-файл создаётся в OUTPUT_DIR.
        self.OUTPUT_WRITE_SQLITE: bool = _get_bool('OUTPUT_WRITE_SQLITE', 'false')

        # Имя файла SQLite (лежит в OUTPUT_DIR).
        self.SQLITE_DB_FILENAME: str = os.getenv('SQLITE_DB_FILENAME', 'confluence_chunks.sqlite3').strip()

        # Имя таблицы, куда пишем чанки.
        # Должно быть "простым" идентификатором: латиница/цифры/underscore.
        self.SQLITE_TABLE_NAME: str = os.getenv('SQLITE_TABLE_NAME', 'chunks').strip()

        # Имя поля, куда складываем JSON payload (всё, кроме chunk_id).
        # Должно быть "простым" идентификатором: латиница/цифры/underscore.
        self.SQLITE_PAYLOAD_FIELD: str = os.getenv('SQLITE_PAYLOAD_FIELD', 'payload').strip()

    # ------------------------------------------------------------------
    def validate(self) -> None:
        """Валидация настроек приложения. Бросает ValueError при ошибках."""
        errors: List[str] = []

        if not self.CONFLUENCE_BASE_URL:
            errors.append("CONFLUENCE_BASE_URL must be set")

        if self.CHUNK_SIZE <= 0:
            errors.append("CHUNK_SIZE must be positive")

        if self.CHUNK_OVERLAP < 0:
            errors.append("CHUNK_OVERLAP must be non-negative")

        if self.CHUNK_OVERLAP >= self.CHUNK_SIZE:
            errors.append("CHUNK_OVERLAP must be strictly less than CHUNK_SIZE")

        if self.CHUNKING_STRATEGY not in ('tokenizer', 'simple'):
            errors.append("CHUNKING_STRATEGY must be 'tokenizer' or 'simple'")

        if self.SENTENCE_SPLITTER not in ('regex', 'razdel', 'spacy'):
            errors.append("SENTENCE_SPLITTER must be 'regex', 'razdel' or 'spacy'")

        if self.MAX_HEADING_LEVELS <= 0:
            errors.append("MAX_HEADING_LEVELS must be positive")

        if self.MAX_CONCURRENT_REQUESTS <= 0:
            errors.append("MAX_CONCURRENT_REQUESTS must be positive")

        if self.MAX_CONCURRENT_PARSING <= 0:
            errors.append("MAX_CONCURRENT_PARSING must be positive")

        if self.PAGE_ID_QUEUE_SIZE <= 0:
            errors.append("PAGE_ID_QUEUE_SIZE must be positive")

        if self.PAGE_QUEUE_SIZE <= 0:
            errors.append("PAGE_QUEUE_SIZE must be positive")

        if self.RESULT_QUEUE_SIZE <= 0:
            errors.append("RESULT_QUEUE_SIZE must be positive")

        if self.REQUEST_TIMEOUT <= 0:
            errors.append("REQUEST_TIMEOUT must be positive")

        # --- Output validation ---
        # Если включён SQLite — валидируем параметры, которые будут использоваться
        # как SQL-идентификаторы. Это защита от случайных ошибок и от инъекций
        # через переменные окружения.
        if self.OUTPUT_WRITE_SQLITE:
            if not self.SQLITE_DB_FILENAME:
                errors.append("SQLITE_DB_FILENAME must be set when OUTPUT_WRITE_SQLITE=true")
            else:
                # Файл должен лежать в OUTPUT_DIR, поэтому запрещаем подкаталоги в имени.
                from pathlib import Path as _Path
                if _Path(self.SQLITE_DB_FILENAME).name != self.SQLITE_DB_FILENAME:
                    errors.append("SQLITE_DB_FILENAME must be a filename only (no directories)")

            if not _is_safe_identifier(self.SQLITE_TABLE_NAME):
                errors.append("SQLITE_TABLE_NAME must be a safe identifier (A-Za-z0-9_, not starting with digit)")

            if not _is_safe_identifier(self.SQLITE_PAYLOAD_FIELD):
                errors.append("SQLITE_PAYLOAD_FIELD must be a safe identifier (A-Za-z0-9_, not starting with digit)")

        if errors:
            raise ValueError("Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))

    # ------------------------------------------------------------------
    def display(self) -> None:
        """Вывод текущих настроек в читаемом виде."""
        print("=" * 80)
        print("CONFIGURATION SETTINGS")
        print("=" * 80)
        print(f"  Confluence URL       : {self.CONFLUENCE_BASE_URL}")
        print(f"  Auth Token           : {'Set' if self.CONFLUENCE_AUTH_TOKEN else 'Not set (anonymous)'}")
        print(f"  Page IDs             : {', '.join(self.CONFLUENCE_PAGE_IDS) if self.CONFLUENCE_PAGE_IDS else 'All pages'}")
        print(f"  Chunk size           : {self.CHUNK_SIZE} tokens")
        print(f"  Chunk overlap        : {self.CHUNK_OVERLAP} tokens")
        print(f"  Chunking strategy    : {self.CHUNKING_STRATEGY}")
        print(f"  Sentence splitter    : {self.SENTENCE_SPLITTER}")
        print(f"  Max heading levels   : {self.MAX_HEADING_LEVELS}")
        print(f"  Show progress bar    : {self.SHOW_PROGRESS_BAR}")
        print(f"  Performance metrics  : {self.SHOW_PERFORMANCE_METRICS}")
        print(f"  Max concurrent reqs  : {self.MAX_CONCURRENT_REQUESTS}")
        print(f"  Max concurrent parse : {self.MAX_CONCURRENT_PARSING}")
        print(f"  Queues (id/page/out) : {self.PAGE_ID_QUEUE_SIZE}/{self.PAGE_QUEUE_SIZE}/{self.RESULT_QUEUE_SIZE}")
        print(f"  Max retries          : {self.MAX_RETRIES}")
        print(f"  Output directory     : {self.OUTPUT_DIR}")
        print(f"  Output -> JSON       : {self.OUTPUT_WRITE_JSON}")
        print(f"  Output -> SQLite     : {self.OUTPUT_WRITE_SQLITE}")
        if self.OUTPUT_WRITE_SQLITE:
            print(f"  SQLite filename      : {self.SQLITE_DB_FILENAME}")
            print(f"  SQLite table         : {self.SQLITE_TABLE_NAME}")
            print(f"  SQLite payload field : {self.SQLITE_PAYLOAD_FIELD}")
        print("=" * 80)


# Единственный экземпляр настроек для использования во всех модулях
settings = Settings()
