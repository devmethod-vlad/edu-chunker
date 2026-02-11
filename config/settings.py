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
        self.REQUEST_TIMEOUT: int = _get_int('REQUEST_TIMEOUT', '30')
        self.MAX_RETRIES: int = _get_int('MAX_RETRIES', '3')

        # --- Output ---
        self.OUTPUT_DIR: str = os.getenv('OUTPUT_DIR', 'output').strip()
        self.INCLUDE_BLOCKS_IN_OUTPUT: bool = _get_bool('INCLUDE_BLOCKS_IN_OUTPUT', 'false')

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

        if self.MAX_HEADING_LEVELS <= 0:
            errors.append("MAX_HEADING_LEVELS must be positive")

        if self.MAX_CONCURRENT_REQUESTS <= 0:
            errors.append("MAX_CONCURRENT_REQUESTS must be positive")

        if self.REQUEST_TIMEOUT <= 0:
            errors.append("REQUEST_TIMEOUT must be positive")

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
        print(f"  Max heading levels   : {self.MAX_HEADING_LEVELS}")
        print(f"  Show progress bar    : {self.SHOW_PROGRESS_BAR}")
        print(f"  Performance metrics  : {self.SHOW_PERFORMANCE_METRICS}")
        print(f"  Max concurrent reqs  : {self.MAX_CONCURRENT_REQUESTS}")
        print(f"  Max retries          : {self.MAX_RETRIES}")
        print(f"  Output directory     : {self.OUTPUT_DIR}")
        print("=" * 80)


# Единственный экземпляр настроек для использования во всех модулях
settings = Settings()
