"""
Модуль для логирования событий приложения.

Использует стандартный logging для совместимости с tqdm (пишем в stderr,
чтобы не ломать прогресс-бар в stdout).
"""

import logging
import sys


def _setup_logger(name: str = "confluence_parser") -> logging.Logger:
    """
    Настройка и возврат логгера.

    Все сообщения пишутся в stderr, чтобы не конфликтовать с tqdm,
    который работает через stderr/stdout.
    """
    log = logging.getLogger(name)

    if log.handlers:
        # Логгер уже настроен (защита от повторного вызова при переимпорте)
        return log

    log.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    log.addHandler(handler)

    return log


# Глобальный экземпляр логгера
logger = _setup_logger()
