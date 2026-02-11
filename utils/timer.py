"""
Утилита для замера времени выполнения участков кода.

Точность замера: миллисекунды с 4 знаками после запятой.

==========================================================================
КАК ИСПОЛЬЗОВАТЬ
==========================================================================

1. Как контекстный менеджер (рекомендуется):

    from utils.timer import timer

    with timer.measure("parsing_html"):
        # код, время которого измеряем
        parse_html(content)
    # Результат будет залогирован автоматически при выходе из блока

2. Явное указание меток начала и конца:

    timer.start("data_processing")
    process_data()
    timer.end("data_processing")  # Логирует результат

3. Получение времени без логирования:

    timer.start("calculation")
    result = calculate()
    elapsed = timer.get_elapsed("calculation")
    print(f"Calculation took {elapsed:.4f}ms")

Замеры отображаются в логах ТОЛЬКО если переменная окружения
SHOW_PERFORMANCE_METRICS=true. Иначе замеры работают, но ничего
не печатают — можно использовать get_elapsed() для ручного контроля.
==========================================================================
"""

import time
from typing import Dict, Optional
from contextlib import contextmanager

from utils.logger import logger


class PerformanceTimer:
    """
    Класс для замера времени выполнения участков кода.

    Args:
        show_metrics: Выводить ли замеры в лог (управляется через .env)
    """

    def __init__(self, show_metrics: bool = False):
        self.show_metrics = show_metrics
        self._timers: Dict[str, float] = {}

    # ------------------------------------------------------------------
    def start(self, label: str) -> None:
        """
        Начало замера.

        Args:
            label: Произвольная метка (имя) замера
        """
        self._timers[label] = time.perf_counter()

    # ------------------------------------------------------------------
    def end(self, label: str) -> Optional[float]:
        """
        Окончание замера и вывод результата в лог (если включено).

        Args:
            label: Метка, переданная ранее в start()

        Returns:
            Время выполнения в миллисекундах или None если метка не найдена
        """
        if label not in self._timers:
            if self.show_metrics:
                logger.warning(f"Timer '{label}' was not started")
            return None

        elapsed = self.get_elapsed(label)

        if self.show_metrics and elapsed is not None:
            logger.info(f"⏱  [{label}] completed in {elapsed:.4f} ms")

        # Удаляем метку после использования
        del self._timers[label]
        return elapsed

    # ------------------------------------------------------------------
    def get_elapsed(self, label: str) -> Optional[float]:
        """
        Текущее время от start() до «сейчас» без вывода в лог и без удаления метки.

        Returns:
            Время в миллисекундах (float) или None если метка не найдена
        """
        if label not in self._timers:
            return None
        return (time.perf_counter() - self._timers[label]) * 1000

    # ------------------------------------------------------------------
    @contextmanager
    def measure(self, label: str):
        """
        Контекстный менеджер для замера блока кода.

        Пример:
            with timer.measure("processing"):
                process_data()
        """
        self.start(label)
        try:
            yield
        finally:
            self.end(label)

    # ------------------------------------------------------------------
    def reset(self) -> None:
        """Сброс всех активных таймеров."""
        self._timers.clear()


def _create_timer() -> PerformanceTimer:
    """
    Создание глобального таймера с учётом настроек.

    Вынесено в функцию, чтобы избежать циклического импорта
    (timer ← settings ← ... ← timer).
    """
    from config.settings import settings
    return PerformanceTimer(show_metrics=settings.SHOW_PERFORMANCE_METRICS)


# Глобальный экземпляр — импортируйте как: from utils.timer import timer
timer = _create_timer()
