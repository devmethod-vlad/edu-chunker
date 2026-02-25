"""
Асинхронный API-клиент для работы с Confluence Data Center 7.19.

Ключевые улучшения:
  - Один httpx.AsyncClient на весь жизненный цикл (connection pooling)
  - Retry с экспоненциальной задержкой при 429/5xx
  - Запрос body.view (отрендеренный HTML, а не body.storage)
  - Явный фильтр type=page при обходе всех страниц
"""

import asyncio
from typing import List, Optional, Dict, Any
from typing import AsyncIterator

import httpx

from config.settings import settings
from utils.logger import logger
from utils.timer import timer
from .models import ConfluencePage


# ---------------------------------------------------------------------------
# Клиент
# ---------------------------------------------------------------------------

class ConfluenceAPIClient:
    """
    Асинхронный клиент для Confluence REST API v1.

    Использует httpx с connection pooling и семафором для ограничения
    количества параллельных запросов.
    """

    def __init__(
        self,
        base_url: str,
        auth_token: Optional[str] = None,
        max_concurrent: int = 10,
        timeout: int = 30,
        max_retries: int = 3,
    ):
        self.base_url = base_url.rstrip('/')
        self.max_retries = max_retries

        # Семафор для ограничения конкурентности
        self._max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)

        # Заголовки
        headers: Dict[str, str] = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        if auth_token:
            headers['Authorization'] = auth_token

        # Один клиент на все запросы (connection pool!)
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=timeout,
            # httpx по умолчанию хранит до 100 соединений
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Закрытие HTTP-клиента (освобождение соединений)."""
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    # ------------------------------------------------------------------
    # Низкоуровневый запрос с retry
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        HTTP-запрос с семафором и retry при 429 / 5xx.

        Returns:
            Распарсенный JSON ответа.
        """
        url = endpoint  # base_url уже в клиенте

        async with self._semaphore:
            last_exc: Optional[Exception] = None

            for attempt in range(1, self.max_retries + 1):
                try:
                    resp = await self._client.request(method, url, params=params)
                    resp.raise_for_status()
                    return resp.json()

                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code
                    # Retry только при 429 (rate limit) и 5xx (серверные ошибки)
                    if status in (429,) or 500 <= status < 600:
                        last_exc = exc
                        delay = 2 ** attempt  # 2, 4, 8 …
                        logger.warning(
                            f"HTTP {status} for {url}, retry {attempt}/{self.max_retries} "
                            f"in {delay}s"
                        )
                        await asyncio.sleep(delay)
                        continue
                    # Остальные HTTP-ошибки пробрасываем сразу
                    raise

                except httpx.RequestError as exc:
                    last_exc = exc
                    delay = 2 ** attempt
                    logger.warning(
                        f"Request error for {url}: {exc!r}, "
                        f"retry {attempt}/{self.max_retries} in {delay}s"
                    )
                    await asyncio.sleep(delay)
                    continue

            # Все попытки исчерпаны
            raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Получение списка всех страниц
    # ------------------------------------------------------------------

    async def get_all_page_ids(self) -> List[str]:
        """
        Получение ID всех доступных страниц (type=page).

        Пагинация через start/limit.
        """
        page_ids: List[str] = []
        start = 0
        limit = 100

        logger.info("Fetching list of all available pages…")

        while True:
            data = await self._request('GET', '/rest/api/content', params={
                'type': 'page',   # Только страницы, не блоги
                'limit': limit,
                'start': start,
                'expand': 'space',
            })

            results = data.get('results', [])
            if not results:
                break

            for item in results:
                page_ids.append(str(item['id']))

            # Проверяем, есть ли следующая порция
            if len(results) < limit:
                break
            start += limit

        logger.info(f"Found {len(page_ids)} pages")
        return page_ids

    async def iter_all_page_ids(self) -> AsyncIterator[str]:
        """Потоковое получение ID всех доступных страниц.

        Зачем это нужно:
        - На больших порталах хранить весь список IDs в памяти может быть нежелательно.
        - Для пайплайна (fetch -> parse -> write) удобнее получать IDs "на лету".

        Реализация:
        - Пагинация через start/limit
        - yield каждого page_id по мере получения пачки
        """
        start = 0
        limit = 100

        logger.info("Streaming list of all available pages…")

        while True:
            data = await self._request('GET', '/rest/api/content', params={
                'type': 'page',
                'limit': limit,
                'start': start,
                'expand': 'space',
            })

            results = data.get('results', [])
            if not results:
                break

            for item in results:
                yield str(item['id'])

            if len(results) < limit:
                break
            start += limit

    # ------------------------------------------------------------------
    # Получение контента одной страницы
    # ------------------------------------------------------------------

    async def get_page(self, page_id: str) -> Optional[ConfluencePage]:
        """
        Получение полного контента страницы.

        Запрашивает body.view — отрендеренный HTML, тот же что видит
        пользователь в браузере (без chrome Confluence).
        """
        try:
            data = await self._request('GET', f'/rest/api/content/{page_id}', params={
                'expand': 'body.view,version,space',
            })

            page = ConfluencePage.from_api_response(data)

            # Формируем абсолютный URL если нужно
            if page.url and not page.url.startswith('http'):
                page.url = f"{self.base_url}{page.url}"

            return page

        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                logger.warning(f"Page {page_id} not found")
                return None
            raise
        except Exception as exc:
            logger.error(f"Error fetching page {page_id}: {exc!r}")
            return None

    # ------------------------------------------------------------------
    # Пакетное получение страниц
    # ------------------------------------------------------------------

    async def get_pages_batch(self, page_ids: List[str]) -> List[ConfluencePage]:
        """
        Параллельное получение нескольких страниц.
        """
        with timer.measure("fetch_pages_batch"):
            # Важно: не создаём по задаче на КАЖДУЮ страницу сразу —
            # на больших порталах это легко съедает память.
            #
            # Вместо этого идём "окнами": внутри окна параллелим,
            # конкурентность всё равно ограничена семафором.
            window = max(1, self._max_concurrent * 10)
            out: List[ConfluencePage] = []

            for start in range(0, len(page_ids), window):
                batch = page_ids[start:start + window]
                tasks = [self.get_page(pid) for pid in batch]
                pages = await asyncio.gather(*tasks)
                out.extend([p for p in pages if p is not None])

            return out


# ---------------------------------------------------------------------------
# Высокоуровневая функция — единственная точка входа для main.py
# ---------------------------------------------------------------------------

async def fetch_confluence_pages(
    page_ids: Optional[List[str]] = None,
) -> List[ConfluencePage]:
    """
    Получение страниц из Confluence.

    Args:
        page_ids: Конкретные ID (если не указаны — все доступные).

    Returns:
        Список ConfluencePage.
    """
    async with ConfluenceAPIClient(
        base_url=settings.CONFLUENCE_BASE_URL,
        auth_token=settings.CONFLUENCE_AUTH_TOKEN,
        max_concurrent=settings.MAX_CONCURRENT_REQUESTS,
        timeout=settings.REQUEST_TIMEOUT,
        max_retries=settings.MAX_RETRIES,
    ) as client:

        if not page_ids:
            logger.info("No specific pages requested — fetching all…")
            page_ids = await client.get_all_page_ids()
        else:
            logger.info(f"Fetching {len(page_ids)} specified pages…")

        pages = await client.get_pages_batch(page_ids)

    logger.info(f"Successfully fetched {len(pages)} pages")
    return pages
