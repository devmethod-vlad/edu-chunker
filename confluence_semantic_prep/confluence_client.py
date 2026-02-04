from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from .settings import Settings

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class PageMeta:
    page_id: str
    title: str
    space_key: str | None
    version: int | None
    # ISO-like timestamp when the page was last modified. Comes from
    # the page's version.when field or history.lastUpdated.when.
    last_modified: str | None
    webui: str | None


@dataclass(slots=True, frozen=True)
class PageFull(PageMeta):
    body_view_html: str


class ConfluenceClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.base = settings.confluence_base_url.rstrip("/")
        self.api = settings.confluence_rest_api_prefix.rstrip("/")

        auth = None
        if not settings.confluence_use_anonymous and settings.confluence_username and settings.confluence_api_token:
            auth = (settings.confluence_username, settings.confluence_api_token)

        limits = httpx.Limits(
            max_connections=max(10, settings.confluence_concurrency * 2),
            max_keepalive_connections=max(10, settings.confluence_concurrency),
            keepalive_expiry=30,
        )

        self.client = httpx.AsyncClient(
            base_url=self.base,
            timeout=httpx.Timeout(settings.confluence_timeout_seconds),
            limits=limits,
            follow_redirects=True,
            auth=auth,
            headers={"Accept": "application/json"},
        )

    async def aclose(self) -> None:
        await self.client.aclose()

    async def list_all_pages(self) -> list[PageMeta]:
        results: list[PageMeta] = []
        start = 0
        limit = self.settings.confluence_page_list_limit
        expand = "space,version,_links,history"

        while True:
            url = f"{self.api}/content"
            params = {"type": "page", "status": "current", "limit": str(limit), "start": str(start), "expand": expand}
            r = await self.client.get(url, params=params)
            if r.status_code in (401, 403):
                logger.warning("Cannot list pages (status=%s). Provide CONFLUENCE_PAGE_ID or enable auth.", r.status_code)
                break
            r.raise_for_status()
            data = r.json()
            items = data.get("results", []) or []

            for it in items:
                pid = str(it.get("id"))
                title = it.get("title") or ""
                space_key = (it.get("space") or {}).get("key") if isinstance(it.get("space"), dict) else None
                version = None
                last_modified: str | None = None
                version_info = it.get("version")
                if isinstance(version_info, dict):
                    vn = version_info.get("number")
                    version = int(vn) if isinstance(vn, int) or (isinstance(vn, str) and vn.isdigit()) else None
                    when = version_info.get("when")
                    if isinstance(when, str) and when.strip():
                        last_modified = when.strip()
                # Fallback: history.lastUpdated.when
                if not last_modified:
                    hist = it.get("history") or {}
                    if isinstance(hist, dict):
                        last_upd = hist.get("lastUpdated") or {}
                        if isinstance(last_upd, dict):
                            when = last_upd.get("when")
                            if isinstance(when, str) and when.strip():
                                last_modified = when.strip()
                webui = (it.get("_links") or {}).get("webui") if isinstance(it.get("_links"), dict) else None
                results.append(PageMeta(page_id=pid, title=title, space_key=space_key, version=version, last_modified=last_modified, webui=webui))

            if len(items) < limit:
                break
            start += limit

        logger.info("Listed %d pages", len(results))
        return results

    async def fetch_page_view(self, page_id: str) -> PageFull | None:
        url = f"{self.api}/content/{page_id}"
        params = {"expand": "body.view,space,version,_links,history"}
        r = await self.client.get(url, params=params)
        if r.status_code in (401, 403, 404):
            logger.info("Skip page %s (status=%s)", page_id, r.status_code)
            return None
        r.raise_for_status()
        data = r.json()

        title = data.get("title") or ""
        space_key = (data.get("space") or {}).get("key") if isinstance(data.get("space"), dict) else None
        version = None
        last_modified: str | None = None
        version_info = data.get("version")
        if isinstance(version_info, dict):
            vn = version_info.get("number")
            version = int(vn) if isinstance(vn, int) or (isinstance(vn, str) and vn.isdigit()) else None
            when = version_info.get("when")
            if isinstance(when, str) and when.strip():
                last_modified = when.strip()
        # Fallback to history.lastUpdated.when
        if not last_modified:
            hist = data.get("history") or {}
            if isinstance(hist, dict):
                last_upd = hist.get("lastUpdated") or {}
                if isinstance(last_upd, dict):
                    when = last_upd.get("when")
                    if isinstance(when, str) and when.strip():
                        last_modified = when.strip()
        webui = (data.get("_links") or {}).get("webui") if isinstance(data.get("_links"), dict) else None

        html = ""
        body = data.get("body") or {}
        if isinstance(body, dict):
            view = body.get("view") or {}
            if isinstance(view, dict):
                html = view.get("value") or ""

        if not html.strip():
            logger.info("Page %s has empty body.view", page_id)
            return None

        return PageFull(page_id=str(page_id), title=title, space_key=space_key, version=version, last_modified=last_modified, webui=webui, body_view_html=html)