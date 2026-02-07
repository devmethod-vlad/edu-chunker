from __future__ import annotations

"""Pipeline for crawling Confluence pages and producing chunked output.

This module orchestrates the entire extraction process: it fetches pages via
the Confluence client, parses them into block drafts, splits oversized
blocks, chunks the resulting blocks, and writes the final payload to disk.
The pipeline here is largely a copy of the original implementation from
``edu-chunker`` but has been updated to handle the new ``attributes`` field
on blocks. When serialising blocks for output we include the ``attributes``
mapping so that downstream consumers can reconstruct the original HTML
structure faithfully.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .block_splitter import BlockSplitter
from .chunker import Chunker
from .confluence_client import ConfluenceClient, PageFull
from .html_parser import HTMLToBlockDraftsParser
from .models import Block, Chunk, Heading
from .settings import Settings
from .tokenizer import TokenCounter

logger = logging.getLogger(__name__)


def _h_to_dict(h: Heading) -> Dict[str, Any]:
    """Serialise a Heading dataclass into a plain dictionary."""
    return {"level": h.level, "text": h.text, "html_id": h.html_id}


def _block_to_dict(b: Block) -> Dict[str, Any]:
    """Serialise a Block dataclass into a dictionary for JSON output.

    In addition to the original fields, this function now includes the
    ``attributes`` mapping which captures HTML attributes present on the
    source tag. Without this, clients would lose information such as CSS
    classes, styles or table configuration that may be necessary to
    reassemble valid HTML when rendering chunks.
    """
    return {
        "block_index": b.block_index,
        "block_id": b.block_id,
        "block_type": b.block_type,
        "heading_path_full": [_h_to_dict(x) for x in b.heading_path_full],
        "heading_path_text": [_h_to_dict(x) for x in b.heading_path_text],
        "nearest_heading_id": b.nearest_heading_id,
        "char_start": b.char_start,
        "char_end": b.char_end,
        "token_est": b.token_est,
        "html": b.html,
        "markdown": b.markdown,
        "text": b.text,
        "parent_block_id": b.parent_block_id,
        "attributes": b.attributes,
        "page_last_modified": b.page_last_modified,
        "page_version": b.page_version,
    }


def _chunk_to_dict(c: Chunk) -> Dict[str, Any]:
    """Serialise a Chunk dataclass into a dictionary for JSON output."""
    return {
        "chunk_id": c.chunk_id,
        "page_id": c.page_id,
        "space_key": c.space_key,
        "block_indices": c.block_indices,
        "heading_path_full": [_h_to_dict(x) for x in c.heading_path_full],
        "nearest_heading_id": c.nearest_heading_id,
        "char_start": c.char_start,
        "char_end": c.char_end,
        "token_est": c.token_est,
        "html": c.html,
        "markdown": c.markdown,
        "chunk_text": c.chunk_text,
        "text_for_embedding": c.text_for_embedding,
        "page_last_modified": c.page_last_modified,
        "page_version": c.page_version,
    }


async def run_pipeline(settings: Settings) -> None:
    """Entrypoint for the Confluence semantic chunker.

    Given a Settings object, this function will:
    1. Create a token counter according to the specified strategy.
    2. Fetch the list of pages to process (all pages or filtered by ID).
    3. For each page, parse its HTML into block drafts, split oversized
       drafts, chunk the resulting blocks and collect the results.
    4. Assemble a payload capturing metadata, blocks and chunks for all
       processed pages and write it to JSON in the configured output
       directory.

    The function includes progress bar support when enabled via settings.
    """
    token_counter = TokenCounter.from_settings(settings.token_count_strategy, settings.tokenizer_local_path)

    client = ConfluenceClient(settings)
    try:
        # Determine which pages to process
        if settings.confluence_page_ids:
            page_ids = settings.confluence_page_ids
            logger.info("Using CONFLUENCE_PAGE_ID filter (%d ids)", len(page_ids))
        else:
            metas = await client.list_all_pages()
            page_ids = [m.page_id for m in metas]
            logger.info("Crawling portal (%d pages from listing)", len(page_ids))

        sem = asyncio.Semaphore(settings.confluence_concurrency)

        async def fetch_one(pid: str) -> PageFull | None:
            async with sem:
                return await client.fetch_page_view(pid)

        # Launch concurrent fetch tasks
        tasks = [asyncio.create_task(fetch_one(pid)) for pid in page_ids]
        pages: List[PageFull] = []

        # Optional progress bar for fetching pages
        if settings.enable_progress_bar:
            from tqdm import tqdm  # type: ignore
            fetch_pbar = tqdm(total=len(tasks), desc="Fetching pages", unit="page")
        else:
            fetch_pbar = None

        for coro in asyncio.as_completed(tasks):
            p = await coro
            if p is not None:
                pages.append(p)
            if fetch_pbar:
                fetch_pbar.update(1)

        if fetch_pbar:
            fetch_pbar.close()

        parser = HTMLToBlockDraftsParser(token_counter, settings.ignore_tags, settings.heading_levels_for_text)
        splitter = BlockSplitter(token_counter, settings.ignore_tags)
        chunker = Chunker(
            token_counter=token_counter,
            splitter=splitter,
            chunk_size_tokens=settings.chunk_size_tokens,
            chunk_min_tokens=settings.chunk_min_tokens,
            add_page_prefix=settings.add_page_prefix,
            add_section_prefix=settings.add_section_prefix,
            heading_levels_for_text=settings.heading_levels_for_text,
            chunk_overlap_tokens=settings.chunk_overlap_tokens,
        )

        out_pages: List[Dict[str, Any]] = []

        # Optional progress bar for processing pages
        if settings.enable_progress_bar:
            from tqdm import tqdm  # type: ignore
            process_pbar = tqdm(total=len(pages), desc="Processing pages", unit="page")
        else:
            process_pbar = None

        for p in pages:
            drafts = parser.parse(p.body_view_html)
            chunked = chunker.chunk_page(
                p.page_id,
                p.space_key,
                p.title,
                drafts,
                p.last_modified,
                p.version,
            )

            out_pages.append(
                {
                    "page": {
                        "page_id": p.page_id,
                        "title": p.title,
                        "space_key": p.space_key,
                        "version": p.version,
                        "last_modified": p.last_modified,
                        "webui": p.webui,
                    },
                    "page_text_normalized": chunked.page_text_normalized,
                    "blocks": [_block_to_dict(b) for b in chunked.blocks],
                    "chunks": [_chunk_to_dict(c) for c in chunked.chunks],
                }
            )

            if process_pbar:
                process_pbar.update(1)

        if process_pbar:
            process_pbar.close()

        # Prepare output payload
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
        out_path = Path(settings.output_dir) / f"{settings.output_prefix}_{ts}.json"
        payload: Dict[str, Any] = {
            "generated_at": ts,
            "settings": {
                "chunk_size_tokens": settings.chunk_size_tokens,
                "chunk_min_tokens": settings.chunk_min_tokens,
                "chunk_overlap_tokens": settings.chunk_overlap_tokens,
                "heading_levels_for_text": settings.heading_levels_for_text,
                "add_page_prefix": settings.add_page_prefix,
                "add_section_prefix": settings.add_section_prefix,
                "ignore_tags": settings.ignore_tags,
                "token_count_strategy": settings.token_count_strategy,
            },
            "pages_count": len(out_pages),
            "pages": out_pages,
        }
        # Ensure output directory exists
        Path(settings.output_dir).mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Wrote %s", out_path)

    finally:
        await client.aclose()