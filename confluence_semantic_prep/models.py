from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class Heading:
    level: int
    text: str
    html_id: str | None = None


@dataclass(slots=True)
class Block:
    # Index from start of the page AFTER all splitting
    block_index: int

    # 'EDU:{page_id}-{block_index}'
    block_id: str

    # Parent/root tag of the block HTML (p, h2, div, ul_li/ol_li, table_row, blockquote, pre)
    block_type: str

    # Heading hierarchy context at this block position
    heading_path_full: list[Heading]
    heading_path_text: list[Heading]

    # Nearest preceding heading id (for navigation)
    nearest_heading_id: str | None

    # Offsets inside `page_text_normalized` (computed after final blocks are built)
    char_start: int
    char_end: int

    # Estimated tokens for `text` (plain normalized)
    token_est: int

    # Minimal valid HTML for this block (suitable for markdownify)
    html: str

    # Markdown representation of `html`
    markdown: str

    # Normalized plain text
    text: str

    # Identifier of the parent block when this block is part of a grouped
    # structure such as a list or table. For the first element in a group
    # this is None. Child elements reference the block_id of the parent.
    parent_block_id: str | None = None

    # Metadata about the Confluence page this block originates from. The
    # timestamp string comes from the page's version information ("when"
    # field) and is left as-is (e.g. "2023-05-29T14:36:52.854+0000"). The
    # version number is taken from the page's version.number. Both fields
    # may be None when unavailable.
    page_last_modified: str | None = None
    page_version: int | None = None


@dataclass(slots=True)
class Chunk:
    page_id: str
    space_key: str | None

    # 'EDU:{page_id}:{first_block}-{last_block}'
    chunk_id: str

    # Indices of blocks included into this chunk
    block_indices: list[int]

    # Full heading hierarchy for the chunk (taken from first block)
    heading_path_full: list[Heading]

    # Nearest heading id for navigation (from first block)
    nearest_heading_id: str | None

    # Offsets inside `page_text_normalized` (computed from first/last block)
    char_start: int
    char_end: int

    # Estimated tokens for `text_for_embedding`
    token_est: int

    html: str
    markdown: str
    chunk_text: str

    # Text that later goes to embedder: [PAGE]/[SECTION]/[TEXT] (depending on settings)
    text_for_embedding: str

    # Metadata from the Confluence page. Each chunk inherits the same
    # last modified timestamp and version as its constituent blocks.
    page_last_modified: str | None = None
    page_version: int | None = None