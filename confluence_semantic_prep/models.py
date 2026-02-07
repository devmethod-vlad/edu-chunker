from __future__ import annotations

"""Data models used by the Confluence semantic chunker.

This module defines the immutable ``Heading`` class to represent
headings encountered in a page, along with the ``Block`` and ``Chunk``
classes that carry parsed content through the pipeline. The ``Block``
class has been extended with a generic ``attributes`` mapping to
capture the HTML attributes on the originating element. This is
particularly useful for reconstructing valid HTML structures when
aggregating list items and table rows back into a full representation.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(slots=True, frozen=True)
class Heading:
    level: int
    text: str
    html_id: Optional[str] = None


@dataclass(slots=True)
class Block:
    """A minimal unit of content extracted from a Confluence page.

    Each block stores its position within the page, the raw HTML,
    markdown and plain text representations, and links to its
    hierarchical context (heading path and parent block). A new
    ``attributes`` field has been introduced to capture any HTML
    attributes present on the source tag. This allows callers to
    preserve presentation hints such as table borders or list classes
    when reconstructing partial structures in a chunk.
    """

    # Index from start of the page AFTER all splitting
    block_index: int

    # 'EDU:{page_id}-{block_index}'
    block_id: str

    # Parent/root tag of the block HTML (p, h2, div, ul_li/ol_li, table_row, blockquote, pre)
    block_type: str

    # Heading hierarchy context at this block position
    heading_path_full: List[Heading]
    heading_path_text: List[Heading]

    # Nearest preceding heading id (for navigation)
    nearest_heading_id: Optional[str]

    # Offsets inside ``page_text_normalized`` (computed after final blocks are built)
    char_start: int
    char_end: int

    # Estimated tokens for ``text`` (plain normalized)
    token_est: int

    # Minimal valid HTML for this block (suitable for markdownify)
    html: str

    # Markdown representation of ``html``
    markdown: str

    # Normalized plain text
    text: str

    # Identifier of the parent block when this block is part of a grouped
    # structure such as a list or table. For the first element in a group
    # this is None. Child elements reference the block_id of the parent.
    parent_block_id: Optional[str] = None

    # Mapping of HTML attributes present on the original tag. Keys and
    # values are both strings. When an attribute had a list of values in
    # the source (e.g. ``class="a b"``) the values are joined with a
    # single space.
    attributes: Dict[str, str] = field(default_factory=dict)

    # Metadata about the Confluence page this block originates from. The
    # timestamp string comes from the page's version information ("when"
    # field) and is left as-is (e.g. ``"2023-05-29T14:36:52.854+0000"``). The
    # version number is taken from the page's ``version.number``. Both fields
    # may be None when unavailable.
    page_last_modified: Optional[str] = None
    page_version: Optional[int] = None


@dataclass(slots=True)
class Chunk:
    """A contiguous collection of blocks forming a semantic unit.

    Chunks collect a range of blocks into a single object along with
    aggregated HTML and markdown representations. The ``text_for_embedding``
    field contains optional prefixes and is what downstream embedder
    models use for generating embeddings.
    """

    page_id: str
    space_key: Optional[str]

    # 'EDU:{page_id}:{first_block}-{last_block}'
    chunk_id: str

    # Indices of blocks included into this chunk
    block_indices: List[int]

    # Full heading hierarchy for the chunk (taken from first block)
    heading_path_full: List[Heading]

    # Nearest heading id for navigation (from first block)
    nearest_heading_id: Optional[str]

    # Offsets inside ``page_text_normalized`` (computed from first/last block)
    char_start: int
    char_end: int

    # Estimated tokens for ``text_for_embedding``
    token_est: int

    html: str
    markdown: str
    chunk_text: str

    # Text that later goes to embedder: [PAGE]/[SECTION]/[TEXT] (depending on settings)
    text_for_embedding: str

    # Metadata from the Confluence page. Each chunk inherits the same
    # last modified timestamp and version as its constituent blocks.
    page_last_modified: Optional[str] = None
    page_version: Optional[int] = None