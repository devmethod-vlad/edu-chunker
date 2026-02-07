from __future__ import annotations

"""HTML parsing utilities for extracting logical blocks from Confluence pages.

This module exposes a ``HTMLToBlockDraftsParser`` class which traverses the
DOM and produces a series of ``BlockDraft`` objects, each representing a
minimal semantic unit suitable for further splitting and chunking. The
parser has been extended to capture the attributes of the originating
HTML element on each draft. This allows downstream components to
reconstruct valid parent structures (such as lists and tables) even when
only fragments are present in a chunk.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Iterable, Iterator, Dict, List, Optional

from bs4 import BeautifulSoup, Tag
from markdownify import markdownify as md

from .models import Heading
from .tokenizer import TokenCounter
from .utils_text import normalize_text
from .settings import _split_csv

logger = logging.getLogger(__name__)

# Default block-level tags. These are always considered block boundaries.
_DEFAULT_BLOCK_TAGS: set[str] = {
    "p",
    "div",
    "blockquote",
    "pre",
    "ul",
    "ol",
    "table",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    # Additional HTML5/Confluence container tags. These are promoted to
    # block-level to ensure semantic boundaries are respected.
    "section",
    "header",
    "footer",
    "article",
    "aside",
    "nav",
    "main",
    "figure",
    "figcaption",
}

# Merge environment specified block tags. Users may supply a comma separated
# list via BLOCK_TAGS to extend this set. The defaults are always included.
_env_block_tags = _split_csv(os.getenv("BLOCK_TAGS"))
BLOCK_TAGS: set[str] = {t.lower() for t in _env_block_tags} | _DEFAULT_BLOCK_TAGS

# Wrapper tags act as purely structural containers. When a wrapper tag has no
# own text content it will be skipped so that its children can become
# independent blocks. Should the wrapper contain text it will be treated as a
# regular block. New tags can be added here when Confluence introduces new
# container elements.
WRAPPER_TAGS: set[str] = {
    "div",
    "section",
    "article",
    "header",
    "footer",
    "main",
    "aside",
    "nav",
}


@dataclass(slots=True, frozen=True)
class BlockDraft:
    """Intermediate representation of a parsed block.

    Each draft captures the minimal HTML, markdown and plain text for a block
    along with its estimated token count and heading context. Drafts also
    optionally carry a ``parent_uid`` when they belong to a list or table
    group. The ``parent_uid`` is later resolved to the actual parent block
    identifier in the chunker. A ``attributes`` mapping records the raw
    attributes present on the originating HTML tag so that the chunker can
    rebuild parent structures with the correct properties.
    """

    block_type: str
    html: str
    markdown: str
    text: str
    token_est: int
    heading_path_full: List[Heading]
    heading_path_text: List[Heading]
    nearest_heading_id: Optional[str]
    parent_uid: Optional[str] = None
    # Unique identifier for the group this draft belongs to (lists/tables). The
    # same group_uid is assigned to the parent and all its children. This
    # allows the chunker to resolve parent-child relationships. For
    # stand-alone blocks this remains None.
    group_uid: Optional[str] = None
    # Capture attributes on the originating tag. Keys and values are strings.
    attributes: Dict[str, str] = field(default_factory=dict)


def markdownify_html(html: str) -> str:
    """Convert a snippet of HTML into GitHub-flavoured Markdown.

    The markdownify library is used with ATX heading style to ensure headings
    render correctly. Leading and trailing whitespace is stripped.
    """
    return md(html, heading_style="ATX").strip()


def heading_level(tag_name: str) -> Optional[int]:
    """Return the numeric level of an HTML heading tag or None if not a heading."""
    if len(tag_name) == 2 and tag_name[0] == "h" and tag_name[1].isdigit():
        lvl = int(tag_name[1])
        return lvl if 1 <= lvl <= 6 else None
    return None


def heading_id(tag: Tag) -> Optional[str]:
    """Extract the id attribute of a heading tag if present and non-empty."""
    hid = tag.get("id")
    return hid.strip() if isinstance(hid, str) and hid.strip() else None


def condense_heading_path_for_text(full_path: List[Heading], limit: int) -> List[Heading]:
    """Reduce a heading path to the most relevant levels for embedding text.

    This takes up to ``limit`` headings starting from the closest (least
    important) and moving up to more important headings. See the original
    implementation for details.
    """
    if limit <= 0 or not full_path:
        return []
    picked: List[Heading] = []
    last_level = 10
    for h in reversed(full_path):
        if h.level < last_level:
            picked.append(h)
            last_level = h.level
            if len(picked) >= limit:
                break
    return picked  # least important -> most important


def _has_own_text(tag: Tag) -> bool:
    """Return True if the tag has non-trivial text directly under it (not
    just inside child block elements).

    This correctly distinguishes between a wrapper ``<div><p>text</p></div>``
    (no own text) and ``<div>some text<p>more</p></div>`` (has own text).
    The old ``is_wrapper_tag`` used ``get_text()`` which includes ALL
    descendant text, so ``<div><p>hello</p></div>`` was incorrectly
    considered to have text.
    """
    for child in tag.children:
        # NavigableString (text node) that is not just whitespace
        if not isinstance(child, Tag):
            if child.string and child.string.strip():
                return True
    return False


def is_wrapper_tag(tag: Tag) -> bool:
    """Return True if the given tag is a structural wrapper with no own text.

    Wrapper tags are skipped during block detection so that their children
    surface as independent blocks. A tag qualifies as a wrapper if its name
    appears in ``WRAPPER_TAGS``, it has no significant text of its own and it
    contains at least one child block-level element.
    """
    name = tag.name.lower()
    if name not in WRAPPER_TAGS:
        return False
    # Check only DIRECT text nodes (not descendant text) to avoid false
    # positives on wrappers whose children carry all the content.
    if _has_own_text(tag):
        return False
    # Skip wrappers that simply nest other blocks
    for child in tag.find_all(recursive=False):
        if isinstance(child, Tag) and child.name.lower() in BLOCK_TAGS:
            return True
    return False


def has_descendant_block(tag: Tag) -> bool:
    """Check if the tag contains any descendant that is a block-level element."""
    for d in tag.descendants:
        if isinstance(d, Tag) and d.name.lower() in BLOCK_TAGS:
            return True
    return False


def _direct_table_rows(table: Tag) -> List[Tag]:
    """Return ``<tr>`` elements that belong directly to *table*, ignoring
    rows inside nested tables.

    Handles ``<thead>``, ``<tbody>`` and ``<tfoot>`` wrappers that parsers
    (especially *lxml*) may insert automatically.
    """
    rows: List[Tag] = []
    for child in table.find_all(recursive=False):
        if not isinstance(child, Tag):
            continue
        if child.name == "tr":
            rows.append(child)
        elif child.name in ("thead", "tbody", "tfoot", "colgroup"):
            for tr in child.find_all("tr", recursive=False):
                rows.append(tr)
    return rows


def _row_has_th(tr: Tag) -> bool:
    """Check if a ``<tr>`` contains direct ``<th>`` children."""
    return bool(tr.find_all("th", recursive=False))


def _row_has_td(tr: Tag) -> bool:
    """Check if a ``<tr>`` contains direct ``<td>`` children."""
    return bool(tr.find_all("td", recursive=False))


class HTMLToBlockDraftsParser:
    """Parser for converting Confluence HTML into a list of block drafts."""

    def __init__(self, token_counter: TokenCounter, ignore_tags: Iterable[str], heading_levels_for_text: int) -> None:
        self.token_counter = token_counter
        self.ignore_tags = {t.strip().lower() for t in ignore_tags if t.strip()}
        self.heading_levels_for_text = heading_levels_for_text
        # internal counter used to generate unique parent UIDs for lists and tables
        self._uid_seq: int = 0

    def _next_uid(self, prefix: str) -> str:
        """Generate a monotonically increasing unique identifier for grouping."""
        uid = f"{prefix}_{self._uid_seq}"
        self._uid_seq += 1
        return uid

    def parse(self, html: str) -> List[BlockDraft]:
        """Parse arbitrary HTML into a sequence of block drafts.

        The resulting drafts capture the minimal valid HTML, markdown and plain
        text for each block-level element. Lists are broken into individual
        items, tables are broken into rows and nested wrappers are flattened
        where appropriate. Each draft retains heading context to enable later
        reconstruction. Attributes on the original tags are preserved on the
        resulting drafts for later reuse.
        """
        soup = BeautifulSoup(f"<div id='__root__'>{html}</div>", "lxml")
        root = soup.find("div", {"id": "__root__"})
        if root is None:
            return []

        # Remove ignored tags entirely
        for tname in self.ignore_tags:
            for bad in root.find_all(tname):
                bad.decompose()

        drafts: List[BlockDraft] = []

        heading_stack: List[Heading] = []  # most important -> least important
        last_heading_id: Optional[str] = None

        for node in self.walk_effective_blocks(root):
            name = node.name.lower()
            lvl = heading_level(name)

            heading_full = list(heading_stack)
            heading_text = condense_heading_path_for_text(heading_full, self.heading_levels_for_text)

            # Capture attributes on this node. Values that are lists (e.g. class)
            # are joined with a space. Non-string values are converted to strings.
            attrs: Dict[str, str] = {}
            for k, v in node.attrs.items():
                if isinstance(v, list):
                    attrs[k.lower()] = " ".join(str(x) for x in v)
                else:
                    attrs[k.lower()] = str(v)

            # Lists: break each list into individual items. Each item is
            # considered a block of type ``ul_li`` or ``ol_li``. A unique
            # ``list_uid`` identifies which items belong to the same list.
            if name in {"ul", "ol"}:
                list_uid = self._next_uid(name)
                first = True
                for li in node.find_all("li", recursive=False):
                    html_item = self.wrap_list_item_html(name, li)
                    txt = normalize_text(BeautifulSoup(html_item, "lxml").get_text(" ", strip=True))
                    if not txt:
                        continue
                    block_type = f"{name}_li"
                    # First item has no parent, subsequent items reference the uid
                    parent_uid = None if first else list_uid
                    group_uid = list_uid
                    first = False
                    drafts.append(
                        BlockDraft(
                            block_type=block_type,
                            html=html_item,
                            markdown=markdownify_html(html_item),
                            text=txt,
                            token_est=self.token_counter.count(txt),
                            heading_path_full=heading_full,
                            heading_path_text=heading_text,
                            nearest_heading_id=last_heading_id,
                            parent_uid=parent_uid,
                            group_uid=group_uid,
                            attributes=attrs,
                        )
                    )
                continue

            # Tables: split into row-level blocks. Each row gets a type
            # ``table_row`` and shares a common ``table_uid``. The first row
            # establishes the parent and subsequent rows reference it. A
            # ``group_uid`` is assigned to all rows for later association.
            if name == "table":
                table_uid = self._next_uid("table")
                row_drafts = self.table_to_row_drafts(
                    node,
                    heading_full,
                    heading_text,
                    last_heading_id,
                    table_uid,
                    attrs,
                )
                drafts.extend(row_drafts)
                continue

            # Generic block: emit directly
            html_block = str(node)
            txt = normalize_text(node.get_text(" ", strip=True))
            if txt:
                drafts.append(
                    BlockDraft(
                        block_type=name,
                        html=html_block,
                        markdown=markdownify_html(html_block),
                        text=txt,
                        token_est=self.token_counter.count(txt),
                        heading_path_full=heading_full,
                        heading_path_text=heading_text,
                        nearest_heading_id=last_heading_id,
                        attributes=attrs,
                    )
                )

            # Maintain heading stack. After emitting a heading we push it onto
            # the stack, removing less important headings above it.
            if lvl is not None:
                heading_stack = [h for h in heading_stack if h.level < lvl]
                new_h = Heading(level=lvl, text=txt, html_id=heading_id(node))
                heading_stack.append(new_h)
                last_heading_id = new_h.html_id or last_heading_id

        return drafts

    def wrap_list_item_html(self, list_tag: str, li: Tag) -> str:
        """Wrap the contents of a list item in its parent list tag.

        This ensures that each list item draft has valid HTML with exactly
        one list item inside the list container.
        """
        inner = "".join(str(x) for x in li.contents).strip()
        return f"<{list_tag}><li>{inner}</li></{list_tag}>"

    def table_to_row_drafts(
        self,
        table: Tag,
        heading_full: List[Heading],
        heading_text: List[Heading],
        nearest_heading_id: Optional[str],
        table_uid: str,
        table_attrs: Dict[str, str],
    ) -> List[BlockDraft]:
        """Split a table into row-level BlockDrafts.

        A unique ``table_uid`` is used to link all rows belonging to the same
        table. The first row in the returned list will have ``parent_uid`` set
        to ``None`` and all subsequent rows will reference ``table_uid``. All
        rows share the same ``group_uid`` value for later association. Each
        row's HTML includes the header row (if present) to preserve
        column names. The block type for rows is ``table_row``.
        Attributes on the table itself are captured via ``table_attrs`` and
        propagated to each draft so that the chunker can reapply them when
        reconstructing the full table.
        """
        header_tr = None
        for tr in _direct_table_rows(table):
            if _row_has_th(tr):
                header_tr = tr
                break

        rows = [tr for tr in _direct_table_rows(table) if _row_has_td(tr)]
        out: List[BlockDraft] = []
        first = True
        for tr in rows:
            h_html = str(header_tr) if header_tr is not None else ""
            row_html = str(tr)
            html_block = "<table>" + (h_html or "") + row_html + "</table>"
            txt = normalize_text(BeautifulSoup(html_block, "lxml").get_text(" ", strip=True))
            if not txt:
                continue
            parent_uid = None if first else table_uid
            group_uid = table_uid
            first = False
            out.append(
                BlockDraft(
                    block_type="table_row",
                    html=html_block,
                    markdown=markdownify_html(html_block),
                    text=txt,
                    token_est=self.token_counter.count(txt),
                    heading_path_full=heading_full,
                    heading_path_text=heading_text,
                    nearest_heading_id=nearest_heading_id,
                    parent_uid=parent_uid,
                    group_uid=group_uid,
                    attributes=table_attrs,
                )
            )
        return out

    def walk_effective_blocks(self, root: Tag) -> Iterator[Tag]:
        """Yield candidate block elements by traversing the DOM depth-first.

        The algorithm flattens nested wrappers and only yields tags whose names
        appear in ``BLOCK_TAGS``. Wrapper tags with no own text (see
        ``is_wrapper_tag``) are skipped so that their children can be emitted
        as independent blocks.

        CRITICAL FIX: When a non-wrapper block tag is yielded its children
        are NOT pushed onto the stack. The old code unconditionally pushed
        children for every node, which caused the same content to appear
        both inside a parent block AND as a separate child block
        (e.g. ``<blockquote><p>text</p></blockquote>`` would produce two
        drafts with identical text). Now only wrapper/non-block nodes
        have their children explored.
        """
        stack: List[Tag] = [root]
        while stack:
            node = stack.pop()
            name = node.name.lower()

            # The artificial root wrapper is always descended into.
            if node is root:
                children = [c for c in node.find_all(recursive=False) if isinstance(c, Tag)]
                for c in reversed(children):
                    stack.append(c)
                continue

            # Non-block inline/unknown tags: descend to find nested blocks.
            if name not in BLOCK_TAGS:
                children = [c for c in node.find_all(recursive=False) if isinstance(c, Tag)]
                for c in reversed(children):
                    stack.append(c)
                continue

            # Wrapper tags (div, section, etc.) with no own text that merely
            # wrap other blocks: skip and descend into children.
            if name in WRAPPER_TAGS and (is_wrapper_tag(node) or has_descendant_block(node)):
                children = [c for c in node.find_all(recursive=False) if isinstance(c, Tag)]
                for c in reversed(children):
                    stack.append(c)
                continue

            # This is a non-wrapper block-level tag: yield it and do NOT
            # push its children. The caller (parse) will handle the node's
            # inner structure (e.g. splitting <ul> into <li> items).
            yield node