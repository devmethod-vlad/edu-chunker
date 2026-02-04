from __future__ import annotations

"""HTML parsing utilities for extracting logical blocks from Confluence pages.

The HTML returned by Confluence (``body.view``) contains a mix of inline and
block level elements. To perform semantic chunking we need to identify and
isolate meaningful blocks such as paragraphs, headings, lists and tables. This
module exposes a `HTMLToBlockDraftsParser` class which traverses the DOM and
produces a series of ``BlockDraft`` objects, each representing a minimal
semantic unit suitable for further splitting and chunking.

Several aspects of the parsing process are configurable via environment
variables. Block-level tags may be extended by setting ``BLOCK_TAGS`` to a
comma separated list. Additional wrapper tags beyond ``div`` are handled and
ignored when they only serve as containers. A progress bar may be enabled via
``ENABLE_PROGRESS_BAR`` in the settings module, however progress tracking is
implemented in the pipeline rather than in this parser.
"""

import logging
import os
from dataclasses import dataclass
from typing import Iterable, Iterator

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
    identifier in the chunker.
    """

    block_type: str
    html: str
    markdown: str
    text: str
    token_est: int
    heading_path_full: list[Heading]
    heading_path_text: list[Heading]
    nearest_heading_id: str | None
    parent_uid: str | None = None
    # Unique identifier for the group this draft belongs to (lists/tables). The
    # same group_uid is assigned to the parent and all its children. This
    # allows the chunker to resolve parent-child relationships. For
    # stand-alone blocks this remains None.
    group_uid: str | None = None


def markdownify_html(html: str) -> str:
    """Convert a snippet of HTML into GitHub-flavoured Markdown.

    The markdownify library is used with ATX heading style to ensure headings
    render correctly. Leading and trailing whitespace is stripped.
    """
    return md(html, heading_style="ATX").strip()


def heading_level(tag_name: str) -> int | None:
    """Return the numeric level of an HTML heading tag or None if not a heading."""
    if len(tag_name) == 2 and tag_name[0] == "h" and tag_name[1].isdigit():
        lvl = int(tag_name[1])
        return lvl if 1 <= lvl <= 6 else None
    return None


def heading_id(tag: Tag) -> str | None:
    """Extract the id attribute of a heading tag if present and non-empty."""
    hid = tag.get("id")
    return hid.strip() if isinstance(hid, str) and hid.strip() else None


def condense_heading_path_for_text(full_path: list[Heading], limit: int) -> list[Heading]:
    """Reduce a heading path to the most relevant levels for embedding text.

    This takes up to ``limit`` headings starting from the closest (least
    important) and moving up to more important headings. See the original
    implementation for details.
    """
    if limit <= 0 or not full_path:
        return []
    picked: list[Heading] = []
    last_level = 10
    for h in reversed(full_path):
        if h.level < last_level:
            picked.append(h)
            last_level = h.level
            if len(picked) >= limit:
                break
    return picked  # least important -> most important


def is_wrapper_tag(tag: Tag) -> bool:
    """Return True if the given tag is a structural wrapper with no own text.

    Wrapper tags are skipped during block detection so that their children
    surface as independent blocks. A tag qualifies as a wrapper if its name
    appears in ``WRAPPER_TAGS``, it has no significant text of its own and it
    contains at least one child block-level element. If any text is present
    directly under the tag the wrapper is preserved as a block.
    """
    name = tag.name.lower()
    if name not in WRAPPER_TAGS:
        return False
    # If the wrapper contains its own non-empty text, it should not be skipped
    # because it conveys content that would otherwise be lost.
    if normalize_text(tag.get_text(" ", strip=True)):
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

    def parse(self, html: str) -> list[BlockDraft]:
        """Parse arbitrary HTML into a sequence of block drafts.

        The resulting drafts capture the minimal valid HTML, markdown and plain
        text for each block-level element. Lists are broken into individual
        items, tables are broken into rows and nested wrappers are flattened
        where appropriate. Each draft retains heading context to enable later
        reconstruction.
        """
        soup = BeautifulSoup(f"<div id='__root__'>{html}</div>", "lxml")
        root = soup.find("div", {"id": "__root__"})
        if root is None:
            return []

        # Remove ignored tags entirely
        for tname in self.ignore_tags:
            for bad in root.find_all(tname):
                bad.decompose()

        drafts: list[BlockDraft] = []

        heading_stack: list[Heading] = []  # most important -> least important
        last_heading_id: str | None = None

        for node in self.walk_effective_blocks(root):
            name = node.name.lower()
            lvl = heading_level(name)

            heading_full = list(heading_stack)
            heading_text = condense_heading_path_for_text(heading_full, self.heading_levels_for_text)

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
                )
                drafts.extend(row_drafts)
                continue

            # Skip container wrappers that merely wrap other blocks
            if name in WRAPPER_TAGS and (is_wrapper_tag(node) or has_descendant_block(node)):
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
        heading_full: list[Heading],
        heading_text: list[Heading],
        nearest_heading_id: str | None,
        table_uid: str,
    ) -> list[BlockDraft]:
        """Split a table into row-level BlockDrafts.

        A unique ``table_uid`` is used to link all rows belonging to the same
        table. The first row in the returned list will have ``parent_uid`` set
        to ``None`` and all subsequent rows will reference ``table_uid``. All
        rows share the same ``group_uid`` value for later association. Each
        row's HTML includes the header row (if present) to preserve
        column names. The block type for rows is ``table_row``.
        """
        header_tr = None
        for tr in table.find_all("tr"):
            if tr.find_all("th"):
                header_tr = tr
                break

        rows = [tr for tr in table.find_all("tr") if tr.find_all("td")]
        out: list[BlockDraft] = []
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
                )
            )
        return out

    def walk_effective_blocks(self, root: Tag) -> Iterator[Tag]:
        """Yield candidate block elements by traversing the DOM depth-first.

        The algorithm flattens nested wrappers and only yields tags whose names
        appear in ``BLOCK_TAGS``. Wrapper tags with no own text (see
        ``is_wrapper_tag``) are skipped so that their children can be emitted as
        independent blocks. This function replicates the original behaviour
        while extending support to additional wrapper tags.
        """
        stack: list[Tag] = [root]
        while stack:
            node = stack.pop()
            children = [c for c in node.find_all(recursive=False) if isinstance(c, Tag)]
            for c in reversed(children):
                stack.append(c)

            # Skip the artificial root wrapper
            if node is root:
                continue
            name = node.name.lower()
            if name not in BLOCK_TAGS:
                continue
            # Skip structural wrapper elements that do not represent actual content
            if name in WRAPPER_TAGS and (is_wrapper_tag(node) or has_descendant_block(node)):
                continue
            yield node