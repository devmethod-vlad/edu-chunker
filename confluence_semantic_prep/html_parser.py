from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable, Iterator

from bs4 import BeautifulSoup, Tag
from markdownify import markdownify as md

from .models import Heading
from .tokenizer import TokenCounter
from .utils_text import normalize_text

logger = logging.getLogger(__name__)

BLOCK_TAGS = {
    "p", "div", "blockquote", "pre", "ul", "ol", "table",
    "h1", "h2", "h3", "h4", "h5", "h6",
}


@dataclass(slots=True, frozen=True)
class BlockDraft:
    block_type: str
    html: str
    markdown: str
    text: str
    token_est: int
    heading_path_full: list[Heading]
    heading_path_text: list[Heading]
    nearest_heading_id: str | None


def markdownify_html(html: str) -> str:
    return md(html, heading_style="ATX").strip()


def heading_level(tag_name: str) -> int | None:
    if len(tag_name) == 2 and tag_name[0] == "h" and tag_name[1].isdigit():
        lvl = int(tag_name[1])
        return lvl if 1 <= lvl <= 6 else None
    return None


def heading_id(tag: Tag) -> str | None:
    hid = tag.get("id")
    return hid.strip() if isinstance(hid, str) and hid.strip() else None


def condense_heading_path_for_text(full_path: list[Heading], limit: int) -> list[Heading]:
    """Keep at most `limit` headings starting from the closest (least important) and jumping to more important."""
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


def is_wrapper_div(tag: Tag) -> bool:
    if tag.name != "div":
        return False
    for child in tag.find_all(recursive=False):
        if isinstance(child, Tag) and child.name in BLOCK_TAGS:
            return True
    return False


def has_descendant_block(tag: Tag) -> bool:
    for d in tag.descendants:
        if isinstance(d, Tag) and d.name in BLOCK_TAGS:
            return True
    return False


class HTMLToBlockDraftsParser:
    def __init__(self, token_counter: TokenCounter, ignore_tags: Iterable[str], heading_levels_for_text: int) -> None:
        self.token_counter = token_counter
        self.ignore_tags = {t.strip().lower() for t in ignore_tags if t.strip()}
        self.heading_levels_for_text = heading_levels_for_text

    def parse(self, html: str) -> list[BlockDraft]:
        soup = BeautifulSoup(f"<div id='__root__'>{html}</div>", "lxml")
        root = soup.find("div", {"id": "__root__"})
        if root is None:
            return []

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

            if name in {"ul", "ol"}:
                # Split list into items (each item becomes a block-draft with root tag ul/ol)
                for li in node.find_all("li", recursive=False):
                    html_item = self.wrap_list_item_html(name, li)
                    txt = normalize_text(BeautifulSoup(html_item, "lxml").get_text(" ", strip=True))
                    if not txt:
                        continue
                    drafts.append(
                        BlockDraft(
                            block_type=name,  # parent tag of the block HTML
                            html=html_item,
                            markdown=markdownify_html(html_item),
                            text=txt,
                            token_est=self.token_counter.count(txt),
                            heading_path_full=heading_full,
                            heading_path_text=heading_text,
                            nearest_heading_id=last_heading_id,
                        )
                    )
                continue

            if name == "table":
                drafts.extend(self.table_to_row_drafts(node, heading_full, heading_text, last_heading_id))
                continue

            if name == "div" and (is_wrapper_div(node) or has_descendant_block(node)):
                continue

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

            if lvl is not None:
                heading_stack = [h for h in heading_stack if h.level < lvl]
                new_h = Heading(level=lvl, text=txt, html_id=heading_id(node))
                heading_stack.append(new_h)
                last_heading_id = new_h.html_id or last_heading_id

        return drafts

    def wrap_list_item_html(self, list_tag: str, li: Tag) -> str:
        inner = "".join(str(x) for x in li.contents).strip()
        return f"<{list_tag}><li>{inner}</li></{list_tag}>"

    def table_to_row_drafts(
        self,
        table: Tag,
        heading_full: list[Heading],
        heading_text: list[Heading],
        nearest_heading_id: str | None,
    ) -> list[BlockDraft]:
        header_tr = None
        for tr in table.find_all("tr"):
            if tr.find_all("th"):
                header_tr = tr
                break

        rows = [tr for tr in table.find_all("tr") if tr.find_all("td")]
        out: list[BlockDraft] = []
        for tr in rows:
            h_html = str(header_tr) if header_tr is not None else ""
            row_html = str(tr)
            html_block = "<table>" + (h_html or "") + row_html + "</table>"
            txt = normalize_text(BeautifulSoup(html_block, "lxml").get_text(" ", strip=True))
            if not txt:
                continue
            out.append(
                BlockDraft(
                    block_type="table",
                    html=html_block,
                    markdown=markdownify_html(html_block),
                    text=txt,
                    token_est=self.token_counter.count(txt),
                    heading_path_full=heading_full,
                    heading_path_text=heading_text,
                    nearest_heading_id=nearest_heading_id,
                )
            )
        return out

    def walk_effective_blocks(self, root: Tag) -> Iterator[Tag]:
        stack: list[Tag] = [root]
        while stack:
            node = stack.pop()
            children = [c for c in node.find_all(recursive=False) if isinstance(c, Tag)]
            for c in reversed(children):
                stack.append(c)

            if node is root:
                continue
            name = node.name.lower()
            if name not in BLOCK_TAGS:
                continue
            if name == "div" and (is_wrapper_div(node) or has_descendant_block(node)):
                continue
            yield node
