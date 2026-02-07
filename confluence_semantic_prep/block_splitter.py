from __future__ import annotations

"""Utility for splitting oversized block drafts into smaller pieces.

This module contains the ``BlockSplitter`` class which is responsible for
breaking down a ``BlockDraft`` when it exceeds the target token budget. It
preserves minimal HTML validity by wrapping sentence fragments back into
appropriate tags. Attributes on the original draft are propagated to
newly created drafts so that downstream components can reconstruct the
original structures faithfully.
"""

import html
import logging
from dataclasses import dataclass
from typing import Iterable, List, Optional

from bs4 import BeautifulSoup, Tag

from .html_parser import BlockDraft, markdownify_html, BLOCK_TAGS, _direct_table_rows, _row_has_th, _row_has_td
from .tokenizer import TokenCounter
from .utils_text import normalize_text, split_sentences_with_spans

logger = logging.getLogger(__name__)


@dataclass(slots=True, frozen=True)
class SplitParts:
    head: BlockDraft
    tail: BlockDraft


def _outer_tag_name(draft_html: str) -> Optional[str]:
    soup = BeautifulSoup(draft_html, "lxml")
    # soup.html/body may exist; pick the first tag that looks like a content root
    for t in soup.find_all(True, recursive=True):
        return t.name.lower()
    return None


def _count_table_columns(table_html: str) -> int:
    soup = BeautifulSoup(table_html, "lxml")
    table = soup.find("table")
    if not table:
        return 1
    # Prefer header count
    rows = _direct_table_rows(table)
    if rows:
        header = rows[0]
        ths = header.find_all("th", recursive=False)
        if ths:
            return max(1, len(ths))
    # Otherwise max td in any row
    max_td = 1
    for tr in rows:
        tds = tr.find_all("td", recursive=False)
        if tds:
            max_td = max(max_td, len(tds))
    return max_td or 1


def _extract_table_header_html(table_html: str) -> str:
    soup = BeautifulSoup(table_html, "lxml")
    table = soup.find("table")
    if not table:
        return ""
    for tr in _direct_table_rows(table):
        if _row_has_th(tr):
            return str(tr)
    return ""


class BlockSplitter:
    """Splits a draft into smaller drafts while preserving minimal valid HTML.

    Special cases:
    - list item drafts have root tag ul|ol and one <li> inside -> keep that wrapper
      and maintain the list/table grouping metadata (parent_uid, group_uid).
    - table row drafts have root tag <table> with header (optional) + one data <tr>
      -> keep header in each part and maintain grouping metadata.
    Attributes from the original draft are copied to produced drafts.
    """

    def __init__(self, token_counter: TokenCounter, ignore_tags: Iterable[str]) -> None:
        self.token_counter = token_counter
        self.ignore_tags = {t.strip().lower() for t in ignore_tags if t.strip()}

    def remove_ignored_tags(self, html_text: str) -> str:
        soup = BeautifulSoup(html_text, "lxml")
        for tname in self.ignore_tags:
            for bad in soup.find_all(tname):
                bad.decompose()
        # Return inner HTML of body if present
        body = soup.find("body")
        if body:
            return "".join(str(x) for x in body.contents)
        return str(soup)

    def has_nested_block_tags(self, draft: BlockDraft) -> bool:
        soup = BeautifulSoup(draft.html, "lxml")
        root = soup.find(True)
        if not root:
            return False
        # Any descendant block tag other than the root?
        for d in root.find_all(True):
            if d is root:
                continue
            if d.name.lower() in BLOCK_TAGS:
                return True
        return False

    def explode_by_child_blocks(self, draft: BlockDraft) -> Optional[List[BlockDraft]]:
        """If draft contains nested block tags, split it into child-block drafts.

        This is a best-effort implementation of 2.8.2.1 concept:
        we descend into direct child block tags and return them as separate drafts.
        The grouping metadata (parent_uid, group_uid) of the input draft is
        preserved on all produced drafts. Attributes from the original draft
        are also propagated.
        """
        soup = BeautifulSoup(draft.html, "lxml")
        root = soup.find(True)
        if not root:
            return None

        root_name = root.name.lower()

        # List item draft: <ul|ol><li>...</li></ul|ol>
        if root_name in {"ul", "ol"}:
            li = root.find("li")
            if not li:
                return None
            child_blocks = [c for c in li.find_all(True, recursive=False) if c.name.lower() in BLOCK_TAGS]
            if not child_blocks:
                return None
            out: List[BlockDraft] = []
            for cb in child_blocks:
                html_piece = f"<{root_name}><li>{str(cb)}</li></{root_name}>"
                txt = normalize_text(BeautifulSoup(html_piece, "lxml").get_text(" ", strip=True))
                if not txt:
                    continue
                out.append(
                    BlockDraft(
                        block_type=f"{root_name}_li",
                        html=html_piece,
                        markdown=markdownify_html(html_piece),
                        text=txt,
                        token_est=self.token_counter.count(txt),
                        heading_path_full=draft.heading_path_full,
                        heading_path_text=draft.heading_path_text,
                        nearest_heading_id=draft.nearest_heading_id,
                        parent_uid=draft.parent_uid,
                        group_uid=draft.group_uid,
                        attributes=draft.attributes,
                    )
                )
            return out or None

        # Generic wrapper: split by direct child block tags
        child_blocks = [c for c in root.find_all(True, recursive=False) if c.name.lower() in BLOCK_TAGS]
        if not child_blocks:
            return None

        out: List[BlockDraft] = []
        for cb in child_blocks:
            name = cb.name.lower()
            if name in {"ul", "ol"}:
                # split into items, reusing same logic as parser
                for li in cb.find_all("li", recursive=False):
                    inner = "".join(str(x) for x in li.contents).strip()
                    html_piece = f"<{name}><li>{inner}</li></{name}>"
                    txt = normalize_text(BeautifulSoup(html_piece, "lxml").get_text(" ", strip=True))
                    if not txt:
                        continue
                    out.append(
                        BlockDraft(
                            block_type=f"{name}_li",
                            html=html_piece,
                            markdown=markdownify_html(html_piece),
                            text=txt,
                            token_est=self.token_counter.count(txt),
                            heading_path_full=draft.heading_path_full,
                            heading_path_text=draft.heading_path_text,
                            nearest_heading_id=draft.nearest_heading_id,
                            parent_uid=draft.parent_uid,
                            group_uid=draft.group_uid,
                            attributes=draft.attributes,
                        )
                    )
            elif name == "table":
                # split table into data rows
                header = None
                for tr in _direct_table_rows(cb):
                    if _row_has_th(tr):
                        header = tr
                        break
                rows = [tr for tr in _direct_table_rows(cb) if _row_has_td(tr)]
                for tr in rows:
                    html_piece = "<table>" + (str(header) if header else "") + str(tr) + "</table>"
                    txt = normalize_text(BeautifulSoup(html_piece, "lxml").get_text(" ", strip=True))
                    if not txt:
                        continue
                    out.append(
                        BlockDraft(
                            block_type="table_row",
                            html=html_piece,
                            markdown=markdownify_html(html_piece),
                            text=txt,
                            token_est=self.token_counter.count(txt),
                            heading_path_full=draft.heading_path_full,
                            heading_path_text=draft.heading_path_text,
                            nearest_heading_id=draft.nearest_heading_id,
                            parent_uid=draft.parent_uid,
                            group_uid=draft.group_uid,
                            attributes=draft.attributes,
                        )
                    )
            else:
                html_piece = str(cb)
                txt = normalize_text(BeautifulSoup(html_piece, "lxml").get_text(" ", strip=True))
                if not txt:
                    continue
                out.append(
                    BlockDraft(
                        block_type=name,
                        html=html_piece,
                        markdown=markdownify_html(html_piece),
                        text=txt,
                        token_est=self.token_counter.count(txt),
                        heading_path_full=draft.heading_path_full,
                        heading_path_text=draft.heading_path_text,
                        nearest_heading_id=draft.nearest_heading_id,
                        parent_uid=draft.parent_uid,
                        group_uid=draft.group_uid,
                        attributes=draft.attributes,
                    )
                )

        return out or None

    def split_by_sentences_to_fit(self, draft: BlockDraft, token_budget: int) -> Optional[SplitParts]:
        """Split draft.text into two parts by sentence spans so that the first fits into token_budget.

        HTML wrappers:
        - ul_li/ol_li: wrap parts into <ul>/<ol><li>...</li></ul>
        - table_row: wrap into <table>{header}<tr><td colspan=N>...</td></tr></table>
        - generic tags: wrap into same outer tag
        Attributes from the original draft are propagated to both parts.
        """
        if token_budget < 30:
            return None

        spans = split_sentences_with_spans(draft.text)
        if len(spans) <= 1:
            return None

        acc = []
        used = 0
        last_span_end = None
        for sp in spans:
            t = self.token_counter.count(sp.text)
            if acc and used + t > token_budget:
                break
            acc.append(sp)
            used += t
            last_span_end = sp.end

        if not acc or len(acc) == len(spans) or last_span_end is None:
            return None

        part1_text = normalize_text(draft.text[:last_span_end])
        part2_text = normalize_text(draft.text[last_span_end:])

        if not part1_text or not part2_text:
            return None

        head = self._rebuild_draft_with_text(draft, part1_text)
        tail = self._rebuild_draft_with_text(draft, part2_text)
        return SplitParts(head=head, tail=tail)

    def _rebuild_draft_with_text(self, draft: BlockDraft, new_text: str) -> BlockDraft:
        bt = draft.block_type.lower()

        if bt in {"ul_li", "ol_li"}:
            list_tag = bt.split("_", 1)[0]
            safe = html.escape(new_text)
            html_block = f"<{list_tag}><li>{safe}</li></{list_tag}>"
        elif bt == "table_row":
            header = _extract_table_header_html(draft.html)
            cols = _count_table_columns(draft.html)
            safe = html.escape(new_text)
            html_block = "<table>" + (header or "") + f"<tr><td colspan=\"{cols}\">{safe}</td></tr></table>"
        elif bt in {"p", "div", "blockquote", "pre", "h1", "h2", "h3", "h4", "h5", "h6"}:
            safe = html.escape(new_text)
            html_block = f"<{bt}>{safe}</{bt}>"
        else:
            # generic fallback
            safe = html.escape(new_text)
            html_block = f"<p>{safe}</p>"
            bt = "p"

        txt = normalize_text(BeautifulSoup(html_block, "lxml").get_text(" ", strip=True))
        return BlockDraft(
            block_type=bt,
            html=html_block,
            markdown=markdownify_html(html_block),
            text=txt,
            token_est=self.token_counter.count(txt),
            heading_path_full=draft.heading_path_full,
            heading_path_text=draft.heading_path_text,
            nearest_heading_id=draft.nearest_heading_id,
            parent_uid=draft.parent_uid,
            group_uid=draft.group_uid,
            attributes=draft.attributes,
        )