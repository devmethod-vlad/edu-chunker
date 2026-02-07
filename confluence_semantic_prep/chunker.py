from __future__ import annotations

"""Chunker for converting BlockDrafts into Blocks and grouping them into chunks.

The chunker takes a list of ``BlockDraft`` objects produced by the
``HTMLToBlockDraftsParser`` and converts them into fully fledged ``Block``
instances. It then aggregates these blocks into chunks according to
token budgets. During chunk aggregation, it attempts to reconstruct
valid HTML structures for lists and tables by grouping consecutive
elements that share the same parent. This ensures that even partial
lists or tables are rendered correctly when only a subset of their
elements appears in a chunk.
"""

import logging
import html
from dataclasses import dataclass
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup

from .block_splitter import BlockSplitter
from .html_parser import BlockDraft, condense_heading_path_for_text, markdownify_html, _direct_table_rows, _row_has_th, _row_has_td
from .models import Block, Chunk, Heading
from .tokenizer import TokenCounter
from .utils_text import normalize_text

_HEADING_BLOCK_TYPES: frozenset[str] = frozenset({"h1", "h2", "h3", "h4", "h5", "h6"})

logger = logging.getLogger(__name__)


def compose_text_for_embedding(
    page_title: str,
    heading_path_for_text: List[Heading],
    chunk_text: str,
    add_page: bool,
    add_section: bool,
) -> str:
    parts: List[str] = []
    if add_page:
        parts.append(f"[PAGE] {page_title}")
    if add_section and heading_path_for_text:
        sec = " > ".join(h.text for h in heading_path_for_text)
        parts.append(f"[SECTION] {sec}")
    if parts:
        parts.append(f"[TEXT] {chunk_text}")
        return "\n".join(parts)
    return chunk_text


@dataclass(slots=True)
class ChunkingResult:
    blocks: List[Block]
    chunks: List[Chunk]
    page_text_normalized: str


class Chunker:
    def __init__(
        self,
        token_counter: TokenCounter,
        splitter: BlockSplitter,
        chunk_size_tokens: int,
        chunk_min_tokens: int,
        add_page_prefix: bool,
        add_section_prefix: bool,
        heading_levels_for_text: int = 2,
        chunk_overlap_tokens: int = 0,
    ) -> None:
        self.token_counter = token_counter
        self.splitter = splitter
        self.chunk_size_tokens = chunk_size_tokens
        self.chunk_min_tokens = chunk_min_tokens
        self.add_page_prefix = add_page_prefix
        self.add_section_prefix = add_section_prefix
        self.heading_levels_for_text = heading_levels_for_text
        self.chunk_overlap_tokens = max(0, chunk_overlap_tokens)

        if self.chunk_overlap_tokens >= self.chunk_size_tokens:
            raise ValueError(
                f"chunk_overlap_tokens ({self.chunk_overlap_tokens}) must be "
                f"strictly less than chunk_size_tokens ({self.chunk_size_tokens})"
            )

    def chunk_page(
        self,
        page_id: str,
        space_key: Optional[str],
        page_title: str,
        drafts: List[BlockDraft],
        page_last_modified: Optional[str],
        page_version: Optional[int],
    ) -> ChunkingResult:
        """Create blocks and chunks from a list of BlockDrafts.

        Additional metadata about the page (last modified timestamp and version)
        is passed in to attach to each block and chunk.
        """
        final_blocks: List[Block] = []
        chunks: List[Chunk] = []

        # Current chunk aggregation (over Block objects already created)
        cur_blocks: List[Block] = []
        cur_text_parts: List[str] = []
        # Running token count for the current prefix (page title + section).
        # Computed once when the first block enters the chunk and reused
        # to avoid re-tokenising the prefix on every call.
        cur_prefix_tokens: int = 0

        # Mapping from group_uid (assigned in BlockDraft.group_uid) to the block_id
        # of the first element in that group. Used to resolve parent_block_id on
        # subsequent items.
        uid_to_block_id: dict[str, str] = {}

        def cur_chunk_text() -> str:
            return normalize_text("\n".join(cur_text_parts))

        def _prefix_tokens(heading_text: List[Heading]) -> int:
            """Count tokens in the [PAGE]/[SECTION] prefix without the body."""
            prefix_parts: List[str] = []
            if self.add_page_prefix:
                prefix_parts.append(f"[PAGE] {page_title}")
            if self.add_section_prefix and heading_text:
                sec = " > ".join(h.text for h in heading_text)
                prefix_parts.append(f"[SECTION] {sec}")
            if not prefix_parts:
                return 0
            return self.token_counter.count("\n".join(prefix_parts) + "\n[TEXT] ")

        def cur_chunk_tokens() -> int:
            body = cur_chunk_text()
            if not cur_blocks:
                return 0
            return cur_prefix_tokens + self.token_counter.count(body)

        def tokens_if_add_text(extra: str) -> int:
            tmp = normalize_text("\n".join(cur_text_parts + [extra]))
            prefix = cur_prefix_tokens
            if not cur_blocks:
                # First block – compute prefix from the block's heading
                prefix = _prefix_tokens([])  # will be set when block is actually added
                # For estimation just use a cheap prefix count
                heading_text_est: List[Heading] = []
                prefix = _prefix_tokens(heading_text_est)
                # Actually, compute full to be accurate
                return self.token_counter.count(
                    compose_text_for_embedding(page_title, heading_text_est, tmp, self.add_page_prefix, self.add_section_prefix)
                )
            return prefix + self.token_counter.count(tmp)

        def flush_chunk() -> None:
            nonlocal cur_blocks, cur_text_parts, cur_prefix_tokens
            if not cur_blocks:
                return

            chunk_text = cur_chunk_text()
            # Build aggregated HTML and markdown for this chunk
            html_body, md_body = self._build_chunk_html_and_md(cur_blocks)
            html_out = f"<div>{html_body}</div>"
            markdown_out = md_body.strip()

            first_block = cur_blocks[0]
            heading_full = list(first_block.heading_path_full)
            heading_text = list(first_block.heading_path_text)
            nearest_heading_id = first_block.nearest_heading_id

            # ---- FIX: when the first block in a chunk IS a heading, the
            # chunk's context must reflect that heading, not its predecessor.
            # The parser emits heading drafts with the PREVIOUS heading
            # context (because the stack is updated after the emit). Here we
            # correct it at chunk level: rebuild heading_path so it includes
            # the heading itself, and set nearest_heading_id to this heading's
            # own id.
            first_bt = first_block.block_type.lower()
            if first_bt in _HEADING_BLOCK_TYPES:
                lvl = int(first_bt[1])
                # Keep only ancestor headings with a strictly smaller (more
                # important) level than this heading.
                heading_full = [h for h in heading_full if h.level < lvl]
                own_html_id = first_block.attributes.get("id") or None
                own_heading = Heading(
                    level=lvl,
                    text=first_block.text,
                    html_id=own_html_id,
                )
                heading_full.append(own_heading)
                # Recompute the condensed path used for embedding text.
                heading_text = condense_heading_path_for_text(
                    heading_full, self.heading_levels_for_text,
                )
                if own_html_id:
                    nearest_heading_id = own_html_id

            text_for_embedding = compose_text_for_embedding(
                page_title,
                heading_text,
                chunk_text,
                self.add_page_prefix,
                self.add_section_prefix,
            )
            tok = self.token_counter.count(text_for_embedding)

            first = cur_blocks[0].block_index
            last = cur_blocks[-1].block_index
            chunk_id = f"EDU:{page_id}:{first}-{last}"

            # char offsets will be filled after we compute page_text_normalized
            chunks.append(
                Chunk(
                    page_id=page_id,
                    space_key=space_key,
                    chunk_id=chunk_id,
                    block_indices=[b.block_index for b in cur_blocks],
                    heading_path_full=heading_full,
                    nearest_heading_id=nearest_heading_id,
                    char_start=0,
                    char_end=0,
                    token_est=tok,
                    html=html_out,
                    markdown=markdown_out,
                    chunk_text=chunk_text,
                    text_for_embedding=text_for_embedding,
                    page_last_modified=page_last_modified,
                    page_version=page_version,
                )
            )

            cur_blocks = []
            cur_text_parts = []
            cur_prefix_tokens = 0

        def _append_block(b: Block) -> None:
            nonlocal cur_prefix_tokens
            if not cur_blocks:
                # First block in chunk – compute prefix tokens once
                cur_prefix_tokens = _prefix_tokens(b.heading_path_text)
            cur_blocks.append(b)
            cur_text_parts.append(b.text)

        # -------------------------------------------------------------------
        # Use a deque-like approach with an index pointer. To avoid the
        # fragile splice-into-list pattern that caused duplicates and
        # potential infinite loops, we use a secondary "pending" list.
        # -------------------------------------------------------------------
        queue: List[BlockDraft] = list(drafts)
        i = 0

        # Safety counter to prevent truly infinite loops in pathological input
        max_iterations = len(queue) * 10 + 1000

        while i < len(queue):
            max_iterations -= 1
            if max_iterations <= 0:
                logger.warning("Chunker iteration limit reached for page %s, flushing remaining blocks.", page_id)
                flush_chunk()
                break

            d = queue[i]
            i += 1

            # Determine the parent block id if this draft belongs to a group
            parent_block_id: Optional[str] = None
            if d.group_uid:
                if d.parent_uid is None:
                    # This is the first element in its group. Its block_id will be
                    # recorded after creation so children can reference it.
                    pass
                else:
                    parent_block_id = uid_to_block_id.get(d.parent_uid)

            # Create a new Block object (index assigned here)
            b = Block(
                block_index=len(final_blocks),
                block_id=f"EDU:{page_id}-{len(final_blocks)}",
                block_type=d.block_type,
                heading_path_full=list(d.heading_path_full),
                heading_path_text=list(d.heading_path_text),
                nearest_heading_id=d.nearest_heading_id,
                char_start=0,
                char_end=0,
                token_est=d.token_est,
                html=d.html,
                markdown=d.markdown,
                text=d.text,
                parent_block_id=parent_block_id,
                attributes=d.attributes,
                page_last_modified=page_last_modified,
                page_version=page_version,
            )

            # If this is the first element of a group, register its block id
            if d.group_uid and d.parent_uid is None:
                uid_to_block_id[d.group_uid] = b.block_id

            # If empty -> skip
            if not b.text:
                continue

            # Check fit to current chunk
            if not cur_blocks:
                # start new chunk
                if tokens_if_add_text(b.text) <= self.chunk_size_tokens:
                    _append_block(b)
                    final_blocks.append(b)
                    continue

                # Block alone doesn't fit -> must split
                split_done = self._split_oversize_block_into_queue(b, d, queue, i)
                if split_done:
                    # New drafts were inserted at queue[i:]; do not append b
                    continue
                else:
                    # Fallback: force-add anyway (rare; keeps progress)
                    _append_block(b)
                    final_blocks.append(b)
                    flush_chunk()
                    continue

            # There is an active chunk
            cand = tokens_if_add_text(b.text)
            if cand <= self.chunk_size_tokens:
                _append_block(b)
                final_blocks.append(b)
                continue

            # Doesn't fit in remaining space.
            remaining = self.chunk_size_tokens - cur_chunk_tokens()

            # If current chunk is too small, try to split to fill it.
            if cur_chunk_tokens() < self.chunk_min_tokens and remaining > 30:
                # Try explode if it has nested block tags
                if self.splitter.has_nested_block_tags(d):
                    exploded = self.splitter.explode_by_child_blocks(d)
                    if exploded:
                        # Replace current position with exploded drafts
                        queue[i:i] = exploded
                        # Don't increment i; next iteration picks up first exploded
                        continue

                parts = self.splitter.split_by_sentences_to_fit(d, remaining)
                if parts:
                    # Head fits into remaining space, tail goes back to queue.
                    # Rebuild block from head draft.
                    head_d = parts.head
                    head_b = Block(
                        block_index=len(final_blocks),
                        block_id=f"EDU:{page_id}-{len(final_blocks)}",
                        block_type=head_d.block_type,
                        heading_path_full=list(head_d.heading_path_full),
                        heading_path_text=list(head_d.heading_path_text),
                        nearest_heading_id=head_d.nearest_heading_id,
                        char_start=0,
                        char_end=0,
                        token_est=head_d.token_est,
                        html=head_d.html,
                        markdown=head_d.markdown,
                        text=head_d.text,
                        parent_block_id=parent_block_id,
                        attributes=head_d.attributes,
                        page_last_modified=page_last_modified,
                        page_version=page_version,
                    )
                    _append_block(head_b)
                    final_blocks.append(head_b)
                    # Insert tail back into the queue to be processed next
                    queue[i:i] = [parts.tail]
                    # Flush current chunk (it now includes head)
                    flush_chunk()
                    continue

            # Otherwise flush and retry block in a new chunk
            flush_chunk()
            # Re-insert the draft into the queue so it's processed as the
            # first block of the new chunk.
            queue[i:i] = [d]

        flush_chunk()

        # Compute page_text_normalized + char offsets for blocks, then patch chunk offsets
        page_text_parts: List[str] = []
        cursor = 0
        for b in final_blocks:
            t = normalize_text(b.text)
            start = cursor
            end = start + len(t)
            b.char_start = start
            b.char_end = end
            cursor = end + 1
            page_text_parts.append(t)

        page_text_normalized = "\n".join(page_text_parts)

        for c in chunks:
            first = c.block_indices[0]
            last = c.block_indices[-1]
            c.char_start = final_blocks[first].char_start
            c.char_end = final_blocks[last].char_end

        # Post-processing: enrich chunks with overlap from neighbours
        if self.chunk_overlap_tokens > 0:
            self._apply_overlap(chunks, final_blocks, page_title)

        return ChunkingResult(blocks=final_blocks, chunks=chunks, page_text_normalized=page_text_normalized)

    def _split_oversize_block_into_queue(
        self, block: Block, draft: BlockDraft, queue: List[BlockDraft], insert_pos: int
    ) -> bool:
        """Try to split an oversized draft and insert parts into the queue.

        Returns True if the split was successful and new drafts were inserted
        at ``queue[insert_pos:]``.
        """
        # If nested block tags -> explode by child blocks
        if self.splitter.has_nested_block_tags(draft):
            exploded = self.splitter.explode_by_child_blocks(draft)
            if exploded:
                queue[insert_pos:insert_pos] = exploded
                return True

        # Otherwise split by sentences using full budget (chunk size)
        parts = self.splitter.split_by_sentences_to_fit(draft, self.chunk_size_tokens)
        if parts:
            queue[insert_pos:insert_pos] = [parts.head, parts.tail]
            return True

        return False

    # ------------------------------------------------------------------
    # Overlap post-processing
    # ------------------------------------------------------------------

    def _apply_overlap(
        self, chunks: List[Chunk], blocks: List[Block], page_title: str,
    ) -> None:
        """Enrich each chunk with content from its neighbours.

        For every chunk (except the first) the tail of the **previous** chunk
        is prepended, and for every chunk (except the last) the head of the
        **next** chunk is appended.  The amount of borrowed content is
        controlled by ``self.chunk_overlap_tokens``.

        Only ``html``, ``markdown``, ``chunk_text``, ``text_for_embedding``,
        ``token_est``, ``block_indices``, ``char_start`` and ``char_end`` are
        updated.  Heading context (``heading_path_full``,
        ``nearest_heading_id``) is intentionally left unchanged so that it
        continues to reflect the primary content of the chunk.
        """
        if len(chunks) <= 1:
            return

        budget = self.chunk_overlap_tokens

        # Snapshot original block indices BEFORE any modifications so that
        # each chunk's overlap is computed from its neighbour's primary
        # content, not from already-extended indices.
        orig_indices: List[List[int]] = [list(c.block_indices) for c in chunks]

        for idx, chunk in enumerate(chunks):
            current_indices_set = set(orig_indices[idx])

            # --- previous-chunk tail (prepend) ---
            prev_blocks: List[Block] = []
            if idx > 0:
                tok_acc = 0
                for bi in reversed(orig_indices[idx - 1]):
                    if bi in current_indices_set:
                        continue
                    b = blocks[bi]
                    t = self.token_counter.count(b.text)
                    if prev_blocks and tok_acc + t > budget:
                        break
                    prev_blocks.insert(0, b)
                    tok_acc += t
                    if tok_acc >= budget:
                        break

            # --- next-chunk head (append) ---
            next_blocks: List[Block] = []
            if idx < len(chunks) - 1:
                tok_acc = 0
                for bi in orig_indices[idx + 1]:
                    if bi in current_indices_set:
                        continue
                    b = blocks[bi]
                    t = self.token_counter.count(b.text)
                    if next_blocks and tok_acc + t > budget:
                        break
                    next_blocks.append(b)
                    tok_acc += t
                    if tok_acc >= budget:
                        break

            if not prev_blocks and not next_blocks:
                continue

            # Assemble the full block sequence: prev-overlap + own + next-overlap
            own_blocks = [blocks[bi] for bi in orig_indices[idx]]
            all_blocks = prev_blocks + own_blocks + next_blocks

            # Rebuild chunk content
            html_body, md_body = self._build_chunk_html_and_md(all_blocks)
            chunk.html = f"<div>{html_body}</div>"
            chunk.markdown = md_body.strip()

            chunk_text = normalize_text("\n".join(b.text for b in all_blocks))
            chunk.chunk_text = chunk_text

            heading_text = condense_heading_path_for_text(
                chunk.heading_path_full, self.heading_levels_for_text,
            )
            chunk.text_for_embedding = compose_text_for_embedding(
                page_title,
                heading_text,
                chunk_text,
                self.add_page_prefix,
                self.add_section_prefix,
            )
            chunk.token_est = self.token_counter.count(chunk.text_for_embedding)

            chunk.block_indices = [b.block_index for b in all_blocks]
            chunk.char_start = all_blocks[0].char_start
            chunk.char_end = all_blocks[-1].char_end

    def _build_chunk_html_and_md(self, blocks: List[Block]) -> Tuple[str, str]:
        """Aggregate HTML and Markdown for a list of blocks.

        This method groups consecutive list items and table rows that share
        the same parent into a single list or table respectively. For all
        other block types the original HTML is used directly. Markdown is
        generated from the aggregated HTML.
        """
        html_parts: List[str] = []
        md_parts: List[str] = []
        i = 0
        n = len(blocks)
        while i < n:
            b = blocks[i]
            bt = b.block_type
            # Group list items
            if bt in {"ul_li", "ol_li"}:
                # Determine the root id for grouping: the first item (parent_block_id is None) defines the root
                root_id = b.block_id if b.parent_block_id is None else b.parent_block_id
                list_tag = bt.split("_", 1)[0]
                group_blocks: List[Block] = []
                j = i
                while j < n and blocks[j].block_type == bt:
                    curr = blocks[j]
                    curr_root = curr.block_id if curr.parent_block_id is None else curr.parent_block_id
                    if curr_root != root_id:
                        break
                    group_blocks.append(curr)
                    j += 1
                # Build list HTML with aggregated items
                # Apply attributes from the root list (only first block has parent_block_id None)
                attrs: Optional[dict] = None
                for gb in group_blocks:
                    if gb.parent_block_id is None:
                        attrs = gb.attributes or None
                        break
                attr_str = ""
                if attrs:
                    pieces = []
                    for k, v in attrs.items():
                        # escape attribute values
                        pieces.append(f'{k}="{html.escape(v, quote=True)}"')
                    attr_str = " " + " ".join(pieces) if pieces else ""
                list_html = f"<{list_tag}{attr_str}>"
                # Extract li content from each block's html
                for gb in group_blocks:
                    soup = BeautifulSoup(gb.html, "lxml")
                    li = soup.find("li")
                    if li:
                        # Preserve attributes on li if present
                        li_attrs = {}
                        for k, v in li.attrs.items():
                            if isinstance(v, list):
                                li_attrs[k.lower()] = " ".join(str(x) for x in v)
                            else:
                                li_attrs[k.lower()] = str(v)
                        li_attr_str = ""
                        if li_attrs:
                            parts = []
                            for k, v in li_attrs.items():
                                parts.append(f'{k}="{html.escape(v, quote=True)}"')
                            li_attr_str = " " + " ".join(parts)
                        inner = "".join(str(x) for x in li.contents)
                        list_html += f"<li{li_attr_str}>{inner}</li>"
                list_html += f"</{list_tag}>"
                html_parts.append(list_html)
                md_parts.append(markdownify_html(list_html))
                i = j
                continue
            # Group table rows
            if bt == "table_row":
                root_id = b.block_id if b.parent_block_id is None else b.parent_block_id
                group: List[Block] = []
                j = i
                while j < n and blocks[j].block_type == "table_row":
                    curr = blocks[j]
                    curr_root = curr.block_id if curr.parent_block_id is None else curr.parent_block_id
                    if curr_root != root_id:
                        break
                    group.append(curr)
                    j += 1
                table_html = self._build_table_html(group)
                html_parts.append(table_html)
                md_parts.append(markdownify_html(table_html))
                i = j
                continue
            # Otherwise append HTML directly
            html_parts.append(b.html)
            md_parts.append(b.markdown)
            i += 1
        full_html = "".join(html_parts)
        md_text = "\n\n".join([m for m in md_parts if m.strip()])
        return full_html, md_text

    def _build_table_html(self, rows: List[Block]) -> str:
        """Reconstruct a table from a sequence of table_row blocks.

        The method combines the header (if present) and all data rows into
        a single table. It also attempts to normalise ``rowspan`` and
        ``colspan`` attributes by repeating cell contents horizontally and
        vertically. Table-level attributes from the first block are applied
        to the resulting table tag.
        """
        if not rows:
            return ""
        # Use attributes from the root table of the first block if present
        attrs = rows[0].attributes or {}
        attr_str = ""
        if attrs:
            parts = []
            for k, v in attrs.items():
                parts.append(f'{k}="{html.escape(v, quote=True)}"')
            attr_str = " " + " ".join(parts)
        # Determine header row by inspecting the first block's HTML
        header_html = ""
        for blk in rows:
            soup = BeautifulSoup(blk.html, "lxml")
            table = soup.find("table")
            if not table:
                continue
            for tr in _direct_table_rows(table):
                if _row_has_th(tr):
                    header_html = str(tr)
                    break
            if header_html:
                break
        # Parse data rows and handle rowspans/colspans
        # We'll build a matrix of cell HTML strings
        row_data: List[List[str]] = []
        rowspan_map: dict[int, dict[str, int]] = {}
        # rowspan_map maps column index -> {'html': cell_html, 'remaining': remaining_rows}
        max_cols = 0
        for blk in rows:
            soup = BeautifulSoup(blk.html, "lxml")
            table = soup.find("table")
            if not table:
                continue
            data_tr: Optional[Tag] = None
            for tr in _direct_table_rows(table):
                if _row_has_td(tr):
                    data_tr = tr
                    break
            if data_tr is None:
                continue
            local_cells: List[str] = []
            col_index = 0
            # Fill in pending rowspans before processing this row
            while True:
                found = False
                for idx in sorted(rowspan_map.keys()):
                    if idx == col_index:
                        info = rowspan_map[idx]
                        local_cells.append(info['html'])
                        info['remaining'] -= 1
                        if info['remaining'] == 0:
                            del rowspan_map[idx]
                        col_index += 1
                        found = True
                        break
                if not found:
                    break
            # Process each cell in the current row
            for cell in data_tr.find_all(['td', 'th'], recursive=False):
                # Determine colspan and rowspan
                cs = 1
                rs = 1
                if cell.has_attr('colspan'):
                    try:
                        cs = int(cell['colspan'])
                    except Exception:
                        pass
                if cell.has_attr('rowspan'):
                    try:
                        rs = int(cell['rowspan'])
                    except Exception:
                        pass
                inner_html = "".join(str(x) for x in cell.contents)
                for k in range(cs):
                    local_cells.append(f"<td>{inner_html}</td>")
                if rs > 1:
                    for k in range(cs):
                        idx = col_index + k
                        rowspan_map[idx] = {'html': f"<td>{inner_html}</td>", 'remaining': rs - 1}
                col_index += cs
            # Fill remaining rowspans after explicit cells
            added = True
            while added:
                added = False
                for idx in sorted(rowspan_map.keys()):
                    if idx == col_index:
                        info = rowspan_map[idx]
                        local_cells.append(info['html'])
                        info['remaining'] -= 1
                        if info['remaining'] == 0:
                            del rowspan_map[idx]
                        col_index += 1
                        added = True
                        break
            row_data.append(local_cells)
            max_cols = max(max_cols, len(local_cells))
        # Normalise rows to have equal number of columns
        for row in row_data:
            if len(row) < max_cols:
                row.extend(["<td></td>"] * (max_cols - len(row)))
        rows_html = "".join([f"<tr>{''.join(cells)}</tr>" for cells in row_data])
        table_html = f"<table{attr_str}>{header_html}{rows_html}</table>"
        return table_html