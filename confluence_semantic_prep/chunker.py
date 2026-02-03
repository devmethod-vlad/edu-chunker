from __future__ import annotations

import logging
from dataclasses import dataclass

from .block_splitter import BlockSplitter
from .html_parser import BlockDraft
from .models import Block, Chunk, Heading
from .tokenizer import TokenCounter
from .utils_text import normalize_text

logger = logging.getLogger(__name__)


def compose_text_for_embedding(
    page_title: str,
    heading_path_for_text: list[Heading],
    chunk_text: str,
    add_page: bool,
    add_section: bool,
) -> str:
    parts: list[str] = []
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
    blocks: list[Block]
    chunks: list[Chunk]
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
    ) -> None:
        self.token_counter = token_counter
        self.splitter = splitter
        self.chunk_size_tokens = chunk_size_tokens
        self.chunk_min_tokens = chunk_min_tokens
        self.add_page_prefix = add_page_prefix
        self.add_section_prefix = add_section_prefix

    def chunk_page(
        self,
        page_id: str,
        space_key: str | None,
        page_title: str,
        drafts: list[BlockDraft],
    ) -> ChunkingResult:
        final_blocks: list[Block] = []
        chunks: list[Chunk] = []

        # Current chunk aggregation (over Block objects already created)
        cur_blocks: list[Block] = []
        cur_text_parts: list[str] = []
        cur_html_parts: list[str] = []
        cur_md_parts: list[str] = []

        def cur_chunk_text() -> str:
            return normalize_text("\n".join(cur_text_parts))

        def cur_chunk_tokens() -> int:
            return self.token_counter.count(
                compose_text_for_embedding(
                    page_title,
                    cur_blocks[0].heading_path_text if cur_blocks else [],
                    cur_chunk_text(),
                    self.add_page_prefix,
                    self.add_section_prefix,
                )
            )

        def tokens_if_add_text(extra: str) -> int:
            heading_text = cur_blocks[0].heading_path_text if cur_blocks else []
            tmp = normalize_text("\n".join(cur_text_parts + [extra]))
            return self.token_counter.count(
                compose_text_for_embedding(page_title, heading_text, tmp, self.add_page_prefix, self.add_section_prefix)
            )

        def flush_chunk() -> None:
            nonlocal cur_blocks, cur_text_parts, cur_html_parts, cur_md_parts
            if not cur_blocks:
                return

            chunk_text = cur_chunk_text()
            html = "<div>" + "".join(cur_html_parts) + "</div>"
            markdown = "\n\n".join([m for m in cur_md_parts if m.strip()]).strip()
            heading_full = list(cur_blocks[0].heading_path_full)
            nearest_heading_id = cur_blocks[0].nearest_heading_id

            text_for_embedding = compose_text_for_embedding(
                page_title,
                cur_blocks[0].heading_path_text,
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
                    html=html,
                    markdown=markdown,
                    chunk_text=chunk_text,
                    text_for_embedding=text_for_embedding,
                )
            )

            cur_blocks = []
            cur_text_parts = []
            cur_html_parts = []
            cur_md_parts = []

        # Iterate drafts with an index because we may push new drafts back
        i = 0
        queue: list[BlockDraft] = list(drafts)

        while i < len(queue):
            d = queue[i]
            i += 1

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
            )

            # If empty -> skip
            if not b.text:
                continue

            # Check fit to current chunk
            if not cur_blocks:
                # start new chunk
                if tokens_if_add_text(b.text) <= self.chunk_size_tokens:
                    self._append_block(cur_blocks, cur_text_parts, cur_html_parts, cur_md_parts, b)
                    final_blocks.append(b)
                    continue

                # Block alone doesn't fit -> must split
                split_done = self._split_oversize_block_into_queue(b, d, queue, i, page_title)
                if split_done:
                    # do not append b; it was replaced by new drafts inserted at position i
                    continue
                else:
                    # Fallback: force-add anyway (rare; keeps progress)
                    self._append_block(cur_blocks, cur_text_parts, cur_html_parts, cur_md_parts, b)
                    final_blocks.append(b)
                    flush_chunk()
                    continue

            # There is an active chunk
            cand = tokens_if_add_text(b.text)
            if cand <= self.chunk_size_tokens:
                self._append_block(cur_blocks, cur_text_parts, cur_html_parts, cur_md_parts, b)
                final_blocks.append(b)
                continue

            # Doesn't fit in remaining space. If current chunk is too small, try to split to fill it (2.8.1 intent).
            remaining = self.chunk_size_tokens - cur_chunk_tokens()
            if cur_chunk_tokens() < self.chunk_min_tokens and remaining > 30:
                # Try split this draft by sentences (or explode by child blocks)
                # First: if it has nested block tags, explode and retry with first child
                if self.splitter.has_nested_block_tags(d):
                    exploded = self.splitter.explode_by_child_blocks(d)
                    if exploded:
                        # Put exploded drafts back in place: process them next
                        queue[i:i] = exploded
                        continue

                parts = self.splitter.split_by_sentences_to_fit(d, remaining)
                if parts:
                    # Insert tail back to queue, head becomes b in this chunk.
                    queue[i:i] = [parts.tail]
                    # Replace current block draft with head (re-process head immediately)
                    queue[i-1:i-1] = [parts.head]  # insert head before current position
                    continue

            # Otherwise flush and retry block in a new chunk
            flush_chunk()
            # Re-process this block in the new chunk: easiest is to step back by 1 and also not lose the created Block
            # We inserted Block into final_blocks only when accepted, so we just re-insert the draft.
            queue[i:i] = [d]

        flush_chunk()

        # Compute page_text_normalized + char offsets for blocks, then patch chunk offsets
        page_text_parts: list[str] = []
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

        return ChunkingResult(blocks=final_blocks, chunks=chunks, page_text_normalized=page_text_normalized)

    @staticmethod
    def _append_block(cur_blocks, cur_text_parts, cur_html_parts, cur_md_parts, b: Block) -> None:
        cur_blocks.append(b)
        cur_text_parts.append(b.text)
        cur_html_parts.append(b.html)
        cur_md_parts.append(b.markdown)

    def _split_oversize_block_into_queue(self, block: Block, draft: BlockDraft, queue: list[BlockDraft], insert_pos: int, page_title: str) -> bool:
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
