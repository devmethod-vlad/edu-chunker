"""
Построитель чанков из логических блоков контента.

Формирует чанки заданного размера из блоков с учётом перекрытий,
иерархии заголовков и навигационных метаданных.

Ключевые решения:
  - Overlap работает на ТЕКСТОВОМ уровне — даже если блок больше бюджета
    overlap, берётся часть текста (по предложениям / словам)
  - Бюджет next-overlap РЕЗЕРВИРУЕТСЯ при формировании чанка
  - Иерархия заголовков строится корректным подъёмом по уровням
  - Слишком большие блоки реально разбиваются (а не добавляются целиком)
  - Стратегия создаётся один раз (не для каждой страницы)
"""

from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any
from urllib.parse import quote

from config.settings import settings
from confluence.models import ContentBlock, HeadingInfo, Chunk
from .strategies import ChunkingStrategy, get_chunking_strategy
from utils.logger import logger


# ---------------------------------------------------------------------------
# Внутренние структуры
# ---------------------------------------------------------------------------


@dataclass
class _PendingBlock:
    """Состояние незавершённого разбиения блока между чанками.

    Если один ContentBlock оказался слишком большим и его пришлось нарезать
    на части, мы НЕ должны терять остаток текста. В таком случае следующий
    чанк начинается не со следующего блока страницы, а с продолжения
    текущего.

    Поля:
      - block_pos: позиция блока в списке `blocks` (обычно = block.index)
      - remaining_text: оставшийся текст блока, который ещё не попал в чанки
      - start_char: смещение (в символах) от начала исходного block.text,
        где начинается remaining_text (используется для вычисления text_offset)
    """

    block_pos: int
    remaining_text: str
    start_char: int


def _dedupe_preserve_order(items: List[int]) -> List[int]:
    """Убирает дубли, сохраняя порядок появления."""
    seen: set[int] = set()
    out: List[int] = []
    for x in items:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


# ---------------------------------------------------------------------------
# Построитель чанков
# ---------------------------------------------------------------------------

class ChunkBuilder:
    """
    Строит чанки из списка ContentBlock с учётом:
      - ограничения по размеру в токенах
      - перекрытий между чанками (prev / next) — в т.ч. частичных
      - иерархии заголовков
      - навигационных метаданных
    """

    def __init__(
        self,
        strategy: ChunkingStrategy,
        chunk_size: int = 512,
        chunk_overlap: int = 0,
        max_heading_levels: int = 2,
        include_page_tag: bool = True,
        include_section_tag: bool = True,
    ):
        self.strategy = strategy
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.max_heading_levels = max_heading_levels
        self.include_page_tag = include_page_tag
        self.include_section_tag = include_section_tag

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    def build_chunks(
        self,
        blocks: List[ContentBlock],
        headings: List[HeadingInfo],
        page_id: str,
        page_title: str,
        space_key: str,
        page_version: int,
        last_modified: str,
        page_url: str,
    ) -> List[Chunk]:
        """
        Основной метод: строит список чанков из блоков страницы.

        Два прохода:
          1. Формирование чанков с overlap из предыдущего (+ резерв под next)
          2. Добавление overlap из следующего чанка (в зарезервированный бюджет)
        """
        if not blocks:
            return []

        # Индекс заголовков строим из blocks (а не из переданного списка headings),
        # потому что в процессе чанкования список blocks может меняться (мы можем
        # материализовать разбиение блока в новые ContentBlock и переиндексировать хвост).

        # --- Проход 1: строим чанки ---
        chunks: List[Chunk] = []
        pos = 0
        pending: Optional[_PendingBlock] = None

        while pos < len(blocks):
            heading_idx = self._build_heading_idx_from_blocks(blocks)
            chunk, pos, pending = self._build_one_chunk(
                blocks, pos, heading_idx,
                page_id, page_title, space_key,
                page_version, last_modified, page_url,
                prev_chunk=chunks[-1] if chunks else None,
                pending=pending,
            )
            if chunk:
                chunks.append(chunk)

        # --- Проход 2: overlap из следующего чанка ---
        if self.chunk_overlap > 0:
            self._apply_next_overlap(chunks, page_title)

        return chunks
    # ------------------------------------------------------------------
    # Нормализация блоков перед чанкованием
    # ------------------------------------------------------------------

    def normalize_blocks_for_chunking(
        self,
        blocks: List[ContentBlock],
        headings: List[HeadingInfo],
        *,
        page_id: str,
        page_title: str,
    ) -> Tuple[List[ContentBlock], List[HeadingInfo]]:
        """
        Подготовка блоков перед чанкованием.

        Проблема, которая всплывает на реальных страницах:
          - иногда один ContentBlock настолько большой, что не помещается даже в ПУСТОЙ чанк.
          - текущая логика ChunkBuilder в таком случае режет блок на части и "протаскивает"
            остаток через pending-state в следующий чанк.
          - это делает метрики `block_indices/core_block_indices/overlap_*` менее прозрачными:
            один и тот же block_index начинает жить в нескольких чанках и даже может казаться,
            что "блок перекрывает сам себя".

        Решение (без усложнения основной логики упаковки):
          - ДО формирования чанков заранее режем такие монструозные блоки по предложениям
            (fallback: по словам) и превращаем части в ОТДЕЛЬНЫЕ ContentBlock.
          - затем переиндексируем ВСЕ блоки страницы последовательно (0..N-1),
            пересчитываем block_id и text_offset и обновляем ссылки на parent_heading_id,
            а также список headings.

        Важно:
          - порядок чтения сохраняется;
          - контент не теряется;
          - изменения затрагивают только редкий случай "блок > бюджет пустого чанка".
        """
        if not blocks:
            return [], []

        heading_idx: Dict[int, HeadingInfo] = {h.block_index: h for h in headings}

        heading_tags = {f"h{i}" for i in range(1, 7)}

        @dataclass
        class _Piece:
            # исходные данные (до нормализации)
            orig_index: int
            orig_id: str
            orig_parent_heading_id: Optional[str]
            block_type: str
            text: str
            xpath: str
            css_selector: str
            html_id: Optional[str]

        pieces: List[_Piece] = []

        total_splits = 0

        # 1) Режем только действительно "монструозные" блоки (которые могут спровоцировать pending-state).
        #    Остальные блоки оставляем как есть — так проще сохранять ожидаемую блочную структуру.
        for pos, b in enumerate(blocks):
            if not (b.text or "").strip():
                continue

            # Заголовки почти никогда не бывают слишком длинными; режем их НЕ надо.
            if (b.block_type or "").lower() in heading_tags:
                pieces.append(_Piece(
                    orig_index=b.index,
                    orig_id=b.id,
                    orig_parent_heading_id=b.parent_heading_id,
                    block_type=b.block_type,
                    text=b.text,
                    xpath=b.xpath,
                    css_selector=b.css_selector,
                    html_id=b.html_id,
                ))
                continue

            # Оцениваем бюджет "пустого" чанка для этого блока:
            # - теги PAGE/SECTION/TEXT,
            # - резерв под overlap_next,
            # - и для надёжности (чтобы не резать снова из-за overlap_prev) учитываем overlap_prev.
            tags_tokens, _, _, _ = self._estimate_tags_tokens_exact(
                blocks=blocks,
                start=pos,
                ov_prev_indices=[],
                heading_idx=heading_idx,
                page_title=page_title,
            )
            overlap_reserve = self.chunk_overlap if self.chunk_overlap > 0 else 0
            budget = self.chunk_size - tags_tokens - (2 * overlap_reserve)

            # Чтобы не застрять на страницах с очень длинными заголовками/тегами,
            # не даём бюджету стать слишком маленьким.
            budget = max(10, budget)

            bt = self.strategy.count_tokens(b.text)

            if bt <= budget:
                pieces.append(_Piece(
                    orig_index=b.index,
                    orig_id=b.id,
                    orig_parent_heading_id=b.parent_heading_id,
                    block_type=b.block_type,
                    text=b.text,
                    xpath=b.xpath,
                    css_selector=b.css_selector,
                    html_id=b.html_id,
                ))
                continue

            # Монструозный блок → дробим (сначала по предложениям, fallback по словам)
            parts = self.strategy.split_text(b.text, budget)
            if not parts:
                parts = [b.text]

            total_splits += (len(parts) - 1)

            for part in parts:
                part = (part or "").strip()
                if not part:
                    continue
                pieces.append(_Piece(
                    orig_index=b.index,
                    orig_id=b.id,
                    orig_parent_heading_id=b.parent_heading_id,
                    block_type=b.block_type,
                    text=part,
                    xpath=b.xpath,
                    css_selector=b.css_selector,
                    html_id=b.html_id,
                ))

        if total_splits:
            logger.info(
                f"normalize_blocks_for_chunking: page {page_id} — "
                f"split oversized blocks into +{total_splits} extra blocks"
            )

        # 2) Переиндексация + пересчёт id/text_offset.
        #    Заодно готовим маппинг для заголовков, чтобы обновить parent_heading_id.
        old_heading_id_to_new: Dict[str, str] = {}
        old_heading_index_to_new: Dict[int, int] = {}

        new_blocks: List[ContentBlock] = []
        text_offset = 0

        for new_index, p in enumerate(pieces):
            new_id = f"EDU:{page_id}-{new_index}"

            nb = ContentBlock(
                index=new_index,
                id=new_id,
                block_type=p.block_type,
                text=p.text,
                xpath=p.xpath,
                css_selector=p.css_selector,
                text_offset=text_offset,
                parent_heading_id=p.orig_parent_heading_id,  # временно (починим после маппинга)
                html_id=p.html_id,
            )

            # Обновляем текущее смещение в текстовом представлении страницы.
            # Логика совпадает с HTMLParser._create_block(): +len(text)+1
            text_offset += len(nb.text) + 1

            new_blocks.append(nb)

            # Если это заголовок — запомним соответствие старого id -> нового id
            if (p.block_type or "").lower() in heading_tags:
                old_heading_id_to_new[p.orig_id] = new_id
                old_heading_index_to_new[p.orig_index] = new_index

        # 3) Чиним parent_heading_id у всех блоков (и у заголовков тоже).
        for nb in new_blocks:
            if nb.parent_heading_id and nb.parent_heading_id in old_heading_id_to_new:
                nb.parent_heading_id = old_heading_id_to_new[nb.parent_heading_id]

        # 4) Обновляем список headings (block_id и block_index)
        new_headings: List[HeadingInfo] = []
        for h in headings:
            if h.block_id not in old_heading_id_to_new:
                # Это может случиться только если в исходном HTML был заголовок,
                # но блок по какой-то причине не попал в pieces (например, пустой текст).
                continue

            new_headings.append(HeadingInfo(
                level=h.level,
                text=h.text,
                block_id=old_heading_id_to_new[h.block_id],
                block_index=old_heading_index_to_new[h.block_index],
                html_id=h.html_id,
            ))

        return new_blocks, new_headings
    def _build_heading_idx_from_blocks(self, blocks: List[ContentBlock]) -> Dict[int, HeadingInfo]:
        """Строит индекс заголовков напрямую из списка blocks.

        Это проще и надёжнее, чем опираться на отдельный список `headings`,
        потому что в процессе чанкования мы можем "материализовать" разбиения
        блоков (создавая новые ContentBlock) и переиндексировать хвост списка.
        Тогда `headings`, пришедшие из парсера, становятся устаревшими.
        """
        heading_tags = {f"h{i}" for i in range(1, 7)}
        idx: Dict[int, HeadingInfo] = {}
        for b in blocks:
            bt = (b.block_type or "").lower()
            if bt in heading_tags:
                try:
                    level = int(bt[1])
                except Exception:
                    continue
                idx[b.index] = HeadingInfo(
                    level=level,
                    text=b.text,
                    block_id=b.id,
                    block_index=b.index,
                    html_id=b.html_id,
                )
        return idx

    @staticmethod
    def _recompute_text_offsets(blocks: List[ContentBlock]) -> None:
        """Пересчитывает text_offset для всех блоков страницы.

        Логика совпадает с HTMLParser._create_block(): offset += len(text) + 1
        """
        offset = 0
        for b in blocks:
            b.text_offset = offset
            offset += len(b.text) + 1

    def _materialize_block_split(
        self,
        *,
        blocks: List[ContentBlock],
        split_pos: int,
        prefix_text: str,
        remainder_text: str,
        page_id: str,
    ) -> None:
        """Материализует разбиение блока: вместо pending-state создаём новый блок.

        Зачем:
          - чтобы один и тот же block_index не "жил" в нескольких чанках из-за
            переносимого остатка (pending).
          - чтобы `block_indices/core_block_indices/overlap_*` были однозначными:
            каждое предложение/фрагмент становится отдельным ContentBlock с новым index/id.

        Как:
          - текущий блок на позиции split_pos превращаем в prefix,
          - остаток вставляем как новый ContentBlock сразу после него,
          - переиндексируем хвост списка (index и id = EDU:{page_id}-{index}),
          - обновляем parent_heading_id в хвосте (для заголовков, которые сдвинулись),
          - пересчитываем text_offset.
        """
        cur = blocks[split_pos]

        # 1) Меняем текст текущего блока на prefix
        cur.text = prefix_text
        cur.text_length = len(cur.text)

        # 2) Вставляем новый блок-остаток сразу после текущего
        remainder_block = ContentBlock(
            index=split_pos + 1,  # временно; ниже всё равно переиндексируем
            id=f"EDU:{page_id}-{split_pos + 1}",
            block_type=cur.block_type,
            text=remainder_text,
            xpath=cur.xpath,
            css_selector=cur.css_selector,
            text_offset=0,  # пересчитаем после переиндексации
            parent_heading_id=cur.parent_heading_id,
            html_id=cur.html_id,
        )
        blocks.insert(split_pos + 1, remainder_block)

        # 3) Переиндексация хвоста (split_pos+1 .. end) + сбор маппинга заголовков
        heading_tags = {f"h{i}" for i in range(1, 7)}
        heading_id_map: Dict[str, str] = {}

        for j in range(split_pos + 1, len(blocks)):
            b = blocks[j]
            old_id = b.id
            b.index = j
            b.id = f"EDU:{page_id}-{j}"

            if (b.block_type or "").lower() in heading_tags:
                heading_id_map[old_id] = b.id

        # 4) Обновляем ссылки на заголовки в parent_heading_id в хвосте
        if heading_id_map:
            for b in blocks[split_pos + 1 :]:
                if b.parent_heading_id and b.parent_heading_id in heading_id_map:
                    b.parent_heading_id = heading_id_map[b.parent_heading_id]

        # 5) Пересчёт text_offset по всей странице
        self._recompute_text_offsets(blocks)

    # ------------------------------------------------------------------
    # Проход 2: overlap из следующего чанка
    # ------------------------------------------------------------------

    def _apply_next_overlap(
        self,
        chunks: List[Chunk],
        page_title: str,
    ) -> None:
        """
        Для каждого чанка (кроме последнего) добавляем overlap из начала
        следующего. Бюджет для этого зарезервирован при формировании чанка.
        """
        for i in range(len(chunks) - 1):
            cur = chunks[i]
            nxt = chunks[i + 1]

            # Собираем текст overlap из НАЧАЛА core-блоков следующего чанка
            nxt_core_frags = (
                nxt.highlight_metadata.get('core_fragments', [])
                if isinstance(nxt.highlight_metadata, dict)
                else []
            )
            ov_indices, ov_text, ov_frags = self._collect_overlap_text(
                source_fragments=nxt_core_frags,
                from_end=False,   # с начала следующего
            )
            if not ov_text:
                continue

                        # Индексы overlap_next храним только для блоков, которые НЕ входят в core.
            # Это устраняет ситуацию, когда большой блок нарезан на чанки и кажется,
            # что "блок перекрывается сам собой" по индексам.
            ov_indices_filtered = [idx for idx in ov_indices if idx not in (cur.core_block_indices or [])]
            cur.overlap_next_block_indices = ov_indices_filtered
            cur.overlap_next_text = ov_text
            # Для навигации/подсветки сохраняем фрагменты overlap
            cur.highlight_metadata.setdefault('overlap_next_fragments', [])
            cur.highlight_metadata['overlap_next_fragments'] = ov_frags

            # Добавляем в block_indices (без дублей)
            existing = set(cur.block_indices)
            cur.block_indices = cur.block_indices + [
                idx for idx in ov_indices_filtered if idx not in existing
            ]

            # Пересобираем full_text и embedding_text
            self._rebuild_texts(cur, page_title)

    # ------------------------------------------------------------------
    # Построение одного чанка
    # ------------------------------------------------------------------

    def _build_one_chunk(
        self,
        blocks: List[ContentBlock],
        start: int,
        heading_idx: Dict[int, HeadingInfo],
        page_id: str,
        page_title: str,
        space_key: str,
        page_version: int,
        last_modified: str,
        page_url: str,
        prev_chunk: Optional[Chunk] = None,
        pending: Optional[_PendingBlock] = None,
    ) -> Tuple[Optional[Chunk], int, Optional[_PendingBlock]]:
        """
        Строит один чанк начиная с blocks[start].

        Бюджет токенов распределяется так:
          chunk_size = tags + prev_overlap + core_content + next_overlap_reserve

        Returns:
            (Chunk | None, индекс следующего необработанного блока, pending_state)
        """

        # ---- Overlap из предыдущего чанка (текстовый) ----
        # pending_state — состояние «мы находимся внутри огромного блока»
        pending_state: Optional[_PendingBlock] = pending

        ov_prev_indices: List[int] = []
        ov_prev_text = ""
        ov_prev_tokens = 0
        ov_prev_frags: List[Dict[str, Any]] = []

        if prev_chunk and self.chunk_overlap > 0:
            prev_core_frags = (
                prev_chunk.highlight_metadata.get('core_fragments', [])
                if isinstance(prev_chunk.highlight_metadata, dict)
                else []
            )
            ov_prev_indices, ov_prev_text, ov_prev_frags = self._collect_overlap_text(
                source_fragments=prev_core_frags,
                from_end=True,   # с конца предыдущего
            )
            ov_prev_tokens = self.strategy.count_tokens(ov_prev_text)

        # ---- Точная оценка бюджета тегов (PAGE/SECTION/TEXT) ----
        # Важно считать теги ДО набора контента, иначе можно «переполнить» чанк.
        tags_tokens, pre_full_hier, pre_text_hier, pre_nearest_hid = self._estimate_tags_tokens_exact(
            page_title=page_title,
            blocks=blocks,
            start=start,
            heading_idx=heading_idx,
            ov_prev_indices=ov_prev_indices,
        )

        # Резервируем место для next-overlap (будет заполнен в проходе 2)
        next_ov_reserve = self.chunk_overlap if self.chunk_overlap > 0 else 0

        budget = self.chunk_size - tags_tokens - ov_prev_tokens - next_ov_reserve

        if budget <= 0:
            # Теги + overlap + резерв не влезают → жертвуем prev-overlap
            ov_prev_indices, ov_prev_text, ov_prev_tokens = [], "", 0
            budget = self.chunk_size - tags_tokens - next_ov_reserve
            if budget <= 0:
                # Даже без overlap нет места → убираем и резерв
                next_ov_reserve = 0
                budget = self.chunk_size - tags_tokens
                if budget <= 0:
                    # Теги не влезают → минимальный бюджет для хоть какого-то контента
                    budget = max(10, self.chunk_size // 4)

        # ---- Собираем собственные блоки (core) ----
        core_fragments: List[Dict[str, Any]] = []
        core_indices: List[int] = []
        core_texts: List[str] = []

        i = start
        while i < len(blocks) and budget > 0:
            block = blocks[i]

            # Текущий текст может быть «хвостом» большого блока
            if pending_state and pending_state.block_pos == i:
                cur_text = pending_state.remaining_text
                frag_start = pending_state.start_char
            else:
                cur_text = block.text
                frag_start = 0

            if not cur_text:
                # Пустой блок — просто пропускаем
                pending_state = None
                i += 1
                continue

            bt = self.strategy.count_tokens(cur_text)
            if bt <= budget:
                # Берём весь текущий текст (полностью или остаток)
                frag_text = cur_text
                frag_end = frag_start + len(frag_text)
                core_texts.append(frag_text)
                core_fragments.append(self._make_fragment(block, frag_text, frag_start, frag_end))
                if block.index not in core_indices:
                    core_indices.append(block.index)
                budget -= bt
                pending_state = None
                i += 1
                continue

            # Текст не помещается → берём «префикс», который точно влезает.
            # ВАЖНО: если чанк уже содержит контент, мы НЕ режем предложение.
            # Если следующее предложение не влезает в остаток бюджета —
            # завершаем чанк и перенесём блок целиком в следующий.
            frag_text, remainder = self._take_prefix(
                cur_text,
                budget,
                must_take=(not core_texts),
            )
            if not frag_text:
                # Защитный кейс: если стратегия не смогла выделить ни одного токена
                break

            frag_end = frag_start + len(frag_text)
            core_texts.append(frag_text)
            core_fragments.append(self._make_fragment(block, frag_text, frag_start, frag_end))
            if block.index not in core_indices:
                core_indices.append(block.index)
            budget -= self.strategy.count_tokens(frag_text)

            if remainder:
                # Вместо pending-state материализуем разбиение в отдельные блоки.
                # Тогда каждая часть будет иметь свой block_index/block_id, и не будет
                # ситуации, когда один и тот же block_index "живёт" в нескольких чанках
                # (кроме нормального overlap).
                if pending_state:
                    # Legacy fallback: если pending уже существует (старый сценарий),
                    # оставляем прежнее поведение, чтобы не ломать уже собранные чанки.
                    pending_state = _PendingBlock(
                        block_pos=i,
                        remaining_text=remainder,
                        start_char=frag_end + 1,  # +1 за пробел между частями
                    )
                else:
                    # Новый сценарий: дробим блок прямо в списке blocks, вставляя остаток
                    # отдельным ContentBlock сразу после текущего.
                    self._materialize_block_split(
                        blocks=blocks,
                        split_pos=i,
                        prefix_text=frag_text,
                        remainder_text=remainder,
                        page_id=page_id,
                    )
                    pending_state = None
                    i += 1  # следующий чанк начнётся с блока-остатка
                    break
            else:
                pending_state = None
                i += 1
        if not core_texts:
            # Нет контента → конец
            return None, len(blocks), None

        # ---- Иерархия заголовков ----
        # Мы уже посчитали её до набора контента (для точного бюджета тегов).
        full_hier, text_hier, nearest_hid = pre_full_hier, pre_text_hier, pre_nearest_hid

        # ---- Тексты ----
        normalized_text = ' '.join(core_texts)

        # full_text = prev_overlap + core (next_overlap добавится в проходе 2)
        text_parts: List[str] = []
        if ov_prev_text:
            text_parts.append(ov_prev_text)
        text_parts.append(normalized_text)
        full_text = ' '.join(text_parts)

        embedding_text = self._build_embedding_text(page_title, text_hier, full_text)

        # ---- Навигация ----
        first_frag = core_fragments[0]
        nav_url = self._build_navigation_url(page_url, first_frag, normalized_text, nearest_hid)
        highlight = {
            # Первые 100 символов текста для клиентского поиска на странице
            'text_fragment': normalized_text[:100],
            'block_type': first_frag.get('block_type'),
            'text_offset': first_frag.get('text_offset'),
            # HTML id первого блока чанка (если есть) — для якорной навигации
            'first_block_html_id': first_frag.get('html_id'),
            # HTML id ближайшего заголовка — для навигации к секции
            'nearest_heading_html_id': nearest_hid,
            # Фрагменты (для точной подсветки/перехода в UI)
            'core_fragments': core_fragments,
            'overlap_prev_fragments': ov_prev_frags,
        }

        # ---- ID чанка ----
        chunk_id_base = f"EDU:{page_id}:{core_indices[0]}-{core_indices[-1]}"
        # Сохраняем «базовый» ID отдельно: он соответствует требуемому формату.
        # Реальный chunk_id может получить суффикс, если мы режем один блок на части.
        highlight['chunk_id_base'] = chunk_id_base
        # Если чанк начинается/заканчивается внутри блока, базовый ID будет одинаковым
        # для разных частей. Чтобы не получить коллизии в индексе, добавляем суффикс
        # по смещению в тексте страницы.
        chunk_id = chunk_id_base

        # Общий список индексов: prev_overlap + core
        # Индексы overlap_prev храним только для блоков, которые НЕ входят в core.
        # Это устраняет ситуацию, когда большой блок нарезан на чанки и кажется,
        # что "блок перекрывается сам собой" по индексам.
        ov_prev_indices_filtered = [idx for idx in ov_prev_indices if idx not in core_indices]
        all_block_indices = _dedupe_preserve_order(list(ov_prev_indices_filtered) + core_indices)

        chunk = Chunk(
            chunk_id=chunk_id,
            page_id=page_id,
            space_key=space_key,
            page_title=page_title,
            page_version=page_version,
            last_modified=last_modified,
            block_indices=all_block_indices,
            core_block_indices=core_indices,
            overlap_prev_block_indices=ov_prev_indices_filtered,
            full_heading_hierarchy=full_hier,
            text_heading_hierarchy=text_hier,
            nearest_heading_id=nearest_hid,
            normalized_text=normalized_text,
            overlap_prev_text=ov_prev_text,
            full_text=full_text,
            embedding_text=embedding_text,
            xpath_start=first_frag.get('xpath', ''),
            css_selector_start=first_frag.get('css_selector', ''),
            text_offset_start=int(first_frag.get('text_offset') or 0),
            text_length=len(normalized_text),
            navigation_url=nav_url,
            highlight_metadata=highlight,
        )

        # Делаем chunk_id уникальным при нарезке одного блока на несколько чанков
        # (важно для будущей индексации/обновления по ключу).
        is_partial_chunk = bool(first_frag.get('fragment_start')) or (pending_state is not None)
        if is_partial_chunk:
            chunk.chunk_id = f"{chunk_id_base}@{chunk.text_offset_start}-{chunk.text_offset_start + chunk.text_length}"

        return chunk, i, pending_state

    # ------------------------------------------------------------------
    # Сбор текста для overlap (с поддержкой частичных блоков)
    # ------------------------------------------------------------------

    def _collect_overlap_text(
        self,
        source_fragments: List[Dict[str, Any]],
        from_end: bool,
    ) -> Tuple[List[int], str, List[Dict[str, Any]]]:
        """
        Собирает текст для перекрытия из core-блоков соседнего чанка.

        В отличие от старого _collect_overlap, работает на ТЕКСТОВОМ уровне:
        если блок не помещается целиком, берётся его часть (последние/первые
        предложения), а не пропускается полностью.

        Args:
            source_core_indices: core-индексы блоков соседнего чанка
            all_blocks: все блоки страницы
            from_end: True → берём с конца (для prev-overlap),
                      False → берём с начала (для next-overlap)

        Returns:
            (список затронутых block-индексов, текст overlap)
        """
        if not source_fragments or self.chunk_overlap <= 0:
            return [], "", []

        remaining = self.chunk_overlap

        # Порядок обхода: с конца или с начала
        frags = list(reversed(source_fragments)) if from_end else list(source_fragments)

        collected_pairs: List[Tuple[int, str]] = []   # (block_index, text_fragment)
        collected_frags: List[Dict[str, Any]] = []

        for frag in frags:
            if remaining <= 0:
                break
            idx = int(frag.get('block_index') or -1)
            text = str(frag.get('text') or '')
            if idx < 0 or not text:
                continue

            bt = self.strategy.count_tokens(text)

            if bt <= remaining:
                collected_pairs.append((idx, text))
                collected_frags.append(frag)
                remaining -= bt
            else:
                partial = self._extract_partial_text(text, remaining, from_end=from_end)
                if partial:
                    collected_pairs.append((idx, partial))
                    # Частичный фрагмент — пересчитаем смещения относительно исходного
                    # (для UI этого достаточно: мы знаем xpath/css исходного блока).
                    pf = dict(frag)
                    pf['text'] = partial
                    collected_frags.append(pf)
                    remaining -= self.strategy.count_tokens(partial)
                break

        if not collected_pairs:
            return [], "", []

        # Восстанавливаем правильный порядок если собирали с конца
        if from_end:
            collected_pairs.reverse()
            collected_frags.reverse()

        result_indices = _dedupe_preserve_order([pair[0] for pair in collected_pairs])
        result_text = ' '.join(pair[1] for pair in collected_pairs)

        return result_indices, result_text, collected_frags

    # ------------------------------------------------------------------
    # Разбиение большого блока на префикс, который точно помещается
    # ------------------------------------------------------------------

    def _take_prefix(self, text: str, max_tokens: int, *, must_take: bool) -> Tuple[str, str]:
        """Берёт префикс `text`, который помещается в `max_tokens`.

        Ключевая цель: не резать предложения при упаковке текста в чанк.

        Поведение:
          - must_take=False: если в оставшийся бюджет НЕ влезает ни одно целое
            предложение — вернём ("", text), а ChunkBuilder завершит текущий чанк
            и начнёт новый.
          - must_take=True: чанк сейчас пустой и нам надо прогрессировать,
            поэтому допускается fallback-разбиение внутри «слишком длинного»
            предложения по словам.

        Примечание:
          remainder_text является нормализованной строкой (соединяем части пробелами).
          Для семантики/поиска это ок, но это НЕ гарантирует байт-в-байт совпадение
          с оригинальным HTML.
        """
        return self.strategy.take_prefix(text, max_tokens, must_take=must_take)

    # ------------------------------------------------------------------
    # Фрагменты блоков (для навигации/подсветки)
    # ------------------------------------------------------------------

    @staticmethod
    def _make_fragment(
        block: ContentBlock,
        text: str,
        start_char: int,
        end_char: int,
    ) -> Dict[str, Any]:
        """Создаёт словарь-описание фрагмента блока.

        Этот объект специально сделан сериализуемым (чистый dict), чтобы его можно
        было класть прямо в JSON.

        Поля intentionally redundant:
          - xpath/css_selector позволяют найти DOM-элемент
          - text_offset и fragment_start/end позволяют подсветить внутри элемента
          - html_id даёт шанс прыгнуть по якорю без JS
        """
        abs_offset = int(block.text_offset) + int(start_char)
        return {
            'block_index': int(block.index),
            'block_id': block.id,
            'block_type': block.block_type,
            'xpath': block.xpath,
            'css_selector': block.css_selector,
            'html_id': block.html_id,
            'text_offset': abs_offset,
            'text_length': len(text),
            'fragment_start': int(start_char),
            'fragment_end': int(end_char),
            'text': text,
        }

    def _extract_partial_text(
        self, text: str, max_tokens: int, from_end: bool
    ) -> str:
        """
        Извлекает часть текста блока, укладывающуюся в max_tokens.

        Args:
            text: полный текст блока
            max_tokens: максимум токенов для извлечения
            from_end: True → берём ПОСЛЕДНИЕ предложения (для prev-overlap),
                      False → берём ПЕРВЫЕ предложения (для next-overlap)

        Стратегия: сначала по предложениям, если ни одно не влезает — по словам.
        """
        parts = self.strategy.split_text(text, max_tokens)
        if not parts:
            return ""

        if from_end:
            # Берём последние части, пока помещаются
            result: List[str] = []
            budget = max_tokens
            for part in reversed(parts):
                pt = self.strategy.count_tokens(part)
                if pt <= budget:
                    result.insert(0, part)
                    budget -= pt
                else:
                    break
            return ' '.join(result) if result else parts[-1][:200]
        else:
            # Берём первые части, пока помещаются
            result = []
            budget = max_tokens
            for part in parts:
                pt = self.strategy.count_tokens(part)
                if pt <= budget:
                    result.append(part)
                    budget -= pt
                else:
                    break
            return ' '.join(result) if result else parts[0][:200]

    # ------------------------------------------------------------------
    # Пересборка текстов после добавления next-overlap
    # ------------------------------------------------------------------

    def _rebuild_texts(
        self,
        chunk: Chunk,
        page_title: str,
    ) -> None:
        """
        Обновляет full_text и embedding_text с учётом нового overlap_next_text.

        Использует текстовые поля (normalized_text, overlap_prev_text,
        overlap_next_text), а не перечитывает блоки — это корректно работает
        с частичными overlap'ами.
        """
        parts: List[str] = []
        if chunk.overlap_prev_text:
            parts.append(chunk.overlap_prev_text)
        parts.append(chunk.normalized_text)
        if chunk.overlap_next_text:
            parts.append(chunk.overlap_next_text)

        chunk.full_text = ' '.join(parts)
        chunk.embedding_text = self._build_embedding_text(
            page_title, chunk.text_heading_hierarchy, chunk.full_text
        )

    # ------------------------------------------------------------------
    # Иерархия заголовков
    # ------------------------------------------------------------------

    def _build_heading_hierarchy(
        self,
        chunk_blocks: List[ContentBlock],
        heading_idx: Dict[int, HeadingInfo],
    ) -> Tuple[List[str], List[str], Optional[str]]:
        """
        Построение иерархии заголовков для чанка.

        Логика:
          1. Если чанк НАЧИНАЕТСЯ с заголовка — он и есть ближайший.
          2. Иначе — ближайший заголовок определяется по parent_heading_id
             первого блока, а дальше поднимаемся вверх по уровням.

        Иерархия хранится в порядке: h3 > h2 > h1 (от наименее к наиболее важному).
        Обрезка text_hierarchy — первые MAX_HEADING_LEVELS от наименее важного.
        nearest_heading_id — реальный HTML id-атрибут заголовка (для якорной навигации).

        Returns:
            (полная иерархия, усечённая до max_heading_levels, html_id ближайшего h)
        """
        if not chunk_blocks:
            return [], [], None

        first = chunk_blocks[0]

        # 1. Чанк начинается с заголовка?
        if first.index in heading_idx:
            h = heading_idx[first.index]
            hierarchy = self._climb_hierarchy(h, heading_idx)
            # Обрезка: первые N элементов (от наименее важного)
            text_hier = hierarchy[:self.max_heading_levels]
            return hierarchy, text_hier, h.html_id

        # 2. Ищем ближайший заголовок через parent_heading_id
        nearest_html_id: Optional[str] = None
        start_heading: Optional[HeadingInfo] = None

        if first.parent_heading_id:
            for h in heading_idx.values():
                if h.block_id == first.parent_heading_id:
                    start_heading = h
                    nearest_html_id = h.html_id
                    break

        if not start_heading:
            return [], [], None

        hierarchy = self._climb_hierarchy(start_heading, heading_idx)
        # Обрезка: первые N элементов (от наименее важного)
        text_hier = hierarchy[:self.max_heading_levels]
        return hierarchy, text_hier, nearest_html_id

    def _climb_hierarchy(
        self,
        start: HeadingInfo,
        heading_idx: Dict[int, HeadingInfo],
    ) -> List[str]:
        """
        Подъём вверх по иерархии заголовков от `start`.

        Для каждого уровня выше текущего ищем ближайший заголовок
        с block_index < текущего и level < текущего.

        Результат: [start_text, parent_text, grandparent_text, ...]
        Т.е. от наименее важного к наиболее важному: h3, h2, h1.
        Это соответствует формату из требований: "h3 > h2 > h1".
        """
        chain = [start]
        current = start

        while True:
            # Ищем ближайший заголовок ВЫШЕ по уровню и РАНЬШЕ по позиции
            best: Optional[HeadingInfo] = None
            for h in heading_idx.values():
                if h.level < current.level and h.block_index < current.block_index:
                    if best is None or h.block_index > best.block_index:
                        best = h
            if best is None:
                break
            chain.append(best)   # Добавляем В КОНЕЦ (от мелкого к крупному)
            current = best

        return [h.text for h in chain]

    # ------------------------------------------------------------------
    # Построение текстов
    # ------------------------------------------------------------------

    def _build_embedding_text(
        self,
        page_title: str,
        hierarchy: List[str],
        chunk_text: str,
    ) -> str:
        """
        Формирование текста для эмбеддинга:
          [PAGE] Название
          [SECTION] h3 > h2
          [TEXT] Текст чанка
        """
        parts: List[str] = []

        if self.include_page_tag:
            parts.append(f"[PAGE] {page_title}")

        if self.include_section_tag and hierarchy:
            parts.append(f"[SECTION] {' > '.join(hierarchy)}")

        if not parts:
            # Нет тегов → возвращаем чистый текст (без [TEXT])
            return chunk_text.strip()

        parts.append(f"[TEXT] {chunk_text.strip()}")
        return '\n'.join(parts)

    def _estimate_tags_tokens_exact(
        self,
        page_title: str,
        blocks: List[ContentBlock],
        start: int,
        heading_idx: Dict[int, HeadingInfo],
        ov_prev_indices: List[int],
    ) -> Tuple[int, List[str], List[str], Optional[str]]:
        """Точная оценка токенов для тегов PAGE/SECTION/TEXT.

        Почему это важно:
            Даже «маленькая» ошибка в оценке тегов приводит к переполнению чанка
            (токены > CHUNK_SIZE), что потом ломает индексацию/поиск.

        Мы считаем hierarchy ДО набора контента, потому что она определяется
        исключительно первым блоком чанка (или первого блока overlap).

        Returns:
            (tokens_for_tags, full_hierarchy, text_hierarchy, nearest_heading_html_id)
        """

        # Первый блок чанка — либо самый ранний блок из prev-overlap, либо start
        if ov_prev_indices:
            first_block = blocks[ov_prev_indices[0]]
        else:
            first_block = blocks[start]

        full_hier, text_hier, nearest_hid = self._build_heading_hierarchy(
            [first_block], heading_idx
        )

        parts: List[str] = []
        if self.include_page_tag:
            parts.append(f"[PAGE] {page_title}")
        if self.include_section_tag and text_hier:
            parts.append(f"[SECTION] {' > '.join(text_hier)}")

        # [TEXT] учитываем только если есть хотя бы один тег — иначе его не будет
        if parts:
            parts.append("[TEXT] ")

        tag_text = '\n'.join(parts)
        return self.strategy.count_tokens(tag_text), full_hier, text_hier, nearest_hid

    # ------------------------------------------------------------------
    # Навигация
    # ------------------------------------------------------------------

    @staticmethod
    def _build_navigation_url(
        page_url: str,
        first_fragment: Dict[str, Any],
        chunk_text: str,
        nearest_heading_html_id: Optional[str],
    ) -> str:
        """
        URL для навигации к чанку на странице.

        Приоритеты:
          1. Якорь по html_id элемента (если есть) — самый надёжный
          2. Text Fragments API (#:~:text=...) — подсветка текста в Chrome-based
        """
        first_html_id = first_fragment.get('html_id')
        if first_html_id:
            return f"{page_url}#{first_html_id}"

        # Если нет id у первого блока — но есть id ближайшего заголовка,
        # лучше прыгнуть хотя бы к секции.
        if nearest_heading_html_id:
            return f"{page_url}#{nearest_heading_html_id}"

        # Иначе — Text Fragments API
        fragment = chunk_text[:80].strip()
        encoded = quote(fragment)
        return f"{page_url}#:~:text={encoded}"


# ---------------------------------------------------------------------------
# Внутренний курсор: состояние «мы ещё не дочитали большой блок»
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Фабрика стратегии
# ---------------------------------------------------------------------------

# Важно про многопоточность:
#
# В пайплайне main.py чанкование выполняется в asyncio.to_thread(), то есть
# параллельно в нескольких потоках. В исходной версии здесь использовался
# *глобальный* singleton стратегии (_strategy_instance). Это выглядит безопасно
# для SimpleStrategy (она фактически статична), но НЕ гарантируется безопасным
# для:
#   - TokenizerStrategy (transformers tokenizer имеет внутренние кеши)
#   - spaCy sentencizer (если выбран SENTENCE_SPLITTER=spacy)
#
# Результат: при обработке нескольких страниц параллельно могли «плавать» оценки
# бюджета/границы чанков, а значит и состав block_indices/core/overlap.
#
# Исправление: держим стратегию как singleton **на поток** (thread-local).
# Тогда:
#   - один поток использует один экземпляр стратегии
#   - разные потоки не делят внутренние кеши и не мешают друг другу
#   - при обработке одной страницы или многих результат для конкретной страницы
#     становится идентичным.

import threading

_strategy_local = threading.local()


def _get_strategy() -> ChunkingStrategy:
    inst = getattr(_strategy_local, 'instance', None)
    name = getattr(_strategy_local, 'strategy_name', None)

    # Если стратегию поменяли через env (редко, но возможно) — обновим её.
    cur_name = settings.CHUNKING_STRATEGY
    if inst is None or name != cur_name:
        inst = get_chunking_strategy(cur_name)
        _strategy_local.instance = inst
        _strategy_local.strategy_name = cur_name
    return inst


def create_chunks_from_page(
    blocks: List[ContentBlock],
    headings: List[HeadingInfo],
    page_id: str,
    page_title: str,
    space_key: str,
    page_version: int,
    last_modified: str,
    page_url: str,
) -> List[Chunk]:
    """
    Высокоуровневая функция: создание чанков из блоков одной страницы.
    """
    builder = ChunkBuilder(
        strategy=_get_strategy(),
        chunk_size=settings.CHUNK_SIZE,
        chunk_overlap=settings.CHUNK_OVERLAP,
        max_heading_levels=settings.MAX_HEADING_LEVELS,
        include_page_tag=settings.INCLUDE_PAGE_TAG,
        include_section_tag=settings.INCLUDE_SECTION_TAG,
    )

    # NB: нормализация может добавить новые блоки (если какой-то исходный блок
    #     слишком большой для пустого чанка) и переиндексировать страницу.
    #     Важно делать это ДО формирования чанков, чтобы:
    #       - `block_indices` в чанках соответствовали реальным `blocks`
    #       - не возникала путаница с "один блок в нескольких чанках"
    norm_blocks, norm_headings = builder.normalize_blocks_for_chunking(
        blocks=blocks,
        headings=headings,
        page_id=page_id,
        page_title=page_title,
    )
    # Обновляем список blocks "на месте": process_page() возвращает blocks наружу.
    blocks[:] = norm_blocks
    headings = norm_headings

    return builder.build_chunks(
        blocks=blocks,
        headings=headings,
        page_id=page_id,
        page_title=page_title,
        space_key=space_key,
        page_version=page_version,
        last_modified=last_modified,
        page_url=page_url,
    )
