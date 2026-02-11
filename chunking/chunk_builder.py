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

from typing import List, Optional, Tuple, Dict
from urllib.parse import quote

from config.settings import settings
from confluence.models import ContentBlock, HeadingInfo, Chunk
from .strategies import ChunkingStrategy, get_chunking_strategy
from utils.logger import logger


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

        # Индекс заголовков по block_index для быстрого поиска
        heading_idx: Dict[int, HeadingInfo] = {h.block_index: h for h in headings}

        # --- Проход 1: строим чанки ---
        chunks: List[Chunk] = []
        pos = 0

        while pos < len(blocks):
            chunk, next_pos = self._build_one_chunk(
                blocks, pos, heading_idx,
                page_id, page_title, space_key,
                page_version, last_modified, page_url,
                prev_chunk=chunks[-1] if chunks else None,
            )
            if chunk:
                chunks.append(chunk)
            pos = next_pos

        # --- Проход 2: overlap из следующего чанка ---
        if self.chunk_overlap > 0:
            self._apply_next_overlap(chunks, blocks, page_title)

        return chunks

    # ------------------------------------------------------------------
    # Проход 2: overlap из следующего чанка
    # ------------------------------------------------------------------

    def _apply_next_overlap(
        self,
        chunks: List[Chunk],
        all_blocks: List[ContentBlock],
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
            ov_indices, ov_text = self._collect_overlap_text(
                source_core_indices=nxt.core_block_indices,
                all_blocks=all_blocks,
                from_end=False,   # с начала следующего
            )
            if not ov_text:
                continue

            cur.overlap_next_block_indices = ov_indices
            cur.overlap_next_text = ov_text

            # Добавляем в block_indices (без дублей)
            existing = set(cur.block_indices)
            cur.block_indices = cur.block_indices + [
                idx for idx in ov_indices if idx not in existing
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
    ) -> Tuple[Optional[Chunk], int]:
        """
        Строит один чанк начиная с blocks[start].

        Бюджет токенов распределяется так:
          chunk_size = tags + prev_overlap + core_content + next_overlap_reserve

        Returns:
            (Chunk | None, индекс следующего необработанного блока)
        """

        # ---- Overlap из предыдущего чанка (текстовый) ----
        ov_prev_indices: List[int] = []
        ov_prev_text = ""
        ov_prev_tokens = 0

        if prev_chunk and self.chunk_overlap > 0:
            ov_prev_indices, ov_prev_text = self._collect_overlap_text(
                source_core_indices=prev_chunk.core_block_indices,
                all_blocks=blocks,
                from_end=True,   # с конца предыдущего
            )
            ov_prev_tokens = self.strategy.count_tokens(ov_prev_text)

        # ---- Предварительная оценка бюджета тегов ----
        estimated_tags_tokens = self._estimate_tags_tokens(
            page_title, blocks, start, heading_idx
        )

        # Резервируем место для next-overlap (будет заполнен в проходе 2)
        next_ov_reserve = self.chunk_overlap if self.chunk_overlap > 0 else 0

        budget = self.chunk_size - estimated_tags_tokens - ov_prev_tokens - next_ov_reserve

        if budget <= 0:
            # Теги + overlap + резерв не влезают → жертвуем prev-overlap
            ov_prev_indices, ov_prev_text, ov_prev_tokens = [], "", 0
            budget = self.chunk_size - estimated_tags_tokens - next_ov_reserve
            if budget <= 0:
                # Даже без overlap нет места → убираем и резерв
                next_ov_reserve = 0
                budget = self.chunk_size - estimated_tags_tokens
                if budget <= 0:
                    # Теги не влезают → минимальный бюджет для хоть какого-то контента
                    budget = max(10, self.chunk_size // 4)

        # ---- Собираем собственные блоки (core) ----
        core_blocks: List[ContentBlock] = []
        core_indices: List[int] = []
        i = start

        while i < len(blocks) and budget > 0:
            block = blocks[i]
            bt = self.strategy.count_tokens(block.text)

            if bt <= budget:
                core_blocks.append(block)
                core_indices.append(block.index)
                budget -= bt
                i += 1
            else:
                if not core_blocks:
                    # Первый блок не влезает целиком → разбиваем по предложениям/словам
                    parts = self.strategy.split_text(block.text, budget)
                    if parts:
                        # Создаём «виртуальный» блок с усечённым текстом
                        trimmed = ContentBlock(
                            index=block.index,
                            id=block.id,
                            block_type=block.block_type,
                            text=parts[0],
                            xpath=block.xpath,
                            css_selector=block.css_selector,
                            text_offset=block.text_offset,
                            parent_heading_id=block.parent_heading_id,
                            html_id=block.html_id,
                        )
                        core_blocks.append(trimmed)
                        core_indices.append(block.index)
                    i += 1
                break

        if not core_blocks:
            # Нет контента → конец
            return None, len(blocks)

        # ---- Иерархия заголовков ----
        # Для определения иерархии используем prev-overlap блоки + core
        hier_blocks: List[ContentBlock] = []
        if ov_prev_indices:
            hier_blocks.extend(blocks[idx] for idx in ov_prev_indices if idx < len(blocks))
        hier_blocks.extend(core_blocks)

        full_hier, text_hier, nearest_hid = self._build_heading_hierarchy(
            hier_blocks, heading_idx
        )

        # ---- Тексты ----
        normalized_text = ' '.join(b.text for b in core_blocks)

        # full_text = prev_overlap + core (next_overlap добавится в проходе 2)
        text_parts: List[str] = []
        if ov_prev_text:
            text_parts.append(ov_prev_text)
        text_parts.append(normalized_text)
        full_text = ' '.join(text_parts)

        embedding_text = self._build_embedding_text(page_title, text_hier, full_text)

        # ---- Навигация ----
        first_core = core_blocks[0]
        nav_url = self._build_navigation_url(page_url, first_core, normalized_text)
        highlight = {
            # Первые 100 символов текста для клиентского поиска на странице
            'text_fragment': normalized_text[:100],
            'block_type': first_core.block_type,
            'text_offset': first_core.text_offset,
            # HTML id первого блока чанка (если есть) — для якорной навигации
            'first_block_html_id': first_core.html_id,
            # HTML id ближайшего заголовка — для навигации к секции
            'nearest_heading_html_id': nearest_hid,
        }

        # ---- ID чанка ----
        chunk_id = f"EDU:{page_id}:{core_indices[0]}-{core_indices[-1]}"

        # Общий список индексов: prev_overlap + core
        all_block_indices = list(ov_prev_indices) + core_indices

        chunk = Chunk(
            chunk_id=chunk_id,
            page_id=page_id,
            space_key=space_key,
            page_title=page_title,
            page_version=page_version,
            last_modified=last_modified,
            block_indices=all_block_indices,
            core_block_indices=core_indices,
            overlap_prev_block_indices=ov_prev_indices,
            full_heading_hierarchy=full_hier,
            text_heading_hierarchy=text_hier,
            nearest_heading_id=nearest_hid,
            normalized_text=normalized_text,
            overlap_prev_text=ov_prev_text,
            full_text=full_text,
            embedding_text=embedding_text,
            xpath_start=first_core.xpath,
            css_selector_start=first_core.css_selector,
            text_offset_start=first_core.text_offset,
            text_length=len(normalized_text),
            navigation_url=nav_url,
            highlight_metadata=highlight,
        )

        return chunk, i

    # ------------------------------------------------------------------
    # Сбор текста для overlap (с поддержкой частичных блоков)
    # ------------------------------------------------------------------

    def _collect_overlap_text(
        self,
        source_core_indices: List[int],
        all_blocks: List[ContentBlock],
        from_end: bool,
    ) -> Tuple[List[int], str]:
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
        if not source_core_indices or self.chunk_overlap <= 0:
            return [], ""

        remaining = self.chunk_overlap

        # Порядок обхода: с конца или с начала
        indices = list(reversed(source_core_indices)) if from_end else list(source_core_indices)

        collected_pairs: List[Tuple[int, str]] = []   # (block_index, text_fragment)

        for idx in indices:
            if remaining <= 0:
                break
            if idx >= len(all_blocks):
                continue

            block = all_blocks[idx]
            bt = self.strategy.count_tokens(block.text)

            if bt <= remaining:
                # Блок помещается целиком
                collected_pairs.append((idx, block.text))
                remaining -= bt
            else:
                # Блок НЕ помещается целиком → берём часть текста
                partial = self._extract_partial_text(
                    block.text, remaining, from_end=from_end
                )
                if partial:
                    collected_pairs.append((idx, partial))
                    remaining -= self.strategy.count_tokens(partial)
                # После частичного извлечения прекращаем — нет смысла идти дальше
                break

        if not collected_pairs:
            return [], ""

        # Восстанавливаем правильный порядок если собирали с конца
        if from_end:
            collected_pairs.reverse()

        result_indices = [pair[0] for pair in collected_pairs]
        result_text = ' '.join(pair[1] for pair in collected_pairs)

        return result_indices, result_text

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

    def _estimate_tags_tokens(
        self,
        page_title: str,
        blocks: List[ContentBlock],
        start: int,
        heading_idx: Dict[int, HeadingInfo],
    ) -> int:
        """Приблизительная оценка количества токенов, занимаемых тегами."""
        parts: List[str] = []

        if self.include_page_tag:
            parts.append(f"[PAGE] {page_title}")

        if self.include_section_tag:
            # Предполагаем максимально длинную секцию для безопасного бюджета
            sample_hier = self._quick_hierarchy(blocks, start, heading_idx)
            if sample_hier:
                parts.append(f"[SECTION] {' > '.join(sample_hier)}")

        if parts:
            parts.append("[TEXT] ")

        tag_text = '\n'.join(parts)
        return self.strategy.count_tokens(tag_text)

    def _quick_hierarchy(
        self,
        blocks: List[ContentBlock],
        start: int,
        heading_idx: Dict[int, HeadingInfo],
    ) -> List[str]:
        """Быстрая оценка иерархии для блока start (без полного climb)."""
        block = blocks[start]

        if block.index in heading_idx:
            return [heading_idx[block.index].text]

        if block.parent_heading_id:
            for h in heading_idx.values():
                if h.block_id == block.parent_heading_id:
                    return [h.text]

        return []

    # ------------------------------------------------------------------
    # Навигация
    # ------------------------------------------------------------------

    @staticmethod
    def _build_navigation_url(
        page_url: str,
        first_block: ContentBlock,
        chunk_text: str,
    ) -> str:
        """
        URL для навигации к чанку на странице.

        Приоритеты:
          1. Якорь по html_id элемента (если есть) — самый надёжный
          2. Text Fragments API (#:~:text=...) — подсветка текста в Chrome-based
        """
        # Если у первого блока есть реальный HTML id — используем якорь
        if first_block.html_id:
            return f"{page_url}#{first_block.html_id}"

        # Иначе — Text Fragments API
        fragment = chunk_text[:80].strip()
        encoded = quote(fragment)
        return f"{page_url}#:~:text={encoded}"


# ---------------------------------------------------------------------------
# Фабрика (стратегия создаётся ОДИН РАЗ на всё время работы)
# ---------------------------------------------------------------------------

# Синглтон стратегии — создаётся при первом вызове
_strategy_instance: Optional[ChunkingStrategy] = None


def _get_strategy() -> ChunkingStrategy:
    global _strategy_instance
    if _strategy_instance is None:
        _strategy_instance = get_chunking_strategy(settings.CHUNKING_STRATEGY)
    return _strategy_instance


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
