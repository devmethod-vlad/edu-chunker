"""
Модели данных для работы с Confluence API и результатами парсинга.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


# ---------------------------------------------------------------------------
# Страница Confluence
# ---------------------------------------------------------------------------

@dataclass
class ConfluencePage:
    """Модель страницы Confluence, полученной через REST API."""

    id: str
    title: str
    space_key: str
    version: int
    last_modified: str
    # HTML-контент страницы (body.view — то, что видит пользователь)
    body_html: str
    url: str = ""

    @classmethod
    def from_api_response(cls, data: Dict[str, Any]) -> 'ConfluencePage':
        """
        Создание экземпляра из ответа Confluence REST API.

        Ожидаемые expand-поля: body.view, version, space.
        Если body.view отсутствует — пробуем body.storage как fallback.
        """
        body = data.get('body', {})
        # Приоритет: view (отрендеренный HTML) → storage (raw)
        html = (
            body.get('view', {}).get('value', '')
            or body.get('storage', {}).get('value', '')
        )

        return cls(
            id=str(data.get('id', '')),
            title=data.get('title', ''),
            space_key=data.get('space', {}).get('key', ''),
            version=data.get('version', {}).get('number', 1),
            last_modified=data.get('version', {}).get('when', ''),
            body_html=html,
            url=data.get('_links', {}).get('webui', ''),
        )


# ---------------------------------------------------------------------------
# Логический блок контента
# ---------------------------------------------------------------------------

@dataclass
class ContentBlock:
    """
    Логический блок контента со страницы.

    Блок — минимальная семантическая единица: параграф, элемент списка,
    строка таблицы, заголовок и т. д.
    """

    # Порядковый индекс блока от начала страницы
    index: int

    # Уникальный ID: 'EDU:{page_id}-{index}'
    id: str

    # Тип блока (родительский HTML-тег: p, div, li, tr, h2 …)
    block_type: str

    # Нормализованный текстовый контент
    text: str

    # --- Навигационные поля ---

    # XPath до элемента в DOM
    xpath: str = ""

    # CSS-селектор элемента
    css_selector: str = ""

    # Смещение в символах от начала текстового представления страницы
    text_offset: int = 0

    # Длина текста блока (в символах)
    text_length: int = 0

    # ID ближайшего родительского заголовка (h1-h6)
    parent_heading_id: Optional[str] = None

    # Реальный HTML id-атрибут элемента (для навигации по якорю)
    html_id: Optional[str] = None

    def __post_init__(self):
        self.text_length = len(self.text)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'index': self.index,
            'id': self.id,
            'block_type': self.block_type,
            'text': self.text,
            'xpath': self.xpath,
            'css_selector': self.css_selector,
            'text_offset': self.text_offset,
            'text_length': self.text_length,
            'parent_heading_id': self.parent_heading_id,
            'html_id': self.html_id,
        }


# ---------------------------------------------------------------------------
# Информация о заголовке
# ---------------------------------------------------------------------------

@dataclass
class HeadingInfo:
    """Запись о заголовке (h1-h6), встреченном при парсинге."""

    level: int          # 1-6
    text: str           # Текст заголовка
    block_id: str       # ID блока заголовка (EDU:page-index)
    block_index: int    # Индекс блока заголовка

    # Реальный id-атрибут из HTML (например <h2 id="introduction">)
    # Используется для навигации: page_url#html_id
    html_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Чанк для эмбеддинга
# ---------------------------------------------------------------------------

@dataclass
class Chunk:
    """
    Чанк — фрагмент контента, готовый для эмбеддинга и индексации.

    Формируется из одного или нескольких блоков с учётом ограничения
    по размеру в токенах, перекрытий и иерархии заголовков.
    """

    # --- Идентификация ---
    chunk_id: str               # 'EDU:{page_id}:{first_block}-{last_block}'
    page_id: str
    space_key: str
    page_title: str
    page_version: int = 0
    last_modified: str = ""

    # --- Индексы блоков ---
    block_indices: List[int] = field(default_factory=list)
    core_block_indices: List[int] = field(default_factory=list)
    overlap_prev_block_indices: List[int] = field(default_factory=list)
    overlap_next_block_indices: List[int] = field(default_factory=list)

    # --- Заголовки ---
    # Полная иерархия (для метаинформации), порядок: от наименее важного к наиболее
    full_heading_hierarchy: List[str] = field(default_factory=list)
    # Усечённая иерархия (для текста чанка, с учётом MAX_HEADING_LEVELS)
    text_heading_hierarchy: List[str] = field(default_factory=list)
    # Реальный HTML id-атрибут ближайшего заголовка (для навигации по якорю)
    nearest_heading_id: Optional[str] = None

    # --- Текстовые представления ---
    # Только собственные блоки (без overlap, без тегов) — для выдачи без дублей
    normalized_text: str = ""
    # Текст перекрытия из предыдущего чанка (может быть частичным блоком)
    overlap_prev_text: str = ""
    # Текст перекрытия из следующего чанка (может быть частичным блоком)
    overlap_next_text: str = ""
    # Все блоки с overlap, но без тегов
    full_text: str = ""
    # Готовый для эмбеддинга: [PAGE] + [SECTION] + [TEXT] + overlap + core
    embedding_text: str = ""

    # --- Навигация ---
    xpath_start: str = ""
    css_selector_start: str = ""
    text_offset_start: int = 0
    text_length: int = 0
    navigation_url: str = ""
    highlight_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'chunk_id': self.chunk_id,
            'page_id': self.page_id,
            'space_key': self.space_key,
            'page_title': self.page_title,
            'page_version': self.page_version,
            'last_modified': self.last_modified,
            'block_indices': self.block_indices,
            'core_block_indices': self.core_block_indices,
            'overlap_prev_block_indices': self.overlap_prev_block_indices,
            'overlap_next_block_indices': self.overlap_next_block_indices,
            'full_heading_hierarchy': self.full_heading_hierarchy,
            'text_heading_hierarchy': self.text_heading_hierarchy,
            'nearest_heading_id': self.nearest_heading_id,
            'normalized_text': self.normalized_text,
            'overlap_prev_text': self.overlap_prev_text,
            'overlap_next_text': self.overlap_next_text,
            'full_text': self.full_text,
            'embedding_text': self.embedding_text,
            'navigation': {
                'xpath_start': self.xpath_start,
                'css_selector_start': self.css_selector_start,
                'text_offset_start': self.text_offset_start,
                'text_length': self.text_length,
                'url': self.navigation_url,
                'highlight_metadata': self.highlight_metadata,
            },
        }
