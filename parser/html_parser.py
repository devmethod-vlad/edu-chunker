"""
Парсер HTML-контента страниц Confluence.

Извлекает логические блоки из HTML, сохраняя структуру и порядок
отображения на реальной странице.

Ключевые улучшения:
  - Корректная обработка «смешанного» контента (текст + блочные дети)
  - Текст ПОСЛЕ вложенных списков не теряется
  - XPath с индексами одноимённых siblings (для точной навигации)
  - CSS-селектор с :nth-of-type вместо обрезки на глубине 5
"""

import re
from typing import List, Tuple, Optional
from bs4 import BeautifulSoup, Tag, NavigableString

from config.settings import settings
from confluence.models import ContentBlock, HeadingInfo
from utils.logger import logger


# ---------------------------------------------------------------------------
# Константы
# ---------------------------------------------------------------------------

# Нормализация пробелов
_WS = re.compile(r'[ \t]+')           # горизонтальные пробелы
_MULTI_NL = re.compile(r'\n{3,}')     # три и более переносов → два
NBSP = '\xa0'


# ---------------------------------------------------------------------------
# Парсер
# ---------------------------------------------------------------------------

class HTMLParser:
    """
    Парсер HTML → список ContentBlock + список HeadingInfo.

    Проходит по DOM-дереву, рекурсивно обрабатывает блочные элементы,
    разбивает списки по элементам, таблицы по строкам.
    """

    def __init__(self):
        self.block_tags = set(settings.BLOCK_TAGS)
        self.excluded_tags = set(settings.EXCLUDED_TAGS)

        # Префиксы классов и id для исключения элементов
        self.excluded_classes: List[str] = settings.EXCLUDED_CLASSES
        self.excluded_ids: List[str] = settings.EXCLUDED_IDS

        # Инлайн-теги: не образуют собственных блоков
        self.inline_tags = {
            'strong', 'em', 'u', 'sup', 'sub', 'span',
            'small', 'big', 'br', 'a', 'img', 'b', 'i', 'mark', 'abbr',
        }

        self.heading_tags = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6'}

        # Состояние (сбрасывается в parse())
        self._block_counter = 0
        self._text_offset = 0
        self.blocks: List[ContentBlock] = []
        self.headings: List[HeadingInfo] = []
        self._heading_stack: List[HeadingInfo] = []

    # ------------------------------------------------------------------
    # Публичный API
    # ------------------------------------------------------------------

    def parse(
        self, html_content: str, page_id: str
    ) -> Tuple[List[ContentBlock], List[HeadingInfo]]:
        """
        Парсинг HTML и извлечение логических блоков.

        Args:
            html_content: HTML-контент страницы (body.view)
            page_id: ID страницы

        Returns:
            (список блоков, список заголовков)
        """
        # Сброс состояния
        self._block_counter = 0
        self._text_offset = 0
        self.blocks = []
        self.headings = []
        self._heading_stack = []

        soup = BeautifulSoup(html_content, 'lxml')

        # Удаляем исключённые теги (code, script, style, hr …)
        for tag_name in self.excluded_tags:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        # Удаляем элементы с исключёнными классами
        if self.excluded_classes:
            for el in soup.find_all(self._match_excluded_class):
                el.decompose()

        # Удаляем элементы с исключёнными id
        if self.excluded_ids:
            for el in soup.find_all(self._match_excluded_id):
                el.decompose()

        # Заменяем <br> на пробельный текстовый узел (для корректного извлечения текста)
        for br in soup.find_all('br'):
            br.replace_with(' ')

        root = soup.find('body') or soup
        self._walk(root, page_id, xpath_parts=[], css_parts=[])

        return self.blocks, self.headings

    # ------------------------------------------------------------------
    # Рекурсивный обход
    # ------------------------------------------------------------------

    def _walk(
        self,
        element: Tag,
        page_id: str,
        xpath_parts: List[str],
        css_parts: List[str],
    ) -> None:
        """Рекурсивный обход DOM-дерева."""
        if not isinstance(element, Tag):
            return

        tag = element.name.lower()

        if tag in self.excluded_tags:
            return

        # Проверяем исключение по class / id
        if self._should_exclude(element):
            return

        # Обновляем пути
        x_part = self._xpath_segment(element)
        c_part = self._css_segment(element)
        cur_xpath = xpath_parts + [x_part]
        cur_css = css_parts + [c_part]

        if tag in self.block_tags:
            self._process_block(element, page_id, cur_xpath, cur_css)
        else:
            # Не блочный и не исключённый → просто идём вглубь
            for child in element.children:
                if isinstance(child, Tag):
                    self._walk(child, page_id, cur_xpath, cur_css)

    # ------------------------------------------------------------------
    # Обработка блочного элемента
    # ------------------------------------------------------------------

    def _process_block(
        self,
        element: Tag,
        page_id: str,
        xpath_parts: List[str],
        css_parts: List[str],
    ) -> None:
        tag = element.name.lower()

        # Специальная обработка списков
        if tag in ('ul', 'ol'):
            self._process_list(element, page_id, xpath_parts, css_parts)
            return

        # Специальная обработка таблиц
        if tag == 'table':
            self._process_table(element, page_id, xpath_parts, css_parts)
            return

        # Заголовки
        if tag in self.heading_tags:
            self._process_heading(element, page_id, xpath_parts, css_parts)
            return

        # Проверяем наличие блочных детей
        has_block_child = any(
            isinstance(ch, Tag) and ch.name.lower() in self.block_tags
            for ch in element.children
        )

        if has_block_child:
            # Смешанный контент: собираем «островки» текста между блочными детьми
            self._process_mixed_content(element, page_id, xpath_parts, css_parts)
        else:
            # Лист: извлекаем текст целиком
            text = self._extract_text(element)
            if text:
                self._create_block(text, tag, page_id, xpath_parts, css_parts, element)

    # ------------------------------------------------------------------
    # Смешанный контент (текст + блочные дети внутри одного элемента)
    # ------------------------------------------------------------------

    def _process_mixed_content(
        self,
        element: Tag,
        page_id: str,
        xpath_parts: List[str],
        css_parts: List[str],
    ) -> None:
        """
        Обрабатывает элемент, содержащий и инлайн-контент, и блочных детей.

        Пример: <div>Текст<p>Параграф</p>Ещё текст</div>
        — «Текст» и «Ещё текст» не должны потеряться.
        """
        inline_buf: List[str] = []

        def _flush_inline():
            joined = ' '.join(inline_buf).strip()
            inline_buf.clear()
            if joined:
                self._create_block(
                    joined,
                    element.name.lower(),
                    page_id, xpath_parts, css_parts, element,
                )

        for child in element.children:
            if isinstance(child, NavigableString):
                t = str(child).strip()
                if t:
                    inline_buf.append(t)
            elif isinstance(child, Tag):
                child_tag = child.name.lower()
                if child_tag in self.block_tags:
                    _flush_inline()
                    self._walk(child, page_id, xpath_parts, css_parts)
                elif child_tag not in self.excluded_tags:
                    # Инлайн-тег — достаём текст
                    t = self._extract_text(child)
                    if t:
                        inline_buf.append(t)

        _flush_inline()

    # ------------------------------------------------------------------
    # Списки
    # ------------------------------------------------------------------

    def _process_list(
        self,
        list_el: Tag,
        page_id: str,
        xpath_parts: List[str],
        css_parts: List[str],
    ) -> None:
        """Разбиение списка на отдельные <li>-блоки."""
        for idx, li in enumerate(list_el.find_all('li', recursive=False)):
            li_x = xpath_parts + [f'li[{idx + 1}]']
            li_c = css_parts + [f'li:nth-of-type({idx + 1})']

            # Собираем текст ДО, МЕЖДУ и ПОСЛЕ вложенных списков
            nested_lists = li.find_all(['ul', 'ol'], recursive=False)

            if not nested_lists:
                text = self._extract_text(li)
                if text:
                    self._create_block(text, 'li', page_id, li_x, li_c, li)
                continue

            # Если есть вложенные списки — проходим по children
            inline_buf: List[str] = []

            def _flush():
                joined = ' '.join(inline_buf).strip()
                inline_buf.clear()
                if joined:
                    self._create_block(joined, 'li', page_id, li_x, li_c, li)

            for child in li.children:
                if isinstance(child, NavigableString):
                    t = str(child).strip()
                    if t:
                        inline_buf.append(t)
                elif isinstance(child, Tag):
                    ctag = child.name.lower()
                    if ctag in ('ul', 'ol'):
                        _flush()
                        self._process_list(child, page_id, li_x, li_c)
                    else:
                        t = self._extract_text(child)
                        if t:
                            inline_buf.append(t)

            _flush()

    # ------------------------------------------------------------------
    # Таблицы
    # ------------------------------------------------------------------

    def _process_table(
        self,
        table_el: Tag,
        page_id: str,
        xpath_parts: List[str],
        css_parts: List[str],
    ) -> None:
        """Разбиение таблицы на строки (tr). Ячейки — слева направо через ' | '."""
        for row_idx, row in enumerate(table_el.find_all('tr')):
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            parts = []
            for cell in cells:
                t = self._extract_text(cell)
                if t:
                    parts.append(t)

            if parts:
                row_text = ' | '.join(parts)
                row_x = xpath_parts + [f'tr[{row_idx + 1}]']
                row_c = css_parts + [f'tr:nth-of-type({row_idx + 1})']
                self._create_block(row_text, 'tr', page_id, row_x, row_c, row)

    # ------------------------------------------------------------------
    # Заголовки
    # ------------------------------------------------------------------

    def _process_heading(
        self,
        heading_el: Tag,
        page_id: str,
        xpath_parts: List[str],
        css_parts: List[str],
    ) -> None:
        tag = heading_el.name.lower()
        text = self._extract_text(heading_el)
        if not text:
            return

        block = self._create_block(text, tag, page_id, xpath_parts, css_parts, heading_el)
        level = int(tag[1])

        # Реальный id-атрибут заголовка из HTML (для якорной навигации)
        html_id = heading_el.get('id')

        info = HeadingInfo(
            level=level,
            text=text,
            block_id=block.id,
            block_index=block.index,
            html_id=html_id,
        )
        self.headings.append(info)

        # Обновляем стек: убираем заголовки того же или более низкого уровня
        self._heading_stack = [h for h in self._heading_stack if h.level < level]
        self._heading_stack.append(info)

    # ------------------------------------------------------------------
    # Нормализация текста
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_text(element: Tag) -> str:
        """
        Извлечение и нормализация текста из элемента.

        Используем get_text() БЕЗ separator — это позволяет корректно
        обрабатывать случаи когда инлайн-теги разрывают слово:
          <strong>Э</strong><strong>моциональное</strong> → «Эмоциональное»
        а не «Э моциональное» (как было бы с separator=' ').

        <br> заменяется на пробел ещё до вызова этого метода (в parse()).
        """
        raw = element.get_text()
        raw = raw.replace(NBSP, ' ')
        raw = _WS.sub(' ', raw)           # множественные пробелы → один
        raw = _MULTI_NL.sub('\n\n', raw)   # лишние переносы
        return raw.strip()

    # ------------------------------------------------------------------
    # Исключение элементов по class / id
    # ------------------------------------------------------------------

    def _should_exclude(self, element: Tag) -> bool:
        """
        Проверка: нужно ли исключить элемент из парсинга по class или id.

        Элемент исключается если:
          - хотя бы один его CSS-класс РАВЕН или НАЧИНАЕТСЯ С любого
            значения из EXCLUDED_CLASSES
          - его id-атрибут РАВЕН или НАЧИНАЕТСЯ С любого значения
            из EXCLUDED_IDS
        Сравнение с учётом регистра.
        """
        # Проверка по id
        if self.excluded_ids:
            el_id = element.get('id', '')
            if el_id:
                for prefix in self.excluded_ids:
                    if el_id == prefix or el_id.startswith(prefix):
                        return True

        # Проверка по классам
        if self.excluded_classes:
            el_classes = element.get('class', [])
            if isinstance(el_classes, str):
                el_classes = [el_classes]
            for cls in el_classes:
                for prefix in self.excluded_classes:
                    if cls == prefix or cls.startswith(prefix):
                        return True

        return False

    def _match_excluded_class(self, tag: Tag) -> bool:
        """Фильтр для soup.find_all: совпадение по excluded_classes."""
        if not isinstance(tag, Tag):
            return False
        el_classes = tag.get('class', [])
        if isinstance(el_classes, str):
            el_classes = [el_classes]
        for cls in el_classes:
            for prefix in self.excluded_classes:
                if cls == prefix or cls.startswith(prefix):
                    return True
        return False

    def _match_excluded_id(self, tag: Tag) -> bool:
        """Фильтр для soup.find_all: совпадение по excluded_ids."""
        if not isinstance(tag, Tag):
            return False
        el_id = tag.get('id', '')
        if el_id:
            for prefix in self.excluded_ids:
                if el_id == prefix or el_id.startswith(prefix):
                    return True
        return False

    # ------------------------------------------------------------------
    # Создание блока
    # ------------------------------------------------------------------

    def _create_block(
        self,
        text: str,
        block_type: str,
        page_id: str,
        xpath_parts: List[str],
        css_parts: List[str],
        element: Tag,
    ) -> ContentBlock:
        """Создание и регистрация ContentBlock."""
        block_id = f"EDU:{page_id}-{self._block_counter}"
        xpath = '/' + '/'.join(xpath_parts)
        css_selector = ' > '.join(css_parts)

        parent_heading_id = (
            self._heading_stack[-1].block_id if self._heading_stack else None
        )

        # Реальный id-атрибут из HTML (для навигации по якорю)
        html_id = element.get('id') if isinstance(element, Tag) else None

        block = ContentBlock(
            index=self._block_counter,
            id=block_id,
            block_type=block_type,
            text=text,
            xpath=xpath,
            css_selector=css_selector,
            text_offset=self._text_offset,
            parent_heading_id=parent_heading_id,
            html_id=html_id,
        )

        self.blocks.append(block)
        self._block_counter += 1
        self._text_offset += len(text) + 1
        return block

    # ------------------------------------------------------------------
    # Вспомогательные: XPath / CSS сегменты
    # ------------------------------------------------------------------

    @staticmethod
    def _xpath_segment(element: Tag) -> str:
        """XPath сегмент вида 'div[2]' (индекс среди одноимённых siblings)."""
        tag = element.name
        idx = 1
        for sib in element.previous_siblings:
            if isinstance(sib, Tag) and sib.name == tag:
                idx += 1
        return f'{tag}[{idx}]'

    @staticmethod
    def _css_segment(element: Tag) -> str:
        """CSS-сегмент с id / классами / nth-of-type."""
        tag = element.name

        eid = element.get('id')
        if eid:
            return f'{tag}#{eid}'

        classes = element.get('class')
        if classes:
            cls_str = '.'.join(classes) if isinstance(classes, list) else classes
            return f'{tag}.{cls_str}'

        # nth-of-type для уникальности
        idx = 1
        for sib in element.previous_siblings:
            if isinstance(sib, Tag) and sib.name == tag:
                idx += 1
        return f'{tag}:nth-of-type({idx})'


# ---------------------------------------------------------------------------
# Публичная функция-обёртка
# ---------------------------------------------------------------------------

def parse_page_content(
    html_content: str, page_id: str
) -> Tuple[List[ContentBlock], List[HeadingInfo]]:
    """
    Парсинг HTML-контента страницы.

    Returns:
        (список блоков, список заголовков)
    """
    parser = HTMLParser()
    return parser.parse(html_content, page_id)
