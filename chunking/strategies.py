"""chunking.strategies

Стратегии чанкования текста (подсчёт токенов и разбиение).

Поддерживаются две стратегии:
  1. tokenizer — точный подсчёт через transformers (модель USER-bge-m3)
  2. simple   — приблизительный regex-based подсчёт (слова + пунктуация)

Важно про предложения:
  - В чанковании (ChunkBuilder) мы стараемся НЕ резать предложения на границах
    чанков. Это критично для читаемости результатов поиска.
  - При этом бывают «монструозные» предложения, которые физически не влезают
    даже в пустой чанк (например, юридические полотна без точек). Тогда резать
    внутри предложения неизбежно — и это делается как fallback.
"""

import re
from abc import ABC, abstractmethod
from typing import List, Optional

from utils.logger import logger
from config.settings import settings


# ---------------------------------------------------------------------------
# Sentence segmentation backends
# ---------------------------------------------------------------------------

# 1) Regex fallback
#
# Исторически здесь был очень простой regex: split после .!?… + пробел.
# Он ломается на типичных реальных конструкциях:
#   - кавычки/скобки: "текст." → "текст." (после точки стоит ")/»)
#   - маркеры списков/номера: "... .2." (после точки не пробел)
#   - сокращения: "г.", "т.д.", "и т.п." и т.п.
#
# Важно: regex всё равно остаётся эвристикой. Для более качественного
# сегментирования см. backends 'razdel' и 'spacy'.

_SENT_END_RE = re.compile(
    # конец предложения: один или несколько знаков конца
    r"[.!?…]+"
    # закрывающие кавычки/скобки после знаков конца
    r"(?:[\)\]\"'»]+)?"
    # далее: либо пробелы/переводы строки, либо конец строки
    r"(?:\s+|$)",
    re.UNICODE,
)


def _split_sentences_regex(text: str) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []

    out: List[str] = []
    start = 0

    for m in _SENT_END_RE.finditer(text):
        # m.group() включает и хвостовые пробелы; в предложение их не кладём
        end_no_ws = m.start() + len(m.group(0).rstrip())
        if end_no_ws <= start:
            continue
        sent = text[start:end_no_ws].strip()
        if sent:
            out.append(sent)
        start = m.end()  # пропускаем пробелы после конца предложения

    tail = text[start:].strip()
    if tail:
        out.append(tail)
    return out


# 2) Razdel backend (Natasha project, rule-based Russian segmentation)
def _split_sentences_razdel(text: str) -> Optional[List[str]]:
    try:
        from razdel import sentenize  # type: ignore
    except Exception:
        return None
    return [s.text.strip() for s in sentenize(text or "") if s.text and s.text.strip()]


# 3) spaCy backend (maintained; does NOT require a heavy model)
import threading


# spaCy-пайплайны формально не гарантируют потокобезопасность при одновременном
# использовании одного и того же объекта `nlp` из разных потоков.
#
# В нашем пайплайне чанкование выполняется в asyncio.to_thread(), т.е. параллельно
# в нескольких потоках. Чтобы сегментация предложений была детерминированной
# (и не было редких "плавающих" границ), держим `nlp` как singleton **на поток**.
_SPACY_LOCAL = threading.local()


def _get_spacy_nlp():
    nlp = getattr(_SPACY_LOCAL, 'nlp', None)
    if nlp is not None:
        return nlp

    import spacy  # type: ignore

    nlp = spacy.blank("ru")
    # Rule-based sentence boundary detection, fast, without dependency parse.
    nlp.add_pipe("sentencizer")
    _SPACY_LOCAL.nlp = nlp
    return nlp


def _split_sentences_spacy(text: str) -> Optional[List[str]]:
    try:
        nlp = _get_spacy_nlp()
    except Exception:
        return None
    doc = nlp(text or "")
    return [s.text.strip() for s in doc.sents if s.text and s.text.strip()]


def split_into_sentences(text: str) -> List[str]:
    """Разбиение текста на предложения.

    Backend выбирается через env-переменную SENTENCE_SPLITTER:
      - 'razdel' (рекомендуется для русского; лёгкий, rule-based)
      - 'spacy'  (активно поддерживается; можно без моделей)
      - 'regex'  (fallback без зависимостей)

    Пунктуация остаётся в конце каждого предложения.
    """
    backend = (getattr(settings, 'SENTENCE_SPLITTER', 'regex') or 'regex').strip().lower()

    if backend == 'razdel':
        res = _split_sentences_razdel(text)
        if res is not None:
            return res
        logger.warning("SENTENCE_SPLITTER=razdel, but razdel is not installed — falling back to regex")
        return _split_sentences_regex(text)

    if backend == 'spacy':
        res = _split_sentences_spacy(text)
        if res is not None:
            return res
        logger.warning("SENTENCE_SPLITTER=spacy, but spacy is not installed — falling back to regex")
        return _split_sentences_regex(text)

    # default
    return _split_sentences_regex(text)


# ---------------------------------------------------------------------------
# Базовый класс
# ---------------------------------------------------------------------------

class ChunkingStrategy(ABC):
    """Абстрактная стратегия подсчёта и разбиения токенов."""

    @abstractmethod
    def count_tokens(self, text: str) -> int:
        """Количество токенов в тексте."""
        ...

    def split_text(self, text: str, max_tokens: int) -> List[str]:
        """
        Разбиение текста на части ≤ max_tokens.

        Сначала пытается делить по предложениям; слишком длинные
        предложения дробит по словам.
        """
        sentences = split_into_sentences(text)
        parts: List[str] = []
        buf: List[str] = []
        buf_tokens = 0

        for sent in sentences:
            sent_tok = self.count_tokens(sent)

            if sent_tok > max_tokens:
                # Слишком длинное предложение — сначала сбрасываем буфер
                if buf:
                    parts.append(' '.join(buf))
                    buf, buf_tokens = [], 0
                # Дробим по словам
                parts.extend(self._split_by_words(sent, max_tokens))
                continue

            if buf_tokens + sent_tok > max_tokens:
                parts.append(' '.join(buf))
                buf, buf_tokens = [sent], sent_tok
            else:
                buf.append(sent)
                buf_tokens += sent_tok

        if buf:
            parts.append(' '.join(buf))

        return parts

    # ------------------------------------------------------------------

    def take_prefix(self, text: str, max_tokens: int, *, must_take: bool) -> tuple[str, str]:
        """Берёт префикс текста, который помещается в max_tokens.

        Ключевая цель: **не резать предложения** при упаковке в чанк.

        Логика:
          1) Пытаемся взять максимально длинный префикс, составленный из ЦЕЛЫХ
             предложений, пока не исчерпан бюджет.
          2) Если в бюджет не помещается НИ ОДНО предложение:
             - если must_take=False → возвращаем ("", text)
               (ChunkBuilder завершит текущий чанк и начнёт новый).
             - если must_take=True → это значит, что чанк пуст и нам нужно
               сделать прогресс. Тогда режем внутри предложения по словам.

        Возвращаемые (prefix, remainder) являются НОРМАЛИЗОВАННЫМИ строками
        (соединяем части через пробел). Для семантики/поиска это хорошо, но
        это не гарантирует байт-в-байт совпадение с оригинальным HTML.
        """
        if not text:
            return "", ""

        sentences = split_into_sentences(text)
        if not sentences:
            # нечего делить — fallback по словам, если нужно прогрессировать
            if must_take:
                wp = self._split_by_words(text, max_tokens)
                prefix = (wp[0] if wp else '').strip()
                remainder = ' '.join(wp[1:]).strip() if len(wp) > 1 else ''
                return prefix, remainder
            return "", text

        prefix_sents: List[str] = []
        used = 0

        for i, sent in enumerate(sentences):
            st = self.count_tokens(sent)

            if st > max_tokens:
                # Одно предложение больше бюджета.
                if not must_take:
                    # Внутри заполненного чанка лучше НЕ резать: просто начнём новый чанк.
                    break

                # must_take=True → чанк пустой, иначе мы застрянем. Режем по словам.
                wp = self._split_by_words(sent, max_tokens)
                if not wp:
                    return "", text
                prefix = wp[0].strip()

                remainder_words = ' '.join(wp[1:]).strip() if len(wp) > 1 else ''
                remainder_tail = ' '.join(s.strip() for s in sentences[i + 1:] if s.strip())
                remainder = ' '.join(x for x in [remainder_words, remainder_tail] if x).strip()
                return prefix, remainder

            if used + st <= max_tokens:
                prefix_sents.append(sent)
                used += st
            else:
                break

        prefix = ' '.join(s.strip() for s in prefix_sents if s.strip()).strip()
        remainder = ' '.join(s.strip() for s in sentences[len(prefix_sents):] if s.strip()).strip()

        if not prefix and must_take:
            # Если по каким-то причинам мы не смогли взять ни одного предложения,
            # всё равно обязаны прогрессировать.
            wp = self._split_by_words(text, max_tokens)
            prefix = (wp[0] if wp else '').strip()
            remainder = ' '.join(wp[1:]).strip() if len(wp) > 1 else ''

        return prefix, remainder

    # ------------------------------------------------------------------

    def _split_by_words(self, text: str, max_tokens: int) -> List[str]:
        """Разбиение строки по словам с лимитом токенов."""
        words = text.split()
        parts: List[str] = []
        buf: List[str] = []
        buf_tokens = 0

        for word in words:
            wt = self.count_tokens(word)
            if buf_tokens + wt > max_tokens and buf:
                parts.append(' '.join(buf))
                buf, buf_tokens = [], 0
            buf.append(word)
            buf_tokens += wt

        if buf:
            parts.append(' '.join(buf))

        return parts


# ---------------------------------------------------------------------------
# Стратегия: transformers tokenizer
# ---------------------------------------------------------------------------

class TokenizerStrategy(ChunkingStrategy):
    """
    Точный подсчёт токенов через tokenizer модели USER-bge-m3.

    Библиотека transformers подгружается только при выборе этой стратегии.
    """

    def __init__(self):
        try:
            from transformers import AutoTokenizer

            logger.info("Loading bge-m3 tokenizer…")
            self.tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-m3")
            logger.info("Tokenizer loaded successfully")

        except ImportError:
            raise ImportError(
                "transformers library is required for 'tokenizer' strategy. "
                "Install with: pip install transformers sentencepiece"
            )
        except Exception as exc:
            raise RuntimeError(f"Failed to load tokenizer: {exc!r}")

    def count_tokens(self, text: str) -> int:
        if not text:
            return 0
        return len(self.tokenizer.encode(text, add_special_tokens=False))


# ---------------------------------------------------------------------------
# Стратегия: простая (regex)
# ---------------------------------------------------------------------------

class SimpleStrategy(ChunkingStrategy):
    """
    Приблизительный подсчёт токенов: каждое слово и знак пунктуации = 1 токен.
    """

    _WORD_OR_PUNCT = re.compile(r'\w+|[^\w\s]', re.UNICODE)

    def count_tokens(self, text: str) -> int:
        if not text:
            return 0
        return len(self._WORD_OR_PUNCT.findall(text))


# ---------------------------------------------------------------------------
# Фабрика
# ---------------------------------------------------------------------------

def get_chunking_strategy(strategy_name: str = 'simple') -> ChunkingStrategy:
    """
    Создание стратегии по имени.

    Args:
        strategy_name: 'tokenizer' или 'simple'
    """
    if strategy_name == 'tokenizer':
        return TokenizerStrategy()
    if strategy_name == 'simple':
        return SimpleStrategy()
    raise ValueError(f"Unknown chunking strategy: {strategy_name!r}")
