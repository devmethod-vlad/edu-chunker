"""
Стратегии чанкования текста (подсчёт токенов и разбиение).

Поддерживаются две стратегии:
  1. tokenizer — точный подсчёт через transformers (модель USER-bge-m3)
  2. simple   — приблизительный regex-based подсчёт (слова + пунктуация)
"""

import re
from abc import ABC, abstractmethod
from typing import List

from utils.logger import logger


# ---------------------------------------------------------------------------
# Общее: разбиение текста на предложения
# ---------------------------------------------------------------------------

# Regex: разбиваем ПОСЛЕ знаков конца предложения + пробела,
# но сохраняем пунктуацию в предыдущем предложении (lookbehind).
_SENTENCE_SPLIT = re.compile(r'(?<=[.!?…])\s+')


def split_into_sentences(text: str) -> List[str]:
    """
    Разбиение текста на предложения.

    Пунктуация остаётся в конце каждого предложения, а не теряется.
    """
    parts = _SENTENCE_SPLIT.split(text)
    return [s.strip() for s in parts if s.strip()]


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
