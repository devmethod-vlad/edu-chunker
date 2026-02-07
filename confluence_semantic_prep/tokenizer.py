from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Callable

logger = logging.getLogger(__name__)

_WORD_OR_PUNCT = re.compile(r"\w+|[^\w\s]", re.UNICODE)


def simple_token_count(text: str) -> int:
    return len(_WORD_OR_PUNCT.findall(text))


@dataclass(slots=True)
class TokenCounter:
    strategy: str = "simple"
    hf_tokenizer: object | None = None
    # Cached count function – avoids per-call overhead for HF tokenizers
    # and prevents the O(n²) behaviour that occurred when the chunker
    # re-tokenised the full accumulated text on every iteration.
    _count_fn: Callable[[str], int] | None = field(default=None, repr=False)

    @classmethod
    def from_settings(cls, strategy: str, tokenizer_local_path: str | None) -> "TokenCounter":
        strategy = (strategy or "simple").lower()
        if strategy == "hf":
            try:
                from transformers import AutoTokenizer  # type: ignore
            except Exception as e:
                logger.warning("TOKEN_COUNT_STRATEGY=hf but transformers not available (%s). Falling back to simple.", e)
                return cls(strategy="simple")

            if not tokenizer_local_path:
                logger.warning("TOKEN_COUNT_STRATEGY=hf but TOKENIZER_LOCAL_PATH is empty. Falling back to simple.")
                return cls(strategy="simple")

            tok = AutoTokenizer.from_pretrained(tokenizer_local_path, local_files_only=True)
            counter = cls(strategy="hf", hf_tokenizer=tok)
            # Build a cached version of the HF encode call. The LRU cache
            # dramatically reduces the number of actual tokenizations that
            # happen during the chunking loop where the same prefix text is
            # re-counted many times.
            @lru_cache(maxsize=4096)
            def _hf_count(text: str) -> int:
                return len(tok.encode(text, add_special_tokens=False))

            counter._count_fn = _hf_count
            return counter

        return cls(strategy="simple")

    def count(self, text: str) -> int:
        if self._count_fn is not None:
            return self._count_fn(text)
        if self.strategy == "hf" and self.hf_tokenizer is not None:
            return len(self.hf_tokenizer.encode(text, add_special_tokens=False))
        return simple_token_count(text)