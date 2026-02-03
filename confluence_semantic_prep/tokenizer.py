from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_WORD_OR_PUNCT = re.compile(r"\w+|[^\w\s]", re.UNICODE)


def simple_token_count(text: str) -> int:
    return len(_WORD_OR_PUNCT.findall(text))


@dataclass(slots=True)
class TokenCounter:
    strategy: str = "simple"
    hf_tokenizer: object | None = None

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
            return cls(strategy="hf", hf_tokenizer=tok)

        return cls(strategy="simple")

    def count(self, text: str) -> int:
        if self.strategy == "hf" and self.hf_tokenizer is not None:
            return len(self.hf_tokenizer.encode(text, add_special_tokens=False))
        return simple_token_count(text)
