from __future__ import annotations

import re
from dataclasses import dataclass

_WS = re.compile(r"\s+", re.UNICODE)


def normalize_text(text: str) -> str:
    text = text.replace("\u00A0", " ")
    text = _WS.sub(" ", text)
    return text.strip()


_SENT_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=[A-ZА-ЯЁ0-9])", re.UNICODE)


@dataclass(slots=True, frozen=True)
class SentenceSpan:
    text: str
    start: int
    end: int


def split_sentences_with_spans(text: str) -> list[SentenceSpan]:
    """Split normalized text into sentences + spans (start/end indices in the same string).

    This is intentionally simple (concept). For production-quality RU splitting consider `razdel`.
    """
    text = normalize_text(text)
    if not text:
        return []

    # Find boundary positions
    splits = [m.start() for m in _SENT_BOUNDARY.finditer(text)]
    if not splits:
        return [SentenceSpan(text=text, start=0, end=len(text))]

    spans: list[SentenceSpan] = []
    last = 0
    for pos in splits:
        part = text[last:pos].strip()
        if part:
            # Adjust start to the first non-space in slice
            leading = 0
            while last + leading < pos and text[last + leading].isspace():
                leading += 1
            trailing = 0
            while pos - trailing - 1 >= last and text[pos - trailing - 1].isspace():
                trailing += 1
            start = last + leading
            end = pos - trailing
            spans.append(SentenceSpan(text=part, start=start, end=end))
        last = pos

    tail = text[last:].strip()
    if tail:
        leading = 0
        while last + leading < len(text) and text[last + leading].isspace():
            leading += 1
        start = last + leading
        end = len(text)
        spans.append(SentenceSpan(text=tail, start=start, end=end))

    return spans
