"""Telegram-safe plain-text splitting."""

from __future__ import annotations

from bisect import bisect_right

import regex  # type: ignore[import-untyped]


def split_text(text: str, limit: int = 4096) -> list[str]:
    """Split without loss or broken graphemes, preferring newlines and spaces."""
    if not text:
        raise ValueError("Message text must not be empty")
    if limit < 1:
        raise ValueError("limit must be positive")
    grapheme_ends = [match.end() for match in regex.finditer(r"\X", text)]
    previous = 0
    for end in grapheme_ends:
        if end - previous > limit:
            raise ValueError("A single Unicode grapheme exceeds Telegram's message limit")
        previous = end

    chunks: list[str] = []
    safe_endpoints = set(grapheme_ends)
    start = 0
    while len(text) - start > limit:
        endpoint_index = bisect_right(grapheme_ends, start + limit) - 1
        if endpoint_index < 0 or grapheme_ends[endpoint_index] <= start:
            raise ValueError("No safe Unicode split boundary within Telegram's message limit")
        hard_end = grapheme_ends[endpoint_index]
        window = text[start:hard_end]
        end = hard_end
        for delimiter in ("\n", " "):
            search_to = len(window)
            while search_to > limit // 2:
                preferred = window.rfind(delimiter, 0, search_to)
                if preferred < limit // 2:
                    break
                candidate = start + preferred + 1
                if candidate in safe_endpoints:
                    end = candidate
                    break
                search_to = preferred
            if end != hard_end:
                break
        chunks.append(text[start:end])
        start = end
    chunks.append(text[start:])
    return chunks
