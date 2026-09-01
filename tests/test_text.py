import pytest
import regex

from telegram_mcp.text import split_text


@pytest.mark.parametrize("size,expected", [(1, 1), (4096, 1), (4097, 2), (8192, 2), (8193, 3)])
def test_chunk_count_and_round_trip(size: int, expected: int) -> None:
    text = "x" * size
    chunks = split_text(text)
    assert len(chunks) == expected
    assert "".join(chunks) == text
    assert all(0 < len(chunk) <= 4096 for chunk in chunks)


def test_prefers_a_nearby_newline_without_losing_it() -> None:
    text = "a" * 3000 + "\n" + "b" * 3000
    chunks = split_text(text)
    assert chunks[0].endswith("\n")
    assert "".join(chunks) == text


def test_plain_unicode_round_trips() -> None:
    text = ("👩‍💻é🇺🇿 текст " * 700) + "end"
    chunks = split_text(text)
    assert "".join(chunks) == text
    assert all(len(chunk) <= 4096 for chunk in chunks)
    assert all(not chunk.endswith("\u200d") for chunk in chunks[:-1])


def test_does_not_split_a_zwj_grapheme_at_a_tiny_limit() -> None:
    family = "👨‍👩‍👧‍👦"
    chunks = split_text(family + family, limit=len(family))
    assert chunks == [family, family]


def test_rejects_one_pathological_grapheme_larger_than_limit() -> None:
    with pytest.raises(ValueError, match="single Unicode grapheme"):
        split_text("a" + "\u0301" * 20, limit=10)


def test_preferred_space_with_combining_mark_is_not_split() -> None:
    text = "a" * 30 + " \u0301" + "b" * 30
    chunks = split_text(text, limit=40)
    assert "".join(chunks) == text
    assert not chunks[0].endswith(" ")

    safe_endpoints = {match.end() for match in regex.finditer(r"\X", text)}
    cumulative_end = 0
    for chunk in chunks[:-1]:
        cumulative_end += len(chunk)
        assert cumulative_end in safe_endpoints


def test_preferred_newline_boundary_remains_grapheme_safe() -> None:
    text = "a" * 30 + "\n\u0301" + "b" * 30
    chunks = split_text(text, limit=40)
    assert "".join(chunks) == text
    safe_endpoints = {match.end() for match in regex.finditer(r"\X", text)}
    cumulative_end = 0
    for chunk in chunks[:-1]:
        cumulative_end += len(chunk)
        assert cumulative_end in safe_endpoints


@pytest.mark.parametrize("delimiter", ["\n", " "])
def test_delimiter_immediately_after_hard_limit_never_creates_oversized_chunk(delimiter: str) -> None:
    text = "a" * 4096 + delimiter + "tail"
    chunks = split_text(text)
    assert "".join(chunks) == text
    assert all(0 < len(chunk) <= 4096 for chunk in chunks)


def test_empty_message_rejected() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        split_text("")
