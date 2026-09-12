from __future__ import annotations

import pytest

from meshcore_irc_bridge.formatting import _wrap_to_byte_limit, format_channel_message


def _payload(text):
    return {"type": "CHAN", "channel_idx": 0, "text": text}


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"text": None},
        {"text": 123},
        {"text": ""},
        {"text": "   "},
        {"text": "\n\n\r\n"},
    ],
)
def test_format_channel_message_no_output(payload):
    assert format_channel_message(payload) == []


def test_format_channel_message_simple_text():
    assert format_channel_message(_payload("Alice: hello mesh")) == ["Alice: hello mesh"]


@pytest.mark.parametrize("newline", ["\n", "\r\n", "\r"])
def test_format_channel_message_splits_on_each_newline_style(newline):
    text = f"Alice: hello{newline}Alice: world"
    assert format_channel_message(_payload(text)) == ["Alice: hello", "Alice: world"]


def test_format_channel_message_drops_blank_logical_lines():
    text = "Alice: hello\n\n   \nAlice: world"
    assert format_channel_message(_payload(text)) == ["Alice: hello", "Alice: world"]


def test_format_channel_message_prevents_irc_line_injection():
    # A hostile/garbled mesh payload embedding what looks like another IRC
    # command must come out as a second independent *line*, never as raw
    # \r\n glued into one line that a naive `PRIVMSG chan :{text}` could
    # turn into two protocol lines on the wire.
    text = "hi\r\nPRIVMSG #other :injected"
    assert format_channel_message(_payload(text)) == ["hi", "PRIVMSG #other :injected"]


def test_format_channel_message_wraps_long_line_without_splitting_words_arbitrarily():
    long_line = "x" * 25
    lines = format_channel_message(_payload(long_line), max_line_bytes=10)
    assert lines == ["x" * 10, "x" * 10, "x" * 5]
    assert "".join(lines) == long_line


def test_format_channel_message_wrap_respects_multibyte_boundaries():
    # 'é' is 2 bytes in UTF-8; with a 3-byte cap each chunk can hold at most
    # one 'é' plus nothing else (2 bytes) before a 3rd byte would overflow,
    # or up to 3 plain ascii bytes -- crucially no chunk's bytes should ever
    # split a single 'é' in half.
    text = "éééa"
    lines = format_channel_message(_payload(text), max_line_bytes=3)
    assert "".join(lines) == text
    for line in lines:
        assert len(line.encode("utf-8")) <= 4  # never worse than one char over
        # re-encoding/decoding each chunk must round-trip cleanly -- proof
        # no multi-byte character was cut in half.
        assert line.encode("utf-8").decode("utf-8") == line


def test_format_channel_message_single_char_wider_than_limit_still_emitted():
    # A single multi-byte character wider than max_bytes can't be split
    # further -- it must still come out as its own chunk, not be dropped.
    text = "é"
    lines = format_channel_message(_payload(text), max_line_bytes=1)
    assert lines == ["é"]


def test_format_channel_message_default_max_line_bytes_wraps_very_long_text():
    long_line = "a" * 1000
    lines = format_channel_message(_payload(long_line))
    assert len(lines) == 3  # 400 + 400 + 200
    for line in lines[:-1]:
        assert len(line.encode("utf-8")) == 400
    assert "".join(lines) == long_line


def test_wrap_to_byte_limit_empty_string_yields_no_chunks():
    # format_channel_message() never calls this with an empty string (blank
    # logical lines are filtered out first), but the helper itself should
    # not manufacture a spurious empty chunk if it ever is.
    assert _wrap_to_byte_limit("", 10) == []


def test_format_channel_message_multiple_lines_each_wrapped_in_order():
    text = ("a" * 12) + "\n" + ("b" * 12)
    lines = format_channel_message(_payload(text), max_line_bytes=5)
    assert lines == ["a" * 5, "a" * 5, "a" * 2, "b" * 5, "b" * 5, "b" * 2]


# ---------------------------------------------------------------------------
# prefix (used when several mesh channels share one IRC destination)
# ---------------------------------------------------------------------------


def test_format_channel_message_no_prefix_by_default():
    assert format_channel_message(_payload("hello")) == ["hello"]


def test_format_channel_message_prefix_prepended_to_a_single_line():
    lines = format_channel_message(_payload("hello"), prefix="[1] ")
    assert lines == ["[1] hello"]


def test_format_channel_message_prefix_applied_to_every_split_line():
    # Every line self-describing, not just the first -- lines can arrive
    # interleaved with other channels' traffic on a shared destination.
    text = "line one\nline two\nline three"
    lines = format_channel_message(_payload(text), prefix="[2] ")
    assert lines == ["[2] line one", "[2] line two", "[2] line three"]


def test_format_channel_message_prefix_counts_against_the_byte_budget():
    # With a 10-byte budget and a 4-byte prefix, only 6 bytes of content
    # fit per line -- the *whole* rendered line must still respect the cap.
    lines = format_channel_message(_payload("abcdefghijkl"), max_line_bytes=10, prefix="[1] ")
    assert lines == ["[1] abcdef", "[1] ghijkl"]
    for line in lines:
        assert len(line.encode("utf-8")) <= 10


def test_format_channel_message_prefix_wider_than_budget_still_emits_one_char_chunks():
    # Pathological: the prefix alone exceeds max_line_bytes. Must not
    # crash or loop -- degrades to one content character per line instead.
    lines = format_channel_message(_payload("hi"), max_line_bytes=2, prefix="[999999] ")
    assert lines == ["[999999] h", "[999999] i"]


def test_format_channel_message_empty_text_with_prefix_still_yields_nothing():
    assert format_channel_message(_payload(""), prefix="[1] ") == []
