"""Turn a `CHANNEL_MSG_RECV` payload into one or more safe IRC PRIVMSG lines.

Pure functions -- no I/O, no asyncio -- so this is trivial to get to 100%
coverage and to reason about independently of the mesh/IRC connections.

The `meshcore` `CHANNEL_MSG_RECV` payload carries no sender-name field (mesh
apps conventionally embed "Name: message" in `text` itself), so this module
relays `text` through mostly as-is -- its job is safety (no raw newlines
forwarded into the IRC protocol stream) and IRC's line-length limit, not
adding formatting the payload doesn't have the data for.
"""

from __future__ import annotations

import re
from typing import Any

# Conservative cap on one PRIVMSG's text. IRC's hard limit is 512 bytes for
# the *whole* line as the server relays it -- "PRIVMSG #channel :" plus the
# trailing CRLF, plus a ":nick!user@host " prefix the server prepends before
# relaying to other clients, none of which we control the length of -- so
# this leaves generous headroom rather than trying to compute it exactly
# per destination.
MAX_LINE_BYTES = 400

_LINE_SPLIT = re.compile(r"\r\n|\r|\n")


def _wrap_to_byte_limit(text: str, max_bytes: int) -> list[str]:
    """Split `text` into chunks whose UTF-8 encoding is at most `max_bytes`.

    Splits only between characters, never inside one -- a multi-byte UTF-8
    character is always kept whole, even if that makes one chunk exceed
    `max_bytes` (unavoidable for a single character wider than the limit).
    """
    chunks: list[str] = []
    current: list[str] = []
    current_bytes = 0
    for ch in text:
        ch_bytes = len(ch.encode("utf-8"))
        if current and current_bytes + ch_bytes > max_bytes:
            chunks.append("".join(current))
            current = []
            current_bytes = 0
        current.append(ch)
        current_bytes += ch_bytes
    if current:
        chunks.append("".join(current))
    return chunks


def format_channel_message(
    payload: dict[str, Any], *, max_line_bytes: int = MAX_LINE_BYTES
) -> list[str]:
    """Convert a `CHANNEL_MSG_RECV` event payload into IRC-safe PRIVMSG lines.

    - Any `\\r`/`\\n` embedded in the mesh text becomes separate output
      lines instead of being forwarded raw: a raw newline inside a single
      PRIVMSG line would let mesh content forge additional IRC protocol
      lines (command injection over the wire).
    - Each logical line is wrapped at `max_line_bytes` (UTF-8 bytes, never
      splitting a multi-byte character) rather than silently truncated.
    - A missing/non-string/empty `text`, or one that is blank once split,
      produces no output lines at all.
    """
    text = payload.get("text")
    if not isinstance(text, str) or not text:
        return []

    lines: list[str] = []
    for logical_line in _LINE_SPLIT.split(text):
        if not logical_line.strip():
            continue
        lines.extend(_wrap_to_byte_limit(logical_line, max_line_bytes))
    return lines
