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

# C0 control characters other than tab, which is left alone as harmless
# formatting. CR/LF are handled separately (split into their own output
# lines, see _LINE_SPLIT) rather than here -- everything else in this
# range (NUL, ESC, ...) has no legitimate place in relayed chat text and
# is simply mesh-side noise once it reaches an IRC client, so it's
# stripped rather than forwarded unescaped into PRIVMSG content.
_CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


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
    payload: dict[str, Any], *, max_line_bytes: int = MAX_LINE_BYTES, prefix: str = ""
) -> list[str]:
    """Convert a `CHANNEL_MSG_RECV` event payload into IRC-safe PRIVMSG lines.

    - Any `\\r`/`\\n` embedded in the mesh text becomes separate output
      lines instead of being forwarded raw: a raw newline inside a single
      PRIVMSG line would let mesh content forge additional IRC protocol
      lines (command injection over the wire).
    - Every other C0 control character (NUL, ESC, ...) is stripped -- mesh
      payloads are untrusted input, and none of them have a legitimate
      place in chat text relayed to an IRC client.
    - Each logical line is wrapped at `max_line_bytes` (UTF-8 bytes, never
      splitting a multi-byte character) rather than silently truncated.
    - A missing/non-string/empty `text`, or one that is blank once split,
      produces no output lines at all.
    - `prefix` (e.g. `"[1] "`, the mesh channel index) is prepended to
      *every* output line, not just the first -- callers pass this when
      several mesh channels share one IRC destination, so each line is
      still self-describing on its own rather than only the first of a
      multi-line message. It counts against `max_line_bytes`, so the
      *content* wraps shorter rather than the whole line running over.
    """
    text = payload.get("text")
    if not isinstance(text, str) or not text:
        return []

    # A floor of 1 keeps _wrap_to_byte_limit well-defined even for a
    # pathological prefix at or beyond the line budget on its own -- it
    # degrades to one character per chunk rather than looping or crashing.
    available = max(max_line_bytes - len(prefix.encode("utf-8")), 1)

    lines: list[str] = []
    for raw_line in _LINE_SPLIT.split(text):
        logical_line = _CONTROL_CHARS.sub("", raw_line)
        if not logical_line.strip():
            continue
        lines.extend(f"{prefix}{chunk}" for chunk in _wrap_to_byte_limit(logical_line, available))
    return lines
