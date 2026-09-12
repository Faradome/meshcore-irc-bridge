"""IRCv3 SASL PLAIN mechanism encoding (RFC 4616 / ircv3.net/specs/extensions/sasl-3.1).

Pure functions -- no socket I/O -- so the SASL wire format itself is
trivially and exhaustively unit tested, independent of `irc/client.py`'s
state machine.
"""

from __future__ import annotations

import base64

# Per the IRCv3 SASL spec, each `AUTHENTICATE` line carries at most this
# many bytes of base64; a longer payload is split across several lines.
_AUTHENTICATE_CHUNK_SIZE = 400


def encode_plain(authzid: str, authcid: str, password: str) -> str:
    """Encode a SASL PLAIN response: base64 of `authzid\\0authcid\\0password`.

    `authzid` (the authorization identity) is conventionally left empty
    (`""`) for IRC SASL PLAIN -- the server infers it from `authcid`.
    """
    raw = f"{authzid}\0{authcid}\0{password}".encode()
    return base64.b64encode(raw).decode("ascii")


def chunk_authenticate_payload(
    payload_b64: str, chunk_size: int = _AUTHENTICATE_CHUNK_SIZE
) -> list[str]:
    """Split a base64 SASL payload into the literal `AUTHENTICATE` tokens to send.

    Per the spec: at most `chunk_size` bytes of base64 per line, and if the
    payload's total length is an exact multiple of `chunk_size` (including
    zero, i.e. an empty payload), an extra `"+"` token is appended so the
    server knows the response is complete. `"+"` is also the wire
    representation of an empty chunk (IRC can't send a truly empty
    trailing parameter unambiguously).
    """
    if not payload_b64:
        return ["+"]
    tokens = [
        payload_b64[i : i + chunk_size] for i in range(0, len(payload_b64), chunk_size)
    ]
    if len(payload_b64) % chunk_size == 0:
        tokens.append("+")
    return tokens
