from __future__ import annotations

import base64

from meshcore_irc_bridge.irc.sasl import chunk_authenticate_payload, encode_plain


def test_encode_plain_known_vector():
    # base64("\0alice\0hunter2")
    expected = base64.b64encode(b"\0alice\0hunter2").decode("ascii")
    assert encode_plain("", "alice", "hunter2") == expected


def test_encode_plain_with_authzid():
    expected = base64.b64encode(b"admin\0alice\0hunter2").decode("ascii")
    assert encode_plain("admin", "alice", "hunter2") == expected


def test_encode_plain_roundtrips_unicode_password():
    encoded = encode_plain("", "alice", "pâsswörd™")
    decoded = base64.b64decode(encoded).decode("utf-8")
    assert decoded == "\0alice\0pâsswörd™"


def test_chunk_authenticate_payload_empty_is_single_plus():
    assert chunk_authenticate_payload("") == ["+"]


def test_chunk_authenticate_payload_short_payload_single_chunk():
    assert chunk_authenticate_payload("YWJj") == ["YWJj"]


def test_chunk_authenticate_payload_exact_multiple_gets_trailing_plus():
    payload = "a" * 400
    assert chunk_authenticate_payload(payload, chunk_size=400) == [payload, "+"]


def test_chunk_authenticate_payload_non_multiple_no_trailing_plus():
    payload = "a" * 450
    chunks = chunk_authenticate_payload(payload, chunk_size=400)
    assert chunks == ["a" * 400, "a" * 50]


def test_chunk_authenticate_payload_multiple_full_chunks_plus_trailing_plus():
    payload = "a" * 800
    chunks = chunk_authenticate_payload(payload, chunk_size=400)
    assert chunks == ["a" * 400, "a" * 400, "+"]
