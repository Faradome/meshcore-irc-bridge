from __future__ import annotations

import asyncio

import pytest

from meshcore_irc_bridge.irc.protocol import IRCConnection, Message, format_line

# ---------------------------------------------------------------------------
# Message.parse
# ---------------------------------------------------------------------------


def test_parse_command_only():
    msg = Message.parse("PING")
    assert msg.command == "PING"
    assert msg.params == []
    assert msg.prefix is None
    assert msg.tags == {}


def test_parse_command_lowercase_is_normalized():
    assert Message.parse("ping").command == "PING"


def test_parse_params_no_trailing():
    msg = Message.parse("JOIN #general")
    assert msg.command == "JOIN"
    assert msg.params == ["#general"]


def test_parse_trailing_only():
    msg = Message.parse("PING :12345")
    assert msg.command == "PING"
    assert msg.params == ["12345"]


def test_parse_params_and_trailing():
    msg = Message.parse("PRIVMSG #chan :hello world")
    assert msg.command == "PRIVMSG"
    assert msg.params == ["#chan", "hello world"]


def test_parse_trailing_may_contain_colons():
    msg = Message.parse("PRIVMSG #chan :hi :there")
    assert msg.params == ["#chan", "hi :there"]


def test_parse_with_prefix():
    msg = Message.parse(":irc.example.org 001 mynick :Welcome to the network")
    assert msg.prefix == "irc.example.org"
    assert msg.command == "001"
    assert msg.params == ["mynick", "Welcome to the network"]


def test_parse_with_tags_and_prefix():
    line = "@time=2021-01-01T00:00:00Z;msgid=abc123 :nick!user@host PRIVMSG #chan :hi"
    msg = Message.parse(line)
    assert msg.tags == {"time": "2021-01-01T00:00:00Z", "msgid": "abc123"}
    assert msg.prefix == "nick!user@host"
    assert msg.command == "PRIVMSG"
    assert msg.params == ["#chan", "hi"]


def test_parse_tag_without_value():
    msg = Message.parse("@vendor/flag :prefix COMMAND arg")
    assert msg.tags == {"vendor/flag": ""}


def test_parse_tags_skips_empty_items_from_stray_semicolons():
    msg = Message.parse("@foo=bar;;baz=qux :prefix COMMAND")
    assert msg.tags == {"foo": "bar", "baz": "qux"}


def test_parse_strips_crlf():
    msg = Message.parse("PING :123\r\n")
    assert msg.params == ["123"]


def test_parse_empty_line_raises():
    with pytest.raises(ValueError, match="empty IRC line"):
        Message.parse("")


def test_parse_prefix_only_raises():
    with pytest.raises(ValueError, match="missing a command"):
        Message.parse(":onlyprefix")


def test_message_param_helper_in_range_and_default():
    msg = Message.parse("JOIN #general")
    assert msg.param(0) == "#general"
    assert msg.param(1) is None
    assert msg.param(1, "fallback") == "fallback"


# ---------------------------------------------------------------------------
# format_line
# ---------------------------------------------------------------------------


def test_format_line_simple():
    assert format_line("NICK", "bob") == "NICK bob"


def test_format_line_with_trailing():
    assert format_line("PRIVMSG", "#chan", trailing="hello world") == "PRIVMSG #chan :hello world"


def test_format_line_trailing_only():
    assert format_line("PING", trailing="12345") == "PING :12345"


def test_format_line_no_params_no_trailing():
    assert format_line("CAP") == "CAP"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"params": ("bad\r\nnick",)},
        {"params": (), "trailing": "bad\r\ntrailing"},
    ],
)
def test_format_line_rejects_crlf_injection(kwargs):
    params = kwargs.get("params", ())
    trailing = kwargs.get("trailing")
    with pytest.raises(ValueError, match="must not contain CR or LF"):
        format_line("PRIVMSG", *params, trailing=trailing)


def test_format_line_rejects_crlf_in_command():
    with pytest.raises(ValueError, match="must not contain CR or LF"):
        format_line("PRIVMSG\r\nQUIT")


def test_format_line_rejects_empty_param():
    with pytest.raises(ValueError, match="invalid as a non-trailing parameter"):
        format_line("JOIN", "")


def test_format_line_rejects_param_starting_with_colon():
    with pytest.raises(ValueError, match="invalid as a non-trailing parameter"):
        format_line("JOIN", ":notallowed")


def test_format_line_rejects_param_with_space():
    with pytest.raises(ValueError, match="invalid as a non-trailing parameter"):
        format_line("JOIN", "has space")


def test_format_line_rejects_line_too_long():
    with pytest.raises(ValueError, match="too long"):
        format_line("PRIVMSG", "#chan", trailing="x" * 600)


# ---------------------------------------------------------------------------
# IRCConnection, over a real loopback socket
# ---------------------------------------------------------------------------


class _ServerSide:
    def __init__(self) -> None:
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.connected: asyncio.Event = asyncio.Event()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer
        self.connected.set()


@pytest.fixture
async def loopback():
    """A real (loopback-only) TCP server + an `IRCConnection` connected to it."""
    server_side = _ServerSide()
    server = await asyncio.start_server(server_side.handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]

    client_reader, client_writer = await asyncio.open_connection(host, port)
    client = IRCConnection(client_reader, client_writer)
    await asyncio.wait_for(server_side.connected.wait(), timeout=5)

    try:
        yield client, server_side
    finally:
        await client.close()
        if server_side.writer is not None:
            server_side.writer.close()
        server.close()
        await asyncio.wait_for(server.wait_closed(), timeout=5)


async def test_irc_connection_open_connects_over_plain_tcp():
    server_side = _ServerSide()
    server = await asyncio.start_server(server_side.handle, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()[:2]
    try:
        client = await IRCConnection.open(host, port, tls=False)
        await asyncio.wait_for(server_side.connected.wait(), timeout=5)
        await client.close()
    finally:
        # asyncio.Server.wait_closed() waits for every accepted connection
        # to close too (not just the listening socket) -- the server side
        # of the accepted connection must be closed explicitly, or this
        # hangs forever.
        if server_side.writer is not None:
            server_side.writer.close()
        server.close()
        await asyncio.wait_for(server.wait_closed(), timeout=5)


async def test_send_writes_a_correctly_framed_line(loopback):
    client, server_side = loopback
    await client.send("PRIVMSG", "#chan", trailing="hello mesh")
    raw = await server_side.reader.readline()
    assert raw == b"PRIVMSG #chan :hello mesh\r\n"


async def test_read_message_parses_a_line_from_the_server(loopback):
    client, server_side = loopback
    server_side.writer.write(b":irc.example.org 001 bot :Welcome\r\n")
    await server_side.writer.drain()
    msg = await client.read_message()
    assert msg is not None
    assert msg.command == "001"
    assert msg.params == ["bot", "Welcome"]


async def test_read_message_skips_blank_lines(loopback):
    client, server_side = loopback
    server_side.writer.write(b"\r\nPING :1\r\n")
    await server_side.writer.drain()
    msg = await client.read_message()
    assert msg is not None
    assert msg.command == "PING"
    assert msg.params == ["1"]


async def test_read_message_returns_none_on_clean_eof(loopback):
    client, server_side = loopback
    server_side.writer.close()
    await server_side.writer.wait_closed()
    msg = await client.read_message()
    assert msg is None


async def test_read_message_parses_final_unterminated_line_before_eof(loopback):
    client, server_side = loopback
    server_side.writer.write(b"PING :123")  # no trailing \n
    await server_side.writer.drain()
    server_side.writer.close()
    await server_side.writer.wait_closed()
    msg = await client.read_message()
    assert msg is not None
    assert msg.command == "PING"
    assert msg.params == ["123"]


async def test_close_is_safe_to_call_twice(loopback):
    client, _server_side = loopback
    await client.close()
    await client.close()
