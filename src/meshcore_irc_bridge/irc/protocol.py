"""Line framing and message parsing/formatting for the IRC wire protocol.

`Message.parse`/`format_line` are pure functions, fully unit tested on their
own. `IRCConnection` wraps an `(asyncio.StreamReader, asyncio.StreamWriter)`
pair; its constructor takes an already-open pair so tests can hand it a real
loopback socket (see `tests/fakes/fake_irc_server.py`) without touching a
real remote server. The one thing that can't be exercised without a live
TLS-terminated IRC server -- creating the TLS context in `open()` -- is
narrowly excluded from coverage; the plain-TCP path through `open()` (the
common case for a local/private IRC network, and what the loopback tests
use) is fully covered.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket
import ssl
from dataclasses import dataclass, field

# 512 bytes total per IRC line, including the trailing CRLF (RFC 2812 2.3).
MAX_LINE_BYTES = 512


@dataclass
class Message:
    """One parsed IRC line: `[@tags ][:prefix ]COMMAND[ params...][ :trailing]`."""

    command: str
    params: list[str] = field(default_factory=list)
    prefix: str | None = None
    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def parse(cls, line: str) -> Message:
        line = line.rstrip("\r\n")
        if not line:
            raise ValueError("cannot parse an empty IRC line")

        tags: dict[str, str] = {}
        if line.startswith("@"):
            tag_part, _, remainder = line.partition(" ")
            tags = _parse_tags(tag_part[1:])
            line = remainder.lstrip(" ")

        prefix = None
        if line.startswith(":"):
            prefix_part, _, remainder = line.partition(" ")
            prefix = prefix_part[1:]
            line = remainder.lstrip(" ")

        if " :" in line:
            head, _, trailing = line.partition(" :")
            params = [p for p in head.split(" ") if p]
            params.append(trailing)
        else:
            params = [p for p in line.split(" ") if p]

        if not params:
            raise ValueError(f"IRC message is missing a command: {line!r}")

        command, *rest = params
        return cls(tags=tags, prefix=prefix, command=command.upper(), params=rest)

    def param(self, index: int, default: str | None = None) -> str | None:
        """`params[index]` or `default` if there aren't that many params."""
        return self.params[index] if index < len(self.params) else default


def _parse_tags(tag_str: str) -> dict[str, str]:
    """Parse an IRCv3 `@key=value;key2=value2` tag prefix.

    Values are kept raw (no `\\:`/`\\s` unescaping) -- nothing in this
    bridge reads tag *values*, only whether a line is tagged at all, so
    there is no behaviour to lose by not implementing the full escape
    grammar for a feature this client never requests.
    """
    tags: dict[str, str] = {}
    for item in tag_str.split(";"):
        if not item:
            continue
        key, _, value = item.partition("=")
        tags[key] = value
    return tags


def format_line(command: str, *params: str, trailing: str | None = None) -> str:
    """Build one raw IRC line (no CRLF) from a command, params, and an
    optional trailing (last, space-and-colon-prefixed) parameter.

    Raises `ValueError` rather than silently emitting a corrupt line for
    anything that would desync the protocol: CR/LF embedded in any
    component (line-injection), a non-trailing parameter containing a
    space or leading with `:`, or a line over IRC's 512-byte limit.
    """
    all_components = [command, *params, *([trailing] if trailing is not None else [])]
    for component in all_components:
        if "\r" in component or "\n" in component:
            raise ValueError(f"IRC message component {component!r} must not contain CR or LF")

    for p in params:
        if not p or p.startswith(":") or " " in p:
            raise ValueError(
                f"IRC parameter {p!r} is invalid as a non-trailing parameter "
                "(empty, starting with ':', and containing a space are all "
                "only valid for the trailing parameter)"
            )

    parts = [command, *params]
    if trailing is not None:
        parts.append(f":{trailing}")
    line = " ".join(parts)

    if len(line.encode("utf-8")) + 2 > MAX_LINE_BYTES:  # +2 for the CRLF this line will get
        raise ValueError(f"IRC line too long ({len(line.encode('utf-8'))} bytes): {line!r}")
    return line


class IRCConnection:
    """A framed line reader/writer over an open IRC socket."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._enable_tcp_keepalive()

    def _enable_tcp_keepalive(self) -> None:
        """Best-effort belt-and-braces beneath `IRCClient`'s own
        application-level PING watchdog: ask the OS to probe an idle
        connection too. Not load-bearing on its own -- `SO_KEEPALIVE` is
        off by default and, even enabled, most platforms' default probe
        interval is hours -- but harmless to set, and free insurance on a
        host tuned more aggressively. Silently a no-op if the writer
        doesn't expose a real socket, or the platform doesn't support the
        option.
        """
        sock = self._writer.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    @classmethod
    async def open(cls, host: str, port: int, *, tls: bool) -> IRCConnection:
        ssl_context = ssl.create_default_context() if tls else None  # pragma: no cover
        reader, writer = await asyncio.open_connection(host, port, ssl=ssl_context)
        return cls(reader, writer)

    async def read_message(self) -> Message | None:
        """Read and parse the next non-blank line, or `None` if the
        connection is gone.

        That covers both a clean EOF and any transport-level error
        (`OSError`/`ssl.SSLError` and subclasses) -- notably, closing the
        connection from a *different* task (as `stop()` does, concurrently
        with this read) surfaces here as `ssl.SSLError:
        APPLICATION_DATA_AFTER_CLOSE_NOTIFY` on a real TLS connection, not
        a clean EOF; a plain reset surfaces as `ConnectionResetError`. Both
        mean the same thing to a caller: this connection is over.
        """
        while True:
            try:
                raw = await self._reader.readuntil(b"\n")
            except asyncio.IncompleteReadError as exc:
                if not exc.partial:
                    return None
                raw = exc.partial
            except OSError:
                return None

            line = raw.decode("utf-8", errors="replace")
            if line.strip("\r\n"):
                return Message.parse(line)

    async def send(self, command: str, *params: str, trailing: str | None = None) -> None:
        line = format_line(command, *params, trailing=trailing)
        self._writer.write(line.encode("utf-8") + b"\r\n")
        await self._writer.drain()

    async def close(self) -> None:
        self._writer.close()
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()
