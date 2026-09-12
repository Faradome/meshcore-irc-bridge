"""A minimal, scriptable IRC server for exercising `IRCClient` over a real
(loopback-only) asyncio socket -- real line framing, real registration
round-trips -- instead of mocking `IRCClient`'s internals away.

Usage::

    server = FakeIRCServer()
    await server.start()
    client = IRCClient(config_pointed_at(server), channels=["#chan"])
    task = asyncio.create_task(client.run_until_disconnected())
    conn = await server.accept()
    assert await conn.recv_line() == "CAP LS 302"
    ...
    await server.stop()

Note: `asyncio.Server.wait_closed()` (Python 3.13+) waits for every accepted
connection to close too, not just the listening socket -- `stop()` below
closes every connection it ever accepted before awaiting that, and wraps it
in a timeout, so a mistake here fails fast instead of hanging the suite
(see the `asyncio-server-wait-closed-hang` note).
"""

from __future__ import annotations

import asyncio


class FakeIRCConnection:
    """One accepted connection, from the server's point of view."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader = reader
        self.writer = writer

    async def recv_line(self, timeout: float = 5.0) -> str:
        """Read one raw line (without CRLF) sent by the client."""
        raw = await asyncio.wait_for(self.reader.readline(), timeout=timeout)
        if not raw:
            raise ConnectionError("client closed the connection")
        return raw.decode("utf-8").rstrip("\r\n")

    async def send_line(self, line: str) -> None:
        self.writer.write(line.encode("utf-8") + b"\r\n")
        await self.writer.drain()

    async def send_lines(self, lines: list[str]) -> None:
        for line in lines:
            await self.send_line(line)

    def close(self) -> None:
        self.writer.close()


class FakeIRCServer:
    """Accepts connections on 127.0.0.1:<ephemeral> and hands each one to
    the test as a `FakeIRCConnection` via `accept()`."""

    def __init__(self) -> None:
        self.host = "127.0.0.1"
        self.port = 0
        self._server: asyncio.base_events.Server | None = None
        self._pending: asyncio.Queue[FakeIRCConnection] = asyncio.Queue()
        self._all_connections: list[FakeIRCConnection] = []

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._on_connect, self.host, 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def _on_connect(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        conn = FakeIRCConnection(reader, writer)
        self._all_connections.append(conn)
        await self._pending.put(conn)

    async def accept(self, timeout: float = 5.0) -> FakeIRCConnection:
        """Wait for (and return) the next connection the client opens."""
        return await asyncio.wait_for(self._pending.get(), timeout=timeout)

    async def stop(self) -> None:
        for conn in self._all_connections:
            conn.close()
        if self._server is not None:
            self._server.close()
            await asyncio.wait_for(self._server.wait_closed(), timeout=5)
