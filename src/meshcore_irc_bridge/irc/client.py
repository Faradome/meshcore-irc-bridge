"""The IRC-side connection: registration (SASL / NickServ / unregistered),
joining channels, keepalive, and outbound PRIVMSG -- the only thing this
one-way bridge ever sends. Inbound traffic has to be read to keep the
connection framed correctly, but any `PRIVMSG` from IRC is simply dropped:
this bridge never relays anything back to the mesh.

The auth mode is fixed by config (`irc.auth.mode`) and never switched at
runtime: if it fails -- the server doesn't advertise `sasl`, the exchange
is rejected, the nickname is exhausted -- `run_until_disconnected()` raises
and the caller's own reconnect/backoff loop retries the *same* mode.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence

from ..config import IrcConfig
from .protocol import IRCConnection, Message
from .sasl import chunk_authenticate_payload, encode_plain

logger = logging.getLogger(__name__)

# Bounded retries for "nickname in use" during registration (each retry
# appends an underscore) -- prevents an infinite loop against a server that
# rejects every nick we try.
_MAX_NICK_RETRIES = 5

_RPL_WELCOME = "001"
_RPL_SASLSUCCESS = "903"
_SASL_FAILURE_NUMERICS = frozenset({"904", "905", "906"})
_NICK_IN_USE_NUMERICS = frozenset({"433", "436"})


class IRCError(Exception):
    """Base class for all `IRCClient` errors."""


class IRCAuthError(IRCError):
    """Registration or authentication failed."""


class IRCConnectionClosed(IRCError):
    """The connection closed unexpectedly (peer EOF/reset, not our own `stop()`)."""


class IRCClient:
    """One IRC connection: connect, register, join, relay, reconnectable.

    A single `IRCClient` is not itself reconnect-aware -- `run_until_disconnected()`
    runs one connection's whole lifecycle and then returns (`stop()` was
    called) or raises (anything else went wrong). `bridge.py` is the
    supervisor that calls this in a loop with backoff.
    """

    def __init__(
        self,
        config: IrcConfig,
        channels: Sequence[str],
        *,
        registration_timeout: float = 30.0,
    ) -> None:
        self._config = config
        # De-duplicate while preserving order: several mesh channels may
        # map to the same IRC channel, and we only need to JOIN it once.
        self._channels = list(dict.fromkeys(channels))
        self._registration_timeout = registration_timeout
        self._conn: IRCConnection | None = None
        self._write_lock = asyncio.Lock()
        self._ready = False
        self._stopping = False

    @property
    def is_ready(self) -> bool:
        """True once registration and channel joins have completed and the
        connection is still up -- `bridge.py` checks this before calling
        `send_privmsg`, queuing messages otherwise."""
        return self._ready

    async def run_until_disconnected(self) -> None:
        """Connect, register, join every configured channel, then read
        forever until disconnected or `stop()` is called.

        Returns normally only after a deliberate `stop()`. Raises
        `IRCAuthError`/`IRCConnectionClosed`/`OSError` on any other
        failure; the caller retries with its own backoff.
        """
        self._stopping = False
        self._conn = await IRCConnection.open(
            self._config.server, self._config.port, tls=self._config.tls
        )
        try:
            try:
                await self._run_session()
            except IRCConnectionClosed:
                # A `stop()` closes our own connection to unblock whatever
                # read is in flight, which surfaces as this same exception
                # -- checked here, in one place, rather than in every
                # phase (`_register`/`_wait_while_handling_pings`/
                # `_read_loop`) individually, so `stop()` cleanly unwinds
                # no matter which phase it interrupts.
                if not self._stopping:
                    raise
        finally:
            # `self._conn` is always set here: it's assigned before this
            # `try` (before which no exception could have led into this
            # `finally`), and nothing inside the block ever clears it early.
            self._ready = False
            conn, self._conn = self._conn, None
            assert conn is not None
            await conn.close()

    async def _run_session(self) -> None:
        try:
            await asyncio.wait_for(self._register(), timeout=self._registration_timeout)
        except TimeoutError as exc:
            raise IRCAuthError("registration timed out") from exc

        if self._config.auth.mode == "nickserv":
            nickserv_cfg = self._config.auth.nickserv
            assert nickserv_cfg is not None
            await self._send("PRIVMSG", "NickServ", trailing=f"IDENTIFY {nickserv_cfg.password}")
            await self._wait_while_handling_pings(nickserv_cfg.join_wait_seconds)
            if self._stopping:
                return

        await self._join_all_channels()
        self._ready = True
        await self._read_loop()

    async def stop(self) -> None:
        """Ask a running `run_until_disconnected()` to return cleanly."""
        self._stopping = True
        if self._conn is not None:
            await self._conn.close()

    async def send_privmsg(self, channel: str, text: str) -> None:
        """Send one line of text to `channel`. The only outbound path this
        bridge ever uses -- there is no equivalent for mesh-bound traffic."""
        if not self._ready or self._conn is None:
            raise IRCConnectionClosed("cannot send: not connected and joined")
        await self._send("PRIVMSG", channel, trailing=text)

    # -- internals -----------------------------------------------------

    async def _send(self, command: str, *params: str, trailing: str | None = None) -> None:
        assert self._conn is not None
        async with self._write_lock:
            await self._conn.send(command, *params, trailing=trailing)

    async def _next_message(self) -> Message:
        assert self._conn is not None
        msg = await self._conn.read_message()
        if msg is None:
            raise IRCConnectionClosed("connection closed by peer")
        return msg

    async def _register(self) -> None:
        nick = self._config.nickname
        await self._send("CAP", "LS", "302")
        await self._send("NICK", nick)
        await self._send("USER", self._config.username, "0", "*", trailing=self._config.realname)

        ls_caps = ""
        retries_left = _MAX_NICK_RETRIES
        sasl_completed = False

        while True:
            msg = await self._next_message()

            if msg.command == "PING":
                await self._send("PONG", trailing=msg.param(0, ""))

            elif msg.command == "ERROR":
                raise IRCAuthError(f"server closed the connection: {msg.param(0, '')}")

            elif msg.command in _NICK_IN_USE_NUMERICS:
                if retries_left <= 0:
                    raise IRCAuthError(f"nickname {nick!r} is in use and retries were exhausted")
                retries_left -= 1
                nick = f"{nick}_"
                await self._send("NICK", nick)

            elif msg.command == "CAP":
                sub = msg.param(1)
                if sub == "LS":
                    if msg.param(2) == "*":
                        ls_caps += (msg.param(3) or "") + " "
                    else:
                        ls_caps += msg.param(2) or ""
                        await self._handle_ls_complete(ls_caps)
                elif sub == "ACK" and "sasl" in (msg.param(2) or "").split():
                    await self._send("AUTHENTICATE", "PLAIN")
                elif sub == "NAK" and "sasl" in (msg.param(2) or "").split():
                    await self._send("CAP", "END")
                    raise IRCAuthError("server rejected the sasl capability request")

            elif msg.command == "AUTHENTICATE":
                if msg.param(0) == "+":
                    await self._send_sasl_response()

            elif msg.command == _RPL_SASLSUCCESS:
                sasl_completed = True
                await self._send("CAP", "END")

            elif msg.command in _SASL_FAILURE_NUMERICS:
                await self._send("CAP", "END")
                raise IRCAuthError(
                    f"SASL authentication failed ({msg.command}): {msg.param(-1, '')}"
                )

            elif msg.command == _RPL_WELCOME:
                if self._config.auth.mode == "sasl" and not sasl_completed:
                    # The server never even answered our CAP LS -- most
                    # likely it predates IRCv3 entirely. Registering
                    # unauthenticated when SASL was demanded is a silent
                    # downgrade we must not make (SASL is a first-class,
                    # non-negotiable-at-runtime requirement here).
                    raise IRCAuthError(
                        "registration completed without SASL negotiation "
                        "(server does not appear to support IRCv3 CAP)"
                    )
                return

    async def _handle_ls_complete(self, ls_caps: str) -> None:
        if self._config.auth.mode != "sasl":
            await self._send("CAP", "END")
            return
        cap_names = {token.split("=", 1)[0] for token in ls_caps.split()}
        if "sasl" not in cap_names:
            await self._send("CAP", "END")
            raise IRCAuthError("server does not advertise the sasl capability")
        await self._send("CAP", "REQ", trailing="sasl")

    async def _send_sasl_response(self) -> None:
        sasl_cfg = self._config.auth.sasl
        assert sasl_cfg is not None
        blob = encode_plain("", sasl_cfg.username, sasl_cfg.password)
        for token in chunk_authenticate_payload(blob):
            await self._send("AUTHENTICATE", token)

    async def _wait_while_handling_pings(self, seconds: float) -> None:
        """Block for `seconds`, answering any `PING` that arrives meanwhile
        instead of blind-sleeping through it (a long NickServ join-wait
        must not trip the server's own ping timeout).

        A `stop()` during this wait surfaces as `_next_message()` raising
        `IRCConnectionClosed` (its `conn.close()` unblocks our read) --
        left to propagate to `run_until_disconnected()`'s single
        stopping-aware catch rather than handled here too.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + seconds
        while not self._stopping:
            remaining = deadline - loop.time()
            try:
                # A `remaining` of zero or negative is not special-cased
                # here: `wait_for` itself treats that as "already expired"
                # and raises `TimeoutError` immediately, which the except
                # clause below turns into a plain return.
                msg = await asyncio.wait_for(self._next_message(), timeout=remaining)
            except TimeoutError:
                return
            if msg.command == "PING":
                await self._send("PONG", trailing=msg.param(0, ""))

    async def _join_all_channels(self) -> None:
        for channel in self._channels:
            await self._send("JOIN", channel)

    async def _read_loop(self) -> None:
        while True:
            msg = await self._next_message()
            if msg.command == "PING":
                await self._send("PONG", trailing=msg.param(0, ""))
            # Anything else (PRIVMSG, NOTICE, ...) from IRC: one-way
            # bridge, intentionally dropped.
