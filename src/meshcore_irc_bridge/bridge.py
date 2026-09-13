"""Orchestrates the one-way mesh -> IRC relay.

Connects to both sides, subscribes to `EventType.CHANNEL_MSG_RECV` on the
mesh, and runs each side under its own supervised reconnect-with-backoff
loop so an outage on one side never takes down the other. A small bounded
queue sits between the two: mesh events are cheap/synchronous to enqueue,
while a separate task drains the queue into IRC only once the IRC side is
connected and has joined its channels -- so a message that arrives while
IRC is reconnecting is held (up to a limit) rather than dropped outright or
blocking the mesh event handler.

`mesh_connect` and `irc_client_factory` are injectable, mirroring
`mesh_connection.py`'s/`connect.py`'s own seam, so tests wire in a fake
`meshcore.MeshCore` double and a fake IRC client instead of touching real
hardware or a real IRC network -- see `tests/fakes/meshcore_double.py` and
`tests/unit/test_bridge.py`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

from meshcore import EventType

from . import mesh_connection
from .config import BridgeConfig, IrcConfig, MeshConfig
from .formatting import format_channel_message
from .irc.client import IRCClient
from .mesh_connection import MeshConnectError, MeshCoreConnection

logger = logging.getLogger(__name__)

# Bounded queue of (irc_channel, text) lines waiting for the IRC side to be
# ready -- drop the oldest rather than growing unboundedly or blocking the
# mesh event handler while IRC reconnects.
_MAX_QUEUED_LINES = 200

# The queue-drain loop is a simple poll rather than a condition variable:
# this bridge relays occasional chat messages, not a high-throughput
# stream, so a sub-second poll is both simple and plenty responsive.
_QUEUE_POLL_IDLE_SECONDS = 0.1
_QUEUE_POLL_NOT_READY_SECONDS = 0.2

# Give up on (and drop) a message that fails to send this many times in a
# row, rather than retrying it forever: the drain loop only ever works the
# head of the queue, so an item that can never succeed would otherwise
# block every other channel's queued messages behind it indefinitely.
# config.py's channel-name validation closes the only known way to hit
# this at config time, but an unknown trigger (a persistent transport
# error) must not wedge the queue permanently.
_MAX_SEND_ATTEMPTS = 5


class Bridge:
    """Owns both connections for the lifetime of the process."""

    def __init__(
        self,
        config: BridgeConfig,
        *,
        mesh_connect: Callable[
            [MeshConfig], Awaitable[MeshCoreConnection]
        ] = mesh_connection.connect,
        irc_client_factory: Callable[[IrcConfig, Sequence[str]], IRCClient] | None = None,
        mesh_reconnect_initial_delay: float = 1.0,
        mesh_reconnect_max_delay: float = 60.0,
    ) -> None:
        self._config = config
        self._mesh_connect = mesh_connect
        self._irc_client_factory = irc_client_factory or self._default_irc_client_factory
        self._mesh_reconnect_initial_delay = mesh_reconnect_initial_delay
        self._mesh_reconnect_max_delay = mesh_reconnect_max_delay

        self._queue: deque[tuple[str, str]] = deque(maxlen=_MAX_QUEUED_LINES)
        self._irc_client: IRCClient | None = None
        self._mesh_client: MeshCoreConnection | None = None
        self._mesh_connect_task: asyncio.Task[MeshCoreConnection] | None = None
        self._stopping = False

        # IRC channels fed by more than one mesh channel: messages routed
        # there get a "[<mesh_channel>] " prefix (see _on_channel_message)
        # so they stay attributable once several mesh channels' traffic is
        # interleaved in one place. Computed once from the static config,
        # not from observed traffic -- two mesh channels mapped to the
        # same IRC channel are always potentially ambiguous, even before
        # both have actually sent anything in a given run.
        counts: dict[str, int] = {}
        for mapping in config.channels:
            counts[mapping.irc_channel] = counts.get(mapping.irc_channel, 0) + 1
        self._shared_irc_channels = frozenset(
            channel for channel, count in counts.items() if count > 1
        )

    @staticmethod
    def _default_irc_client_factory(irc_config: IrcConfig, channels: Sequence[str]) -> IRCClient:
        return IRCClient(irc_config, channels)

    @property
    def distinct_irc_channels(self) -> list[str]:
        """Every IRC channel this bridge needs to join, de-duplicated
        (several mesh channels may map to the same IRC channel)."""
        return list(dict.fromkeys(m.irc_channel for m in self._config.channels))

    async def run(self) -> None:
        """Run both sides until `stop()` is called.

        Each side supervises its own reconnect loop; this only returns
        once both have wound down after `stop()`.
        """
        self._irc_client = self._irc_client_factory(self._config.irc, self.distinct_irc_channels)
        tasks = [
            asyncio.create_task(self._run_mesh_supervisor()),
            asyncio.create_task(self._run_irc_supervisor()),
            asyncio.create_task(self._run_queue_drain()),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Ask a running `run()` to wind down cleanly.

        Best-effort on both sides: a failure stopping one side must not
        skip stopping the other, and must not raise out of `stop()`
        itself -- `_run_mesh_supervisor`'s own wait is polled (not solely
        dependent on `disconnect()` succeeding) precisely so a broken
        `disconnect()` here can't wedge it.
        """
        self._stopping = True
        # A connect in progress has no `self._mesh_client` yet for the
        # branch below to act on -- `create_tcp`/`create_serial`/`create_ble`
        # have no timeout of their own (unbounded TCP retries, BLE scanning),
        # so without this a `stop()` landing during connect would have
        # nothing to interrupt and would only take effect once (and if) that
        # connect eventually finished on its own.
        if self._mesh_connect_task is not None and not self._mesh_connect_task.done():
            self._mesh_connect_task.cancel()
        if self._irc_client is not None:
            with contextlib.suppress(Exception):
                await self._irc_client.stop()
        if self._mesh_client is not None:
            with contextlib.suppress(Exception):
                await self._mesh_client.disconnect()

    async def _interruptible_sleep(self, duration: float) -> None:
        """Like `asyncio.sleep(duration)`, but returns early once `stop()`
        sets `_stopping` -- a bare `sleep()` here would make `stop()` block
        for up to a full backoff delay (`reconnect.max_delay_seconds`,
        potentially many seconds) instead of returning promptly.
        """
        step = 0.1
        elapsed = 0.0
        while elapsed < duration and not self._stopping:
            this_step = min(step, duration - elapsed)
            await asyncio.sleep(this_step)
            elapsed += this_step

    # -- mesh side -------------------------------------------------------

    async def _run_mesh_supervisor(self) -> None:
        delay = self._mesh_reconnect_initial_delay
        while not self._stopping:
            self._mesh_connect_task = asyncio.ensure_future(self._mesh_connect(self._config.mesh))
            try:
                client = await self._mesh_connect_task
            except asyncio.CancelledError:
                if self._stopping:
                    return
                raise
            except MeshConnectError:
                logger.exception("failed to connect to the mesh radio")
                if self._stopping:
                    return
                await self._interruptible_sleep(delay)
                delay = min(delay * 2, self._mesh_reconnect_max_delay)
                continue
            finally:
                self._mesh_connect_task = None

            self._mesh_client = client
            connected_at = asyncio.get_running_loop().time()
            needs_reconnect: asyncio.Event = asyncio.Event()
            try:
                try:
                    client.subscribe(
                        EventType.DISCONNECTED, lambda _e, ev=needs_reconnect: ev.set()
                    )
                    client.subscribe(EventType.CHANNEL_MSG_RECV, self._on_channel_message)
                    await self._log_channel_mappings(client)
                    await client.start_auto_message_fetching()
                    # Polled rather than a bare `await needs_reconnect.wait()`:
                    # `stop()`'s own `client.disconnect()` call is what would
                    # otherwise need to be relied on to emit the DISCONNECTED
                    # event that unblocks this -- but if that call itself
                    # raises (a flaky/half-broken transport), the event never
                    # fires and this would wait forever. Checking `_stopping`
                    # directly every 100ms needs no such cooperation.
                    while not self._stopping and not needs_reconnect.is_set():
                        with contextlib.suppress(TimeoutError):
                            await asyncio.wait_for(needs_reconnect.wait(), timeout=0.1)
                except Exception:
                    # A transient error anywhere in an otherwise-connected
                    # session (e.g. a BLE write failure the library doesn't
                    # convert to a DISCONNECTED event) must not propagate out
                    # of this supervisor -- that would kill `_run_irc_supervisor`
                    # and the queue-drain task right along with it via
                    # `run()`'s `asyncio.gather`, taking down a perfectly
                    # healthy IRC side over a one-off mesh hiccup. Treat it
                    # the same as an ordinary disconnect instead, matching
                    # `_run_irc_supervisor`'s own except-Exception around
                    # `run_until_disconnected()`.
                    logger.exception("mesh session error")
            finally:
                self._mesh_client = None
                with contextlib.suppress(Exception):
                    await client.disconnect()

            if self._stopping:
                return

            session_duration = asyncio.get_running_loop().time() - connected_at
            if session_duration >= self._mesh_reconnect_max_delay:
                delay = self._mesh_reconnect_initial_delay
            logger.warning("mesh connection lost; reconnecting in %.1fs", delay)
            await self._interruptible_sleep(delay)
            delay = min(delay * 2, self._mesh_reconnect_max_delay)

    async def _log_channel_mappings(self, client: MeshCoreConnection) -> None:
        """Read-only sanity check: log what's actually configured on the
        radio for each mapped channel index. Never writes to the radio --
        use `meshcorectl create channel` to configure one."""
        for mapping in self._config.channels:
            try:
                result = await client.commands.get_channel(mapping.mesh_channel)
            except Exception:
                logger.exception(
                    "failed to query mesh channel %d from the radio", mapping.mesh_channel
                )
                continue
            if result is None or result.is_error():
                logger.warning(
                    "mesh channel %d (-> IRC %s) is not configured on the radio",
                    mapping.mesh_channel,
                    mapping.irc_channel,
                )
                continue
            channel_name = result.payload.get("channel_name", "")
            logger.info(
                "mesh channel %d (%r) -> IRC %s",
                mapping.mesh_channel,
                channel_name,
                mapping.irc_channel,
            )

    def _on_channel_message(self, event: Any) -> None:
        channel_idx = event.payload.get("channel_idx")
        irc_channel = (
            self._config.irc_channel_for(channel_idx) if channel_idx is not None else None
        )
        if irc_channel is None:
            logger.debug("dropping channel message for unmapped mesh channel %r", channel_idx)
            return
        # Only prefix when this IRC channel is actually shared -- the
        # common case (one mesh channel, one IRC channel) stays unadorned.
        prefix = f"[{channel_idx}] " if irc_channel in self._shared_irc_channels else ""
        for line in format_channel_message(event.payload, prefix=prefix):
            self._enqueue(irc_channel, line)

    def _enqueue(self, irc_channel: str, text: str) -> None:
        if len(self._queue) >= (self._queue.maxlen or 0):
            logger.warning(
                "IRC send queue full (%d); dropping the oldest queued message",
                self._queue.maxlen,
            )
        self._queue.append((irc_channel, text))

    # -- irc side ----------------------------------------------------------

    async def _run_irc_supervisor(self) -> None:
        assert self._irc_client is not None
        reconnect_cfg = self._config.irc.reconnect
        delay = reconnect_cfg.initial_delay_seconds
        while not self._stopping:
            connected_at = asyncio.get_running_loop().time()
            try:
                await self._irc_client.run_until_disconnected()
            except Exception:
                logger.exception("IRC connection error")

            if self._stopping:
                return

            session_duration = asyncio.get_running_loop().time() - connected_at
            if session_duration >= reconnect_cfg.max_delay_seconds:
                delay = reconnect_cfg.initial_delay_seconds
            logger.warning("reconnecting to IRC in %.1fs", delay)
            await self._interruptible_sleep(delay)
            delay = min(delay * 2, reconnect_cfg.max_delay_seconds)

    async def _run_queue_drain(self) -> None:
        assert self._irc_client is not None
        loop = asyncio.get_running_loop()
        min_interval = self._config.irc.min_send_interval_seconds
        last_sent_at: float | None = None
        consecutive_failures = 0
        while not self._stopping:
            if not self._queue:
                await asyncio.sleep(_QUEUE_POLL_IDLE_SECONDS)
                continue
            if not self._irc_client.is_ready:
                await asyncio.sleep(_QUEUE_POLL_NOT_READY_SECONDS)
                continue
            if last_sent_at is not None:
                # Pace sends rather than firing the whole queue back-to-back
                # as fast as the socket accepts -- see _MAX_SEND_ATTEMPTS's
                # neighbor, min_send_interval_seconds, for why.
                remaining = min_interval - (loop.time() - last_sent_at)
                if remaining > 0:
                    await self._interruptible_sleep(remaining)
                    continue
            irc_channel, text = self._queue[0]
            try:
                await self._irc_client.send_privmsg(irc_channel, text)
            except Exception:
                consecutive_failures += 1
                if consecutive_failures >= _MAX_SEND_ATTEMPTS:
                    logger.error(
                        "dropping message to %s after %d failed send attempts: %r",
                        irc_channel,
                        consecutive_failures,
                        text,
                    )
                    self._queue.popleft()
                    consecutive_failures = 0
                    continue
                logger.exception("failed to send a queued message to IRC; will retry")
                await asyncio.sleep(_QUEUE_POLL_NOT_READY_SECONDS)
                continue
            last_sent_at = loop.time()
            consecutive_failures = 0
            self._queue.popleft()
