"""bridge.py orchestration tests: a FakeMeshCore double + a fake IRC client
double wired together, no real hardware or IRC network involved -- the wire
protocols themselves are already covered by test_mesh_connection.py and
irc/test_client.py.
"""

from __future__ import annotations

import asyncio

import pytest
from meshcore import EventType
from meshcore.events import Event

from meshcore_irc_bridge.bridge import _MAX_QUEUED_LINES, Bridge
from meshcore_irc_bridge.config import (
    AuthConfig,
    BridgeConfig,
    ChannelMapping,
    IrcConfig,
    MeshConfig,
    MeshConnectionConfig,
    ReconnectConfig,
)
from meshcore_irc_bridge.mesh_connection import MeshConnectError
from tests.fakes.meshcore_double import FakeMeshCore


def make_bridge_config(**channel_overrides) -> BridgeConfig:
    channels = channel_overrides.get(
        "channels", (ChannelMapping(mesh_channel=0, irc_channel="#general"),)
    )
    return BridgeConfig(
        mesh=MeshConfig(connection=MeshConnectionConfig(kind="ble", address="AA:BB")),
        irc=IrcConfig(
            server="irc.example.org",
            port=6697,
            nickname="meshbot",
            auth=AuthConfig(mode="none"),
            reconnect=ReconnectConfig(initial_delay_seconds=0.01, max_delay_seconds=0.05),
        ),
        channels=channels,
    )


class FakeIRCClient:
    """A fake standing in for `IRCClient`'s public surface: becomes ready
    as soon as `run_until_disconnected()` is called, and blocks until
    `stop()` (or `disconnect_now()`, simulating an unexpected drop)."""

    def __init__(self, config, channels, *, ready_delay: float = 0.0) -> None:
        self.config = config
        self.channels = list(channels)
        self.sent: list[tuple[str, str]] = []
        self.is_ready = False
        self.stopped = False
        self.run_calls = 0
        self.fail_next_send = False
        self.raise_on_next_run: Exception | None = None
        self._ready_delay = ready_delay
        self._unblock = asyncio.Event()

    async def run_until_disconnected(self) -> None:
        self.run_calls += 1
        if self.raise_on_next_run is not None:
            exc, self.raise_on_next_run = self.raise_on_next_run, None
            raise exc
        if self._ready_delay:
            await asyncio.sleep(self._ready_delay)
        self._unblock = asyncio.Event()
        self.is_ready = True
        await self._unblock.wait()
        self.is_ready = False

    async def stop(self) -> None:
        self.stopped = True
        self._unblock.set()

    def disconnect_now(self) -> None:
        """Simulate an unexpected drop (not a `stop()`)."""
        self._unblock.set()

    async def send_privmsg(self, channel: str, text: str) -> None:
        if self.fail_next_send:
            self.fail_next_send = False
            raise ConnectionError("simulated send failure")
        self.sent.append((channel, text))


async def wait_until(predicate, timeout: float = 2.0, interval: float = 0.005) -> None:
    async def _poll():
        while not predicate():
            await asyncio.sleep(interval)

    await asyncio.wait_for(_poll(), timeout=timeout)


@pytest.fixture
def fake_irc_clients():
    """Returns (factory, created) -- `factory` is passed to `Bridge` as its
    `irc_client_factory`, and `created` accumulates every `FakeIRCClient`
    it builds (there's exactly one per `Bridge` in every test here)."""
    created: list[FakeIRCClient] = []

    def factory(irc_config, channels):
        client = FakeIRCClient(irc_config, channels)
        created.append(client)
        return client

    return factory, created


@pytest.fixture
def fake_mesh_clients():
    created: list[FakeMeshCore] = []

    async def connect(mesh_config):
        client = FakeMeshCore()
        # A quiet, successful default for the read-only channel-info
        # lookup bridge.py does on every (re)connect -- tests specifically
        # about that lookup's failure paths build their own mesh_connect
        # instead of using this fixture.
        client.commands.script(
            "get_channel",
            Event(EventType.CHANNEL_INFO, {"channel_idx": 0, "channel_name": "General"}),
            repeat=True,
        )
        created.append(client)
        return client

    return connect, created


async def _run_and_stop(bridge: Bridge):
    task = asyncio.create_task(bridge.run())
    return task


def channel_msg(channel_idx: int, text: str) -> Event:
    return Event(EventType.CHANNEL_MSG_RECV, {"channel_idx": channel_idx, "text": text})


# ---------------------------------------------------------------------------
# message relay
# ---------------------------------------------------------------------------


async def test_relays_mapped_channel_message_to_irc(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and irc_clients and irc_clients[0].is_ready)
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "hi"))

    await wait_until(lambda: irc_clients[0].sent)
    assert irc_clients[0].sent == [("#general", "hi")]

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_drops_message_for_unmapped_mesh_channel(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and irc_clients and irc_clients[0].is_ready)
    mesh_clients[0].fire(
        EventType.CHANNEL_MSG_RECV,
        Event(EventType.CHANNEL_MSG_RECV, {"channel_idx": 99, "text": "nobody maps to me"}),
    )
    # Prove nothing was queued/sent by sending a mapped message afterwards
    # and observing only *that* one arrive.
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "hi"))
    await wait_until(lambda: irc_clients[0].sent)
    assert irc_clients[0].sent == [("#general", "hi")]

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_multiple_mesh_channels_can_map_to_the_same_irc_channel(
    fake_irc_clients, fake_mesh_clients
):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config(
        channels=(
            ChannelMapping(mesh_channel=0, irc_channel="#general"),
            ChannelMapping(mesh_channel=1, irc_channel="#general"),
        )
    )
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    assert bridge.distinct_irc_channels == ["#general"]
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and irc_clients and irc_clients[0].is_ready)
    assert irc_clients[0].channels == ["#general"]

    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "a"))
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(1, "b"))
    await wait_until(lambda: len(irc_clients[0].sent) == 2)
    assert irc_clients[0].sent == [("#general", "a"), ("#general", "b")]

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_message_is_queued_until_irc_becomes_ready(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)

    # Enqueue directly (white-box) before the IRC side has ever become
    # ready, then start the supervisors and confirm the queued message is
    # flushed once IRC connects.
    bridge._enqueue("#general", "queued before irc connected")
    task = await _run_and_stop(bridge)

    await wait_until(lambda: irc_clients and irc_clients[0].sent)
    assert irc_clients[0].sent == [("#general", "queued before irc connected")]

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_bounded_queue_drops_oldest(fake_irc_clients, fake_mesh_clients):
    irc_factory, _irc_clients = fake_irc_clients
    mesh_connect, _mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)

    for i in range(_MAX_QUEUED_LINES + 5):
        bridge._enqueue("#general", f"msg{i}")

    assert len(bridge._queue) == _MAX_QUEUED_LINES
    # the oldest 5 were dropped; the queue keeps the most recent messages
    assert bridge._queue[0] == ("#general", "msg5")
    assert bridge._queue[-1] == ("#general", f"msg{_MAX_QUEUED_LINES + 4}")


# ---------------------------------------------------------------------------
# channel mapping validation (read-only, log-only)
# ---------------------------------------------------------------------------


async def test_warns_but_continues_when_get_channel_raises(fake_irc_clients):
    created: list[FakeMeshCore] = []

    async def mesh_connect(_mesh_config):
        client = FakeMeshCore()
        client.commands.script("get_channel", RuntimeError("device busy"))
        created.append(client)
        return client

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: created and irc_clients and irc_clients[0].is_ready)
    # The bridge must still be fully up (auto message fetching started)
    # despite the channel-info lookup failing.
    assert created[0].auto_message_fetching_started is True

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_warns_when_get_channel_returns_error_event(fake_irc_clients):
    created: list[FakeMeshCore] = []

    async def mesh_connect(_mesh_config):
        client = FakeMeshCore()
        client.commands.script("get_channel", Event(EventType.ERROR, {"reason": "not found"}))
        created.append(client)
        return client

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: created and irc_clients and irc_clients[0].is_ready)
    assert created[0].auto_message_fetching_started is True

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_warns_when_get_channel_returns_none(fake_irc_clients):
    created: list[FakeMeshCore] = []

    async def mesh_connect(_mesh_config):
        client = FakeMeshCore()
        client.commands.script("get_channel", None)
        created.append(client)
        return client

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: created and irc_clients and irc_clients[0].is_ready)
    assert created[0].auto_message_fetching_started is True

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_logs_channel_name_when_configured(fake_irc_clients):
    created: list[FakeMeshCore] = []

    async def mesh_connect(_mesh_config):
        client = FakeMeshCore()
        client.commands.script(
            "get_channel",
            Event(EventType.CHANNEL_INFO, {"channel_idx": 0, "channel_name": "General"}),
        )
        created.append(client)
        return client

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: created and created[0].commands.call_count("get_channel") == 1)
    assert created[0].commands.calls[0] == ("get_channel", (0,), {})

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


# ---------------------------------------------------------------------------
# mesh reconnect
# ---------------------------------------------------------------------------


async def test_mesh_disconnect_triggers_full_reconnect(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=mesh_connect,
        irc_client_factory=irc_factory,
        mesh_reconnect_initial_delay=0.01,
        mesh_reconnect_max_delay=0.05,
    )
    task = await _run_and_stop(bridge)

    await wait_until(lambda: len(mesh_clients) == 1 and irc_clients and irc_clients[0].is_ready)
    mesh_clients[0].fire(
        EventType.DISCONNECTED, Event(EventType.DISCONNECTED, {"reason": "timeout"})
    )

    await wait_until(lambda: len(mesh_clients) == 2)
    assert mesh_clients[0].disconnected is True

    # the new connection is fully live too
    await wait_until(lambda: mesh_clients[1].auto_message_fetching_started)

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_mesh_connect_failure_retries_with_backoff(fake_irc_clients):
    attempts = 0

    async def flaky_connect(_mesh_config):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise MeshConnectError("simulated failure")
        client = FakeMeshCore()
        return client

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=flaky_connect,
        irc_client_factory=irc_factory,
        mesh_reconnect_initial_delay=0.01,
        mesh_reconnect_max_delay=0.02,
    )
    task = await _run_and_stop(bridge)

    await wait_until(lambda: attempts >= 3, timeout=2)
    await wait_until(lambda: irc_clients and irc_clients[0].is_ready)

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


# ---------------------------------------------------------------------------
# irc reconnect
# ---------------------------------------------------------------------------


async def test_irc_disconnect_triggers_reconnect(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: irc_clients and irc_clients[0].run_calls == 1)
    irc_clients[0].disconnect_now()

    await wait_until(lambda: irc_clients[0].run_calls == 2)

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_send_failure_is_retried(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and irc_clients and irc_clients[0].is_ready)
    irc_clients[0].fail_next_send = True
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "hi"))

    await wait_until(lambda: irc_clients[0].sent == [("#general", "hi")], timeout=2)

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------


async def test_stop_disconnects_both_sides_cleanly(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and irc_clients and irc_clients[0].is_ready)
    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)

    assert irc_clients[0].stopped is True
    assert mesh_clients[0].disconnected is True


async def test_stop_suppresses_mesh_disconnect_errors(fake_irc_clients):
    class ExplodingMeshClient(FakeMeshCore):
        async def disconnect(self) -> None:
            raise RuntimeError("boom")

    created: list[FakeMeshCore] = []

    async def mesh_connect(_mesh_config):
        client = ExplodingMeshClient()
        created.append(client)
        return client

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: created and irc_clients and irc_clients[0].is_ready)
    await bridge.stop()  # must not raise even though disconnect() blows up
    await asyncio.wait_for(task, timeout=2)


async def test_stop_before_run_is_a_noop():
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=lambda _c: None, irc_client_factory=lambda c, ch: None)
    await bridge.stop()  # nothing ever connected; must not raise


# ---------------------------------------------------------------------------
# additional coverage: default factory, already-stopping races, backoff reset
# ---------------------------------------------------------------------------


def test_default_irc_client_factory_builds_a_real_ircclient():
    from meshcore_irc_bridge.irc.client import IRCClient

    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=lambda _c: None)
    client = bridge._irc_client_factory(config.irc, ["#general"])
    assert isinstance(client, IRCClient)


async def test_run_returns_immediately_if_already_stopping(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    bridge._stopping = True

    await asyncio.wait_for(bridge.run(), timeout=2)

    assert mesh_clients == []
    assert irc_clients[0].run_calls == 0


async def test_stop_during_mesh_connect_backoff_stops_cleanly(fake_irc_clients):
    async def always_fails(_mesh_config):
        raise MeshConnectError("simulated failure")

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=always_fails,
        irc_client_factory=irc_factory,
        mesh_reconnect_initial_delay=1,
        mesh_reconnect_max_delay=5,
    )
    task = await _run_and_stop(bridge)

    await wait_until(lambda: irc_clients and irc_clients[0].is_ready)
    await asyncio.sleep(0.01)  # let the mesh supervisor enter its backoff sleep
    await bridge.stop()
    # The interruptible backoff sleep notices _stopping within ~0.1s --
    # proves stop() doesn't block for the full 1s backoff delay.
    await asyncio.wait_for(task, timeout=0.5)


async def test_mesh_connect_failure_while_already_stopping_skips_backoff():
    # _run_mesh_supervisor tested in isolation (not through run()): a
    # MeshConnectError arriving after stop() was already requested must
    # return immediately rather than sleeping out a (possibly long)
    # backoff delay first.
    bridge_holder: list[Bridge] = []

    async def fails_and_marks_stopping(_mesh_config):
        bridge_holder[0]._stopping = True
        raise MeshConnectError("simulated")

    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=fails_and_marks_stopping,
        irc_client_factory=lambda c, ch: None,
        mesh_reconnect_initial_delay=5,
        mesh_reconnect_max_delay=10,
    )
    bridge_holder.append(bridge)

    await asyncio.wait_for(bridge._run_mesh_supervisor(), timeout=1)


async def test_mesh_backoff_resets_after_a_long_session(fake_irc_clients, fake_mesh_clients):
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=mesh_connect,
        irc_client_factory=irc_factory,
        mesh_reconnect_initial_delay=0.01,
        mesh_reconnect_max_delay=0.03,
    )
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and irc_clients and irc_clients[0].is_ready)
    await asyncio.sleep(0.05)  # outlast mesh_reconnect_max_delay
    mesh_clients[0].fire(EventType.DISCONNECTED, Event(EventType.DISCONNECTED, {"reason": "x"}))
    await wait_until(lambda: len(mesh_clients) == 2)

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_irc_reconnect_on_run_until_disconnected_exception(fake_mesh_clients):
    mesh_connect, mesh_clients = fake_mesh_clients
    created: list[FakeIRCClient] = []

    def irc_factory(irc_config, channels):
        client = FakeIRCClient(irc_config, channels)
        created.append(client)
        return client

    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=mesh_connect,
        irc_client_factory=irc_factory,
    )
    task = asyncio.create_task(bridge.run())
    await wait_until(lambda: created and created[0].is_ready)

    # End the current (successful) run and make the *next* one raise, so
    # the exception branch (not the clean disconnect_now() path already
    # covered elsewhere) is what drives the following reconnect.
    created[0].raise_on_next_run = ConnectionRefusedError("simulated")
    created[0].disconnect_now()

    # run #2 raises immediately (caught -> backoff -> reconnect); run #3
    # succeeds and becomes ready again.
    await wait_until(lambda: created[0].run_calls >= 3 and created[0].is_ready, timeout=2)

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_irc_backoff_resets_after_a_long_session(fake_mesh_clients):
    mesh_connect, mesh_clients = fake_mesh_clients
    created: list[FakeIRCClient] = []

    def irc_factory(irc_config, channels):
        client = FakeIRCClient(irc_config, channels)
        created.append(client)
        return client

    config = BridgeConfig(
        mesh=MeshConfig(connection=MeshConnectionConfig(kind="ble", address="AA:BB")),
        irc=IrcConfig(
            server="irc.example.org",
            port=6697,
            nickname="meshbot",
            auth=AuthConfig(mode="none"),
            reconnect=ReconnectConfig(initial_delay_seconds=0.01, max_delay_seconds=0.03),
        ),
        channels=(ChannelMapping(mesh_channel=0, irc_channel="#general"),),
    )

    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: created and created[0].is_ready)
    await asyncio.sleep(0.05)  # outlast max_delay_seconds
    created[0].disconnect_now()
    await wait_until(lambda: created[0].run_calls == 2)

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_queue_drain_waits_while_irc_not_ready(fake_mesh_clients):
    mesh_connect, mesh_clients = fake_mesh_clients
    created: list[FakeIRCClient] = []

    def irc_factory(irc_config, channels):
        client = FakeIRCClient(irc_config, channels, ready_delay=0.2)
        created.append(client)
        return client

    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients)
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "hi"))
    # Immediately after enqueueing, IRC is deliberately not ready yet --
    # give the drain loop a beat to observe that and poll rather than send.
    await asyncio.sleep(0.05)
    assert created[0].is_ready is False
    assert created[0].sent == []

    await wait_until(lambda: created[0].sent == [("#general", "hi")], timeout=2)

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_run_propagates_unexpected_task_failure_and_cancels_siblings():
    async def boom_mesh_connect(_mesh_config):
        raise RuntimeError("totally unexpected, not a MeshConnectError")

    class ForeverIRCClient:
        def __init__(self, config, channels):
            self.is_ready = False
            self._stopped = asyncio.Event()

        async def run_until_disconnected(self):
            await self._stopped.wait()

        async def stop(self):
            self._stopped.set()

        async def send_privmsg(self, channel, text):
            raise AssertionError("never sent in this test")

    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=boom_mesh_connect,
        irc_client_factory=lambda c, ch: ForeverIRCClient(c, ch),
    )

    # _run_mesh_supervisor treats non-MeshConnectError as fatal (it isn't
    # caught), so run() must propagate it and tear down the still-running
    # irc/queue-drain tasks rather than leaving them dangling.
    with pytest.raises(RuntimeError, match="totally unexpected"):
        await asyncio.wait_for(bridge.run(), timeout=2)
