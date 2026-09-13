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
            # Pacing (min_send_interval_seconds) is exercised by its own
            # dedicated tests below; disabled here so every other test's
            # multi-message assertions aren't slowed down incidentally.
            min_send_interval_seconds=0.0,
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
    # Two mesh channels sharing one IRC destination is exactly the case
    # that needs the "[<mesh_channel>] " disambiguating prefix -- without
    # it, messages from either mesh channel would be indistinguishable
    # once interleaved in the same IRC channel.
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
    assert irc_clients[0].sent == [("#general", "[0] a"), ("#general", "[1] b")]

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_unique_mesh_to_irc_mapping_gets_no_prefix(fake_irc_clients, fake_mesh_clients):
    # The common case -- one mesh channel, one IRC channel, nothing
    # shared -- must stay unadorned; only ambiguous destinations need the
    # prefix.
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config(
        channels=(ChannelMapping(mesh_channel=0, irc_channel="#general"),)
    )
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and irc_clients and irc_clients[0].is_ready)
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "hi"))
    await wait_until(lambda: irc_clients[0].sent)
    assert irc_clients[0].sent == [("#general", "hi")]

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_prefix_applies_only_to_the_shared_destination(fake_irc_clients, fake_mesh_clients):
    # Mixed config: channels 0 and 1 share #general (ambiguous, prefixed);
    # channel 2 has #other all to itself (unambiguous, unprefixed).
    irc_factory, irc_clients = fake_irc_clients
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config(
        channels=(
            ChannelMapping(mesh_channel=0, irc_channel="#general"),
            ChannelMapping(mesh_channel=1, irc_channel="#general"),
            ChannelMapping(mesh_channel=2, irc_channel="#other"),
        )
    )
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    assert bridge._shared_irc_channels == frozenset({"#general"})
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and irc_clients and irc_clients[0].is_ready)
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "a"))
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(1, "b"))
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(2, "c"))
    await wait_until(lambda: len(irc_clients[0].sent) == 3)
    assert irc_clients[0].sent == [
        ("#general", "[0] a"),
        ("#general", "[1] b"),
        ("#other", "c"),
    ]

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


# ---------------------------------------------------------------------------
# stop() interrupting an in-flight mesh connect (H1)
# ---------------------------------------------------------------------------


async def test_stop_cancels_an_in_flight_mesh_connect(fake_irc_clients):
    # mesh_connect (create_ble/create_serial/create_tcp underneath) has no
    # timeout of its own -- TCP retries and BLE discovery can both take far
    # longer than any reasonable shutdown should wait, and self._mesh_client
    # is still None throughout, so stop()'s own disconnect() branch has
    # nothing to act on. stop() must cancel the connect itself instead of
    # only setting a flag nothing here would ever check.
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def hanging_connect(_mesh_config):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise
        raise AssertionError("unreachable")  # pragma: no cover

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=hanging_connect, irc_client_factory=irc_factory)
    task = asyncio.create_task(bridge.run())

    await asyncio.wait_for(started.wait(), timeout=2)
    await bridge.stop()
    # Must return promptly -- not hang until the (never-completing) connect
    # finishes on its own.
    await asyncio.wait_for(task, timeout=1)

    assert cancelled.is_set()


async def test_mesh_connect_cancellation_propagates_when_not_stopping(fake_irc_clients):
    # Distinct from test_stop_cancels_an_in_flight_mesh_connect: a
    # cancellation that didn't come from our own stop() (_stopping still
    # False) must propagate as a real CancelledError, not be swallowed as
    # if it were a deliberate stop().
    started = asyncio.Event()

    async def hanging_connect(_mesh_config):
        started.set()
        await asyncio.Event().wait()

    irc_factory, _irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=hanging_connect, irc_client_factory=irc_factory)

    task = asyncio.create_task(bridge._run_mesh_supervisor())
    await asyncio.wait_for(started.wait(), timeout=2)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_stop_is_a_noop_once_the_mesh_connect_has_already_finished(fake_mesh_clients):
    # The counterpart to the above: once the connect task is done, stop()
    # must not try to cancel it (a no-op on an already-done task, but
    # exercised explicitly here rather than left implicit).
    mesh_connect, mesh_clients = fake_mesh_clients
    config = make_bridge_config()
    bridge = Bridge(
        config, mesh_connect=mesh_connect, irc_client_factory=lambda c, ch: FakeIRCClient(c, ch)
    )
    task = asyncio.create_task(bridge.run())
    await wait_until(lambda: mesh_clients)
    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


# ---------------------------------------------------------------------------
# mesh session errors after connect must not crash the whole bridge (H2)
# ---------------------------------------------------------------------------


async def test_mesh_session_error_after_connect_triggers_reconnect_not_crash(fake_irc_clients):
    # start_auto_message_fetching() (or either subscribe() call) raising
    # something other than MeshConnectError -- e.g. a live BLE write
    # failure the library doesn't convert into a DISCONNECTED event -- must
    # be treated like an ordinary disconnect-and-reconnect, not propagate
    # out of run() and tear down the (perfectly healthy) IRC side with it.
    attempts = 0

    class ExplodingOnceMeshClient(FakeMeshCore):
        async def start_auto_message_fetching(self) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("simulated transient radio error")
            self.auto_message_fetching_started = True

    created: list[FakeMeshCore] = []

    async def mesh_connect(_mesh_config):
        client = ExplodingOnceMeshClient()
        created.append(client)
        return client

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=mesh_connect,
        irc_client_factory=irc_factory,
        mesh_reconnect_initial_delay=0.01,
        mesh_reconnect_max_delay=0.02,
    )
    task = await _run_and_stop(bridge)

    await wait_until(lambda: len(created) >= 2 and created[1].auto_message_fetching_started)
    # The IRC side must never have been touched by the mesh-side error.
    assert irc_clients[0].is_ready is True
    assert irc_clients[0].stopped is False

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_mesh_session_error_logs_instead_of_propagating(fake_irc_clients, caplog):
    class ExplodingMeshClient(FakeMeshCore):
        async def start_auto_message_fetching(self) -> None:
            raise RuntimeError("simulated transient radio error")

    created: list[FakeMeshCore] = []

    async def mesh_connect(_mesh_config):
        client = ExplodingMeshClient()
        created.append(client)
        return client

    irc_factory, irc_clients = fake_irc_clients
    config = make_bridge_config()
    bridge = Bridge(
        config,
        mesh_connect=mesh_connect,
        irc_client_factory=irc_factory,
        mesh_reconnect_initial_delay=0.01,
        mesh_reconnect_max_delay=0.02,
    )
    task = await _run_and_stop(bridge)

    with caplog.at_level("ERROR"):
        await wait_until(lambda: len(created) >= 2)
    assert "mesh session error" in caplog.text

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


# ---------------------------------------------------------------------------
# outbound send pacing (M2)
# ---------------------------------------------------------------------------


async def test_queue_drain_paces_successive_sends(fake_mesh_clients):
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
            reconnect=ReconnectConfig(initial_delay_seconds=0.01, max_delay_seconds=0.05),
            min_send_interval_seconds=0.2,
        ),
        channels=(ChannelMapping(mesh_channel=0, irc_channel="#general"),),
    )
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and created and created[0].is_ready)
    loop = asyncio.get_running_loop()
    start = loop.time()
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "a"))
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "b"))

    await wait_until(lambda: len(created[0].sent) == 2, timeout=3)
    # The second send must have been held back by ~min_send_interval_seconds
    # rather than firing immediately back-to-back with the first.
    assert loop.time() - start >= 0.2
    assert created[0].sent == [("#general", "a"), ("#general", "b")]

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


async def test_queue_drain_pacing_disabled_by_default_zero_is_immediate(fake_mesh_clients):
    # make_bridge_config() sets min_send_interval_seconds=0.0 explicitly;
    # this proves that setting is actually load-bearing (no implicit
    # pacing sneaks in when it's zero).
    mesh_connect, mesh_clients = fake_mesh_clients
    created: list[FakeIRCClient] = []

    def irc_factory(irc_config, channels):
        client = FakeIRCClient(irc_config, channels)
        created.append(client)
        return client

    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and created and created[0].is_ready)
    loop = asyncio.get_running_loop()
    start = loop.time()
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "a"))
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "b"))

    await wait_until(lambda: len(created[0].sent) == 2, timeout=2)
    assert loop.time() - start < 0.2

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)


# ---------------------------------------------------------------------------
# bounded send retries (M5)
# ---------------------------------------------------------------------------


async def test_persistently_failing_send_is_dropped_after_max_attempts(fake_mesh_clients, caplog):
    mesh_connect, mesh_clients = fake_mesh_clients

    class SelectivelyFailingIRCClient(FakeIRCClient):
        async def send_privmsg(self, channel: str, text: str) -> None:
            if text == "poison":
                raise ConnectionError("simulated permanent failure")
            await super().send_privmsg(channel, text)

    created: list[FakeIRCClient] = []

    def irc_factory(irc_config, channels):
        client = SelectivelyFailingIRCClient(irc_config, channels)
        created.append(client)
        return client

    config = make_bridge_config()
    bridge = Bridge(config, mesh_connect=mesh_connect, irc_client_factory=irc_factory)
    task = await _run_and_stop(bridge)

    await wait_until(lambda: mesh_clients and created and created[0].is_ready)
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "poison"))
    mesh_clients[0].fire(EventType.CHANNEL_MSG_RECV, channel_msg(0, "after"))

    # "poison" must be dropped (not retried forever) so "after" -- stuck
    # behind it in the same single queue -- eventually gets through too.
    with caplog.at_level("ERROR"):
        await wait_until(lambda: created[0].sent == [("#general", "after")], timeout=3)
    assert "dropping message" in caplog.text
    assert "poison" in caplog.text
    assert len(bridge._queue) == 0

    await bridge.stop()
    await asyncio.wait_for(task, timeout=2)
