"""A hardware-free double for `meshcore.MeshCore`, used by `bridge.py` tests.

Provides a `FakeCommandHandler` scripting mechanism, and real
`meshcore.events.Event`/`EventType` objects for scripted results (plain
dataclasses/enums, no I/O) so assertions match exactly what real
orchestration code checks (`result.is_error()`, `result.payload`, ...).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from meshcore import EventType
from meshcore.events import Event


class FakeCommandHandler:
    """Stands in for `meshcore`'s `CommandHandler` (the `.commands` object).

    Script a response for a method before calling it:

        fake.script("get_channel", Event(EventType.CHANNEL_INFO, {...}))
        result = await fake.get_channel(0)   # returns that Event
        assert fake.calls == [("get_channel", (0,), {})]

    Any attribute access returns an async callable (`__getattr__`), so the
    double never falls behind as new `commands.*` methods are wired up --
    only the methods a given test actually scripts need to be mentioned.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self._scripts: dict[str, dict[str, Any]] = {}

    def script(self, method: str, *results: Any, repeat: bool = False) -> None:
        """Queue one or more results for `method`.

        Each result is either an `Event` to return or an `Exception`
        instance to raise. By default each call consumes the next result
        in order; pass `repeat=True` to reuse a single scripted result for
        every call instead.
        """
        self._scripts[method] = {"queue": list(results), "repeat": repeat}

    def __getattr__(self, name: str) -> Callable[..., Any]:
        if name.startswith("_"):
            raise AttributeError(name)

        async def _call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((name, args, kwargs))
            spec = self._scripts.get(name)
            if spec is None or not spec["queue"]:
                raise AssertionError(
                    f"FakeCommandHandler.{name}() called with no scripted result; "
                    f"call fake.commands.script({name!r}, Event(...)) in the test first"
                )
            result = spec["queue"][0] if spec["repeat"] else spec["queue"].pop(0)
            if isinstance(result, Exception):
                raise result
            return result

        return _call

    def call_count(self, method: str) -> int:
        return sum(1 for called, _, _ in self.calls if called == method)


@dataclass
class FakeMeshCore:
    """A hardware-free stand-in for a connected `meshcore.MeshCore` client."""

    self_info: dict[str, Any] = field(default_factory=lambda: {"name": "test-node"})
    commands: FakeCommandHandler = field(default_factory=FakeCommandHandler)
    subscriptions: list[tuple[EventType, Callable[..., Any]]] = field(default_factory=list)
    disconnected: bool = False
    auto_message_fetching_started: bool = False

    def subscribe(self, event_type: EventType, handler: Callable[..., Any]) -> None:
        self.subscriptions.append((event_type, handler))

    def fire(self, event_type: EventType, event: Any) -> None:
        """Test helper: synchronously invoke every handler subscribed to
        `event_type` with `event`. `bridge.py` only ever registers plain
        synchronous callbacks (matching the real dispatcher's inline-call
        path for non-coroutine callbacks), so this is a faithful enough
        stand-in for the real `EventDispatcher` without needing a running
        dispatch loop."""
        for subscribed_type, handler in list(self.subscriptions):
            if subscribed_type == event_type:
                handler(event)

    async def start_auto_message_fetching(self) -> None:
        self.auto_message_fetching_started = True

    async def disconnect(self) -> None:
        # The real `ConnectionManager.disconnect()` emits a DISCONNECTED
        # event via the dispatcher -- code under test relies on that (e.g.
        # bridge.py's stop() unblocking its own reconnect-wait via this
        # same event), so the fake must reproduce it too.
        self.disconnected = True
        self.fire(
            EventType.DISCONNECTED, Event(EventType.DISCONNECTED, {"reason": "manual_disconnect"})
        )
