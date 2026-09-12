"""Opens a live connection to the companion radio described by a
`MeshConfig`.

Mirrors `meshcorectl`'s own connection seam
(`/Users/william/src/meshcore/src/meshcorectl/connect.py`): `meshcore.MeshCore`
is imported inside `connect()`, not at module scope, so tests can
monkeypatch `MeshCore.create_ble/create_serial/create_tcp` without ever
importing `bleak`/`pyserial` for real, and `bridge.py` depends only on the
small `MeshCoreConnection` protocol below, never on `meshcore.MeshCore`
directly.
"""

from __future__ import annotations

from typing import Any, Protocol, cast, runtime_checkable

from .config import MeshConfig


class MeshConnectError(Exception):
    """Raised when a connection to the companion radio cannot be established."""


@runtime_checkable
class MeshCoreConnection(Protocol):
    """The subset of `meshcore.MeshCore` this bridge uses."""

    commands: Any

    def subscribe(self, event_type: Any, handler: Any) -> Any: ...  # pragma: no cover

    async def disconnect(self) -> None: ...  # pragma: no cover

    async def start_auto_message_fetching(self) -> Any: ...  # pragma: no cover


async def connect(mesh_config: MeshConfig, *, debug: bool = False) -> MeshCoreConnection:
    """Open a connection to the companion radio described by `mesh_config`.

    Raises `MeshConnectError` wrapping whatever the underlying transport
    raised (`BleakError`, `ConnectionError`, `TimeoutError`, ...), or if the
    factory returned `None` -- `meshcore`'s own "connect failed" signal,
    which (unlike an exception) is not automatically wrapped by anything
    below this function -- so callers only need to handle one exception
    type either way.
    """
    from meshcore import MeshCore

    connection = mesh_config.connection
    try:
        if connection.kind == "ble":
            client = await MeshCore.create_ble(
                address=connection.address,
                debug=debug,
                auto_reconnect=mesh_config.auto_reconnect,
                max_reconnect_attempts=mesh_config.max_reconnect_attempts,
            )
        elif connection.kind == "serial":
            assert connection.port is not None  # guaranteed by MeshConnectionConfig.__post_init__
            client = await MeshCore.create_serial(
                port=connection.port,
                baudrate=connection.baudrate,
                debug=debug,
                auto_reconnect=mesh_config.auto_reconnect,
                max_reconnect_attempts=mesh_config.max_reconnect_attempts,
            )
        else:  # tcp
            assert connection.host is not None  # guaranteed by MeshConnectionConfig.__post_init__
            client = await MeshCore.create_tcp(
                host=connection.host,
                port=connection.tcp_port,
                debug=debug,
                auto_reconnect=mesh_config.auto_reconnect,
                max_reconnect_attempts=mesh_config.max_reconnect_attempts,
            )
    except Exception as exc:
        raise MeshConnectError(f"failed to connect to the companion radio: {exc}") from exc

    if client is None:
        raise MeshConnectError(
            "failed to connect to the companion radio: no response from the device"
        )

    # MeshCore satisfies MeshCoreConnection structurally, but `subscribe`'s
    # signature is wider than this protocol exposes, which reads as a
    # mismatch to a structural type checker.
    return cast(MeshCoreConnection, client)
