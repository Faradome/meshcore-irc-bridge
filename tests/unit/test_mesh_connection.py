"""Unit tests for mesh_connection.py's transport-dispatch logic.

`meshcore.MeshCore.create_ble/create_serial/create_tcp` are monkeypatched
with fake async functions (success, None, and raising) so this fully
exercises `connect()` -- including its error-wrapping -- without touching
real BLE, serial, or TCP hardware, mirroring meshcorectl's own
`tests/unit/test_connect.py`.
"""

from __future__ import annotations

import pytest
from meshcore import MeshCore

from meshcore_irc_bridge.config import MeshConfig, MeshConnectionConfig
from meshcore_irc_bridge.mesh_connection import MeshConnectError, connect


async def _fake_ok(*args, **kwargs):
    return "connected-client"


async def _fake_returns_none(*args, **kwargs):
    return None


async def _fake_raises(*args, **kwargs):
    raise RuntimeError("no hardware here")


async def test_connect_ble_success(monkeypatch):
    monkeypatch.setattr(MeshCore, "create_ble", _fake_ok)
    mesh_config = MeshConfig(connection=MeshConnectionConfig(kind="ble", address="AA:BB"))
    result = await connect(mesh_config)
    assert result == "connected-client"


async def test_connect_serial_success(monkeypatch):
    monkeypatch.setattr(MeshCore, "create_serial", _fake_ok)
    mesh_config = MeshConfig(
        connection=MeshConnectionConfig(kind="serial", port="/dev/ttyUSB0")
    )
    result = await connect(mesh_config)
    assert result == "connected-client"


async def test_connect_tcp_success(monkeypatch):
    monkeypatch.setattr(MeshCore, "create_tcp", _fake_ok)
    mesh_config = MeshConfig(
        connection=MeshConnectionConfig(kind="tcp", host="10.0.0.1", tcp_port=5000)
    )
    result = await connect(mesh_config)
    assert result == "connected-client"


async def test_connect_ble_returns_none_is_a_connect_error(monkeypatch):
    monkeypatch.setattr(MeshCore, "create_ble", _fake_returns_none)
    mesh_config = MeshConfig(connection=MeshConnectionConfig(kind="ble", address="AA:BB"))
    with pytest.raises(MeshConnectError, match="no response from the device"):
        await connect(mesh_config)


async def test_connect_ble_failure_wrapped(monkeypatch):
    monkeypatch.setattr(MeshCore, "create_ble", _fake_raises)
    mesh_config = MeshConfig(connection=MeshConnectionConfig(kind="ble", address="AA:BB"))
    with pytest.raises(MeshConnectError, match="no hardware here"):
        await connect(mesh_config)


async def test_connect_serial_failure_wrapped(monkeypatch):
    monkeypatch.setattr(MeshCore, "create_serial", _fake_raises)
    mesh_config = MeshConfig(
        connection=MeshConnectionConfig(kind="serial", port="/dev/ttyUSB0")
    )
    with pytest.raises(MeshConnectError, match="no hardware here"):
        await connect(mesh_config)


async def test_connect_tcp_failure_wrapped(monkeypatch):
    monkeypatch.setattr(MeshCore, "create_tcp", _fake_raises)
    mesh_config = MeshConfig(
        connection=MeshConnectionConfig(kind="tcp", host="10.0.0.1", tcp_port=5000)
    )
    with pytest.raises(MeshConnectError, match="no hardware here"):
        await connect(mesh_config)


async def test_connect_passes_auto_reconnect_settings_through(monkeypatch):
    captured = {}

    async def fake_create_ble(**kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(MeshCore, "create_ble", fake_create_ble)
    mesh_config = MeshConfig(
        connection=MeshConnectionConfig(kind="ble", address="AA:BB"),
        auto_reconnect=False,
        max_reconnect_attempts=9,
    )
    await connect(mesh_config, debug=True)
    assert captured == {
        "address": "AA:BB",
        "debug": True,
        "auto_reconnect": False,
        "max_reconnect_attempts": 9,
    }


async def test_connect_serial_passes_baudrate(monkeypatch):
    captured = {}

    async def fake_create_serial(**kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(MeshCore, "create_serial", fake_create_serial)
    mesh_config = MeshConfig(
        connection=MeshConnectionConfig(kind="serial", port="/dev/ttyUSB0", baudrate=9600)
    )
    await connect(mesh_config)
    assert captured["port"] == "/dev/ttyUSB0"
    assert captured["baudrate"] == 9600


async def test_connect_tcp_passes_host_and_port(monkeypatch):
    captured = {}

    async def fake_create_tcp(**kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(MeshCore, "create_tcp", fake_create_tcp)
    mesh_config = MeshConfig(
        connection=MeshConnectionConfig(kind="tcp", host="10.0.0.1", tcp_port=4000)
    )
    await connect(mesh_config)
    assert captured["host"] == "10.0.0.1"
    assert captured["port"] == 4000
