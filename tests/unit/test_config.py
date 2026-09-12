"""Unit tests for meshcore_irc_bridge.config -- no filesystem or asyncio needed
for build_config(); load_config() tests use tmp_path for the one file-I/O seam.
"""

from __future__ import annotations

import copy

import pytest
import yaml

from meshcore_irc_bridge.config import (
    BridgeConfig,
    ConfigError,
    build_config,
    load_config,
)


def _base_config() -> dict:
    return {
        "mesh": {
            "connection": {"type": "ble", "address": "AA:BB:CC:DD:EE:FF"},
        },
        "irc": {
            "server": "irc.example.org",
            "port": 6697,
            "nickname": "meshbridge",
            "auth": {"mode": "none"},
        },
        "channels": [
            {"mesh_channel": 0, "irc_channel": "#mesh-general"},
        ],
    }


def _with(base: dict, **overrides) -> dict:
    """Deep-copy `base` and shallow-merge top-level `overrides` in."""
    cfg = copy.deepcopy(base)
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# happy paths
# ---------------------------------------------------------------------------


def test_build_config_minimal_none_auth():
    cfg = build_config(_base_config())
    assert isinstance(cfg, BridgeConfig)
    assert cfg.mesh.connection.kind == "ble"
    assert cfg.mesh.connection.address == "AA:BB:CC:DD:EE:FF"
    assert cfg.mesh.auto_reconnect is True
    assert cfg.mesh.max_reconnect_attempts == 5
    assert cfg.irc.server == "irc.example.org"
    assert cfg.irc.port == 6697
    assert cfg.irc.auth.mode == "none"
    assert cfg.irc.username == "meshbridge"  # defaulted from nickname
    assert cfg.irc.realname == "meshbridge"
    assert cfg.irc_channel_for(0) == "#mesh-general"
    assert cfg.irc_channel_for(99) is None


def test_build_config_serial_connection():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "serial", "port": "/dev/ttyUSB0", "baudrate": 9600}
    cfg = build_config(raw)
    assert cfg.mesh.connection.kind == "serial"
    assert cfg.mesh.connection.port == "/dev/ttyUSB0"
    assert cfg.mesh.connection.baudrate == 9600


def test_build_config_serial_connection_default_baudrate():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "serial", "port": "/dev/ttyUSB0"}
    cfg = build_config(raw)
    assert cfg.mesh.connection.baudrate == 115200


def test_build_config_tcp_connection():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "tcp", "host": "192.168.1.50", "port": 4000}
    cfg = build_config(raw)
    assert cfg.mesh.connection.kind == "tcp"
    assert cfg.mesh.connection.host == "192.168.1.50"
    assert cfg.mesh.connection.tcp_port == 4000


def test_build_config_tcp_connection_default_port():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "tcp", "host": "192.168.1.50"}
    cfg = build_config(raw)
    assert cfg.mesh.connection.tcp_port == 5000


def test_build_config_mesh_overrides():
    raw = _base_config()
    raw["mesh"]["auto_reconnect"] = False
    raw["mesh"]["max_reconnect_attempts"] = 2
    cfg = build_config(raw)
    assert cfg.mesh.auto_reconnect is False
    assert cfg.mesh.max_reconnect_attempts == 2


def test_build_config_sasl_auth():
    raw = _base_config()
    raw["irc"]["auth"] = {
        "mode": "sasl",
        "sasl": {"username": "mynick", "password": "hunter2"},
    }
    cfg = build_config(raw)
    assert cfg.irc.auth.mode == "sasl"
    assert cfg.irc.auth.sasl.username == "mynick"
    assert cfg.irc.auth.sasl.password == "hunter2"


def test_build_config_nickserv_auth_default_join_wait():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "nickserv", "nickserv": {"password": "hunter2"}}
    cfg = build_config(raw)
    assert cfg.irc.auth.mode == "nickserv"
    assert cfg.irc.auth.nickserv.password == "hunter2"
    assert cfg.irc.auth.nickserv.join_wait_seconds == 5.0


def test_build_config_nickserv_auth_custom_join_wait():
    raw = _base_config()
    raw["irc"]["auth"] = {
        "mode": "nickserv",
        "nickserv": {"password": "hunter2", "join_wait_seconds": 12},
    }
    cfg = build_config(raw)
    assert cfg.irc.auth.nickserv.join_wait_seconds == 12.0


def test_build_config_explicit_username_realname_not_defaulted():
    raw = _base_config()
    raw["irc"]["username"] = "svc"
    raw["irc"]["realname"] = "Mesh Bridge Bot"
    cfg = build_config(raw)
    assert cfg.irc.username == "svc"
    assert cfg.irc.realname == "Mesh Bridge Bot"


def test_build_config_tls_default_true_and_explicit_false():
    cfg = build_config(_base_config())
    assert cfg.irc.tls is True

    raw = _base_config()
    raw["irc"]["tls"] = False
    cfg2 = build_config(raw)
    assert cfg2.irc.tls is False


def test_build_config_reconnect_defaults_and_overrides():
    cfg = build_config(_base_config())
    assert cfg.irc.reconnect.initial_delay_seconds == 1.0
    assert cfg.irc.reconnect.max_delay_seconds == 60.0

    raw = _base_config()
    raw["irc"]["reconnect"] = {"initial_delay_seconds": 2, "max_delay_seconds": 30}
    cfg2 = build_config(raw)
    assert cfg2.irc.reconnect.initial_delay_seconds == 2.0
    assert cfg2.irc.reconnect.max_delay_seconds == 30.0


def test_build_config_ping_watchdog_defaults():
    cfg = build_config(_base_config())
    assert cfg.irc.ping_idle_seconds == 120.0
    assert cfg.irc.ping_timeout_seconds == 60.0


def test_build_config_ping_watchdog_overrides():
    raw = _base_config()
    raw["irc"]["ping_idle_seconds"] = 30
    raw["irc"]["ping_timeout_seconds"] = 15
    cfg = build_config(raw)
    assert cfg.irc.ping_idle_seconds == 30.0
    assert cfg.irc.ping_timeout_seconds == 15.0


def test_build_config_ping_idle_seconds_not_positive():
    raw = _base_config()
    raw["irc"]["ping_idle_seconds"] = 0
    with pytest.raises(ConfigError, match="irc.ping_idle_seconds must be > 0"):
        build_config(raw)


def test_build_config_ping_timeout_seconds_not_positive():
    raw = _base_config()
    raw["irc"]["ping_timeout_seconds"] = -1
    with pytest.raises(ConfigError, match="irc.ping_timeout_seconds must be > 0"):
        build_config(raw)


def test_build_config_ping_idle_seconds_not_a_number():
    raw = _base_config()
    raw["irc"]["ping_idle_seconds"] = "soon"
    with pytest.raises(ConfigError, match="irc.ping_idle_seconds must be a number"):
        build_config(raw)


def test_build_config_irc_channel_using_ampersand_prefix():
    raw = _base_config()
    raw["channels"] = [{"mesh_channel": 0, "irc_channel": "&local"}]
    cfg = build_config(raw)
    assert cfg.irc_channel_for(0) == "&local"


def test_build_config_multiple_channels():
    raw = _base_config()
    raw["channels"] = [
        {"mesh_channel": 0, "irc_channel": "#general"},
        {"mesh_channel": 1, "irc_channel": "#emergency"},
    ]
    cfg = build_config(raw)
    assert cfg.irc_channel_for(0) == "#general"
    assert cfg.irc_channel_for(1) == "#emergency"


# ---------------------------------------------------------------------------
# top-level structural errors
# ---------------------------------------------------------------------------


def test_build_config_rejects_non_dict_input():
    with pytest.raises(ConfigError, match="must be a mapping"):
        build_config(["not", "a", "dict"])  # type: ignore[arg-type]


def test_build_config_missing_mesh_section():
    raw = _base_config()
    del raw["mesh"]
    with pytest.raises(ConfigError, match="mesh is required"):
        build_config(raw)


def test_build_config_mesh_not_a_mapping():
    raw = _with(_base_config(), mesh="oops")
    with pytest.raises(ConfigError, match="mesh must be a mapping"):
        build_config(raw)


def test_build_config_missing_irc_section():
    raw = _base_config()
    del raw["irc"]
    with pytest.raises(ConfigError, match="irc is required"):
        build_config(raw)


def test_build_config_irc_not_a_mapping():
    raw = _with(_base_config(), irc="oops")
    with pytest.raises(ConfigError, match="irc must be a mapping"):
        build_config(raw)


def test_build_config_missing_channels():
    raw = _base_config()
    del raw["channels"]
    with pytest.raises(ConfigError, match="channels is required"):
        build_config(raw)


# ---------------------------------------------------------------------------
# mesh.connection errors
# ---------------------------------------------------------------------------


def test_build_config_mesh_missing_connection():
    raw = _base_config()
    del raw["mesh"]["connection"]
    with pytest.raises(ConfigError, match="mesh.connection is required"):
        build_config(raw)


def test_build_config_mesh_connection_not_a_mapping():
    raw = _base_config()
    raw["mesh"]["connection"] = "oops"
    with pytest.raises(ConfigError, match="mesh.connection must be a mapping"):
        build_config(raw)


def test_build_config_mesh_connection_missing_type():
    raw = _base_config()
    del raw["mesh"]["connection"]["type"]
    with pytest.raises(ConfigError, match="mesh.connection.type is required"):
        build_config(raw)


def test_build_config_mesh_connection_invalid_type():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "carrier-pigeon"}
    with pytest.raises(ConfigError, match="mesh.connection.type must be one of"):
        build_config(raw)


def test_build_config_ble_missing_address():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "ble"}
    with pytest.raises(ConfigError, match="mesh.connection.address is required"):
        build_config(raw)


def test_build_config_serial_missing_port():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "serial"}
    with pytest.raises(ConfigError, match="mesh.connection.port is required"):
        build_config(raw)


def test_build_config_tcp_missing_host():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "tcp"}
    with pytest.raises(ConfigError, match="mesh.connection.host is required"):
        build_config(raw)


def test_build_config_tcp_port_not_an_integer():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "tcp", "host": "h", "port": "not-a-number"}
    with pytest.raises(ConfigError, match="mesh.connection.port must be an integer"):
        build_config(raw)


def test_build_config_baudrate_not_an_integer():
    raw = _base_config()
    raw["mesh"]["connection"] = {"type": "serial", "port": "/dev/x", "baudrate": "fast"}
    with pytest.raises(ConfigError, match="mesh.connection.baudrate must be an integer"):
        build_config(raw)


def test_build_config_max_reconnect_attempts_too_low():
    raw = _with(_base_config())
    raw["mesh"]["max_reconnect_attempts"] = 0
    with pytest.raises(ConfigError, match="mesh.max_reconnect_attempts must be >= 1"):
        build_config(raw)


def test_build_config_max_reconnect_attempts_not_an_integer():
    raw = _base_config()
    raw["mesh"]["max_reconnect_attempts"] = "lots"
    with pytest.raises(ConfigError, match="mesh.max_reconnect_attempts must be an integer"):
        build_config(raw)


# ---------------------------------------------------------------------------
# irc top-level errors
# ---------------------------------------------------------------------------


def test_build_config_irc_missing_server():
    raw = _base_config()
    del raw["irc"]["server"]
    with pytest.raises(ConfigError, match="irc.server is required"):
        build_config(raw)


def test_build_config_irc_empty_server():
    raw = _base_config()
    raw["irc"]["server"] = ""
    with pytest.raises(ConfigError, match="irc.server is required"):
        build_config(raw)


def test_build_config_irc_missing_port():
    raw = _base_config()
    del raw["irc"]["port"]
    with pytest.raises(ConfigError, match="irc.port is required"):
        build_config(raw)


def test_build_config_irc_port_not_an_integer():
    raw = _base_config()
    raw["irc"]["port"] = "not-a-port"
    with pytest.raises(ConfigError, match="irc.port must be an integer"):
        build_config(raw)


@pytest.mark.parametrize("bad_port", [0, -1, 65536, 999999])
def test_build_config_irc_port_out_of_range(bad_port):
    raw = _base_config()
    raw["irc"]["port"] = bad_port
    with pytest.raises(ConfigError, match="irc.port must be between 1 and 65535"):
        build_config(raw)


def test_build_config_irc_missing_nickname():
    raw = _base_config()
    del raw["irc"]["nickname"]
    with pytest.raises(ConfigError, match="irc.nickname is required"):
        build_config(raw)


def test_build_config_irc_empty_nickname():
    raw = _base_config()
    raw["irc"]["nickname"] = ""
    with pytest.raises(ConfigError, match="irc.nickname is required"):
        build_config(raw)


def test_build_config_irc_missing_auth():
    raw = _base_config()
    del raw["irc"]["auth"]
    with pytest.raises(ConfigError, match="irc.auth is required"):
        build_config(raw)


def test_build_config_irc_auth_not_a_mapping():
    raw = _base_config()
    raw["irc"]["auth"] = "oops"
    with pytest.raises(ConfigError, match="irc.auth must be a mapping"):
        build_config(raw)


def test_build_config_irc_reconnect_not_a_mapping():
    raw = _base_config()
    raw["irc"]["reconnect"] = "oops"
    with pytest.raises(ConfigError, match="irc.reconnect must be a mapping"):
        build_config(raw)


def test_build_config_irc_reconnect_initial_delay_not_a_number():
    raw = _base_config()
    raw["irc"]["reconnect"] = {"initial_delay_seconds": "soon"}
    with pytest.raises(ConfigError, match="irc.reconnect.initial_delay_seconds must be a number"):
        build_config(raw)


def test_build_config_irc_reconnect_initial_delay_not_positive():
    raw = _base_config()
    raw["irc"]["reconnect"] = {"initial_delay_seconds": 0}
    with pytest.raises(ConfigError, match="irc.reconnect.initial_delay_seconds must be > 0"):
        build_config(raw)


def test_build_config_irc_reconnect_max_delay_below_initial():
    raw = _base_config()
    raw["irc"]["reconnect"] = {"initial_delay_seconds": 10, "max_delay_seconds": 5}
    with pytest.raises(
        ConfigError, match="irc.reconnect.max_delay_seconds must be >= initial_delay_seconds"
    ):
        build_config(raw)


# ---------------------------------------------------------------------------
# irc.auth.* errors
# ---------------------------------------------------------------------------


def test_build_config_auth_missing_mode():
    raw = _base_config()
    raw["irc"]["auth"] = {}
    with pytest.raises(ConfigError, match="irc.auth.mode is required"):
        build_config(raw)


def test_build_config_auth_invalid_mode():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "telepathy"}
    with pytest.raises(ConfigError, match="irc.auth.mode must be one of"):
        build_config(raw)


def test_build_config_auth_sasl_mode_missing_sasl_block():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "sasl"}
    with pytest.raises(ConfigError, match="irc.auth.sasl is required"):
        build_config(raw)


def test_build_config_auth_sasl_block_not_a_mapping():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "sasl", "sasl": "oops"}
    with pytest.raises(ConfigError, match="irc.auth.sasl must be a mapping"):
        build_config(raw)


def test_build_config_auth_sasl_missing_username():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "sasl", "sasl": {"password": "x"}}
    with pytest.raises(ConfigError, match="irc.auth.sasl.username is required"):
        build_config(raw)


def test_build_config_auth_sasl_missing_password():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "sasl", "sasl": {"username": "x"}}
    with pytest.raises(ConfigError, match="irc.auth.sasl.password is required"):
        build_config(raw)


def test_build_config_auth_nickserv_mode_missing_nickserv_block():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "nickserv"}
    with pytest.raises(ConfigError, match="irc.auth.nickserv is required"):
        build_config(raw)


def test_build_config_auth_nickserv_block_not_a_mapping():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "nickserv", "nickserv": "oops"}
    with pytest.raises(ConfigError, match="irc.auth.nickserv must be a mapping"):
        build_config(raw)


def test_build_config_auth_nickserv_missing_password():
    raw = _base_config()
    raw["irc"]["auth"] = {"mode": "nickserv", "nickserv": {}}
    with pytest.raises(ConfigError, match="irc.auth.nickserv.password is required"):
        build_config(raw)


def test_build_config_auth_nickserv_negative_join_wait():
    raw = _base_config()
    raw["irc"]["auth"] = {
        "mode": "nickserv",
        "nickserv": {"password": "x", "join_wait_seconds": -1},
    }
    with pytest.raises(ConfigError, match="irc.auth.nickserv.join_wait_seconds must be >= 0"):
        build_config(raw)


def test_build_config_auth_nickserv_join_wait_not_a_number():
    raw = _base_config()
    raw["irc"]["auth"] = {
        "mode": "nickserv",
        "nickserv": {"password": "x", "join_wait_seconds": "soon"},
    }
    with pytest.raises(
        ConfigError, match="irc.auth.nickserv.join_wait_seconds must be a number"
    ):
        build_config(raw)


# ---------------------------------------------------------------------------
# channels errors
# ---------------------------------------------------------------------------


def test_build_config_channels_not_a_list():
    raw = _base_config()
    raw["channels"] = "oops"
    with pytest.raises(ConfigError, match="channels must be a list"):
        build_config(raw)


def test_build_config_channels_empty_list():
    raw = _base_config()
    raw["channels"] = []
    with pytest.raises(ConfigError, match="channels must contain at least one"):
        build_config(raw)


def test_build_config_channel_entry_not_a_mapping():
    raw = _base_config()
    raw["channels"] = ["oops"]
    with pytest.raises(ConfigError, match=r"channels\[0\] must be a mapping"):
        build_config(raw)


def test_build_config_channel_entry_missing_mesh_channel():
    raw = _base_config()
    raw["channels"] = [{"irc_channel": "#x"}]
    with pytest.raises(ConfigError, match=r"channels\[0\].mesh_channel is required"):
        build_config(raw)


def test_build_config_channel_entry_missing_irc_channel():
    raw = _base_config()
    raw["channels"] = [{"mesh_channel": 0}]
    with pytest.raises(ConfigError, match=r"channels\[0\].irc_channel is required"):
        build_config(raw)


def test_build_config_channel_mesh_channel_not_an_integer():
    raw = _base_config()
    raw["channels"] = [{"mesh_channel": "zero", "irc_channel": "#x"}]
    with pytest.raises(ConfigError, match=r"channels\[0\].mesh_channel must be an integer"):
        build_config(raw)


def test_build_config_channel_mesh_channel_negative():
    raw = _base_config()
    raw["channels"] = [{"mesh_channel": -1, "irc_channel": "#x"}]
    with pytest.raises(ConfigError, match="channels\\[\\].mesh_channel must be >= 0"):
        build_config(raw)


def test_build_config_channel_irc_channel_missing_prefix():
    raw = _base_config()
    raw["channels"] = [{"mesh_channel": 0, "irc_channel": "no-hash"}]
    with pytest.raises(ConfigError, match="must start with '#' or '&'"):
        build_config(raw)


def test_build_config_channel_irc_channel_empty():
    raw = _base_config()
    raw["channels"] = [{"mesh_channel": 0, "irc_channel": ""}]
    with pytest.raises(ConfigError, match="must start with '#' or '&'"):
        build_config(raw)


def test_build_config_duplicate_mesh_channel():
    raw = _base_config()
    raw["channels"] = [
        {"mesh_channel": 0, "irc_channel": "#a"},
        {"mesh_channel": 0, "irc_channel": "#b"},
    ]
    with pytest.raises(ConfigError, match="duplicate channels\\[\\].mesh_channel: 0"):
        build_config(raw)


# ---------------------------------------------------------------------------
# ${ENV_VAR} interpolation (exercised through load_config, the real entry point)
# ---------------------------------------------------------------------------


def test_load_config_interpolates_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("IRC_SASL_PASSWORD", "s3cr3t")
    raw = _base_config()
    raw["irc"]["auth"] = {
        "mode": "sasl",
        "sasl": {"username": "mynick", "password": "${IRC_SASL_PASSWORD}"},
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(raw))

    cfg = load_config(config_path)
    assert cfg.irc.auth.sasl.password == "s3cr3t"


def test_load_config_missing_env_var_is_config_error(tmp_path, monkeypatch):
    monkeypatch.delenv("IRC_SASL_PASSWORD", raising=False)
    raw = _base_config()
    raw["irc"]["auth"] = {
        "mode": "sasl",
        "sasl": {"username": "mynick", "password": "${IRC_SASL_PASSWORD}"},
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(raw))

    with pytest.raises(ConfigError, match="IRC_SASL_PASSWORD.*not set"):
        load_config(config_path)


def test_interpolate_env_handles_nested_structures(tmp_path, monkeypatch):
    monkeypatch.setenv("MESH_ADDR", "AA:BB:CC:DD:EE:FF")
    raw = _base_config()
    raw["mesh"]["connection"]["address"] = "${MESH_ADDR}"
    raw["channels"] = [
        {"mesh_channel": 0, "irc_channel": "#a"},
        {"mesh_channel": 1, "irc_channel": "#b"},
    ]
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(raw))

    cfg = load_config(config_path)
    assert cfg.mesh.connection.address == "AA:BB:CC:DD:EE:FF"
    assert cfg.irc_channel_for(1) == "#b"


def test_interpolate_env_passthrough_non_string_scalars():
    # ints/bools/None flow through build_config's interpolation path
    # untouched -- exercised directly here for the pure function's own sake.
    from meshcore_irc_bridge.config import _interpolate_env

    assert _interpolate_env(6697) == 6697
    assert _interpolate_env(True) is True
    assert _interpolate_env(None) is None


def test_interpolate_env_string_without_placeholder_is_unchanged():
    from meshcore_irc_bridge.config import _interpolate_env

    assert _interpolate_env("plain string") == "plain string"


# ---------------------------------------------------------------------------
# load_config file-handling
# ---------------------------------------------------------------------------


def test_load_config_missing_file(tmp_path):
    missing = tmp_path / "nope.yaml"
    with pytest.raises(ConfigError, match="could not read config file"):
        load_config(missing)


def test_load_config_invalid_yaml(tmp_path):
    bad = tmp_path / "config.yaml"
    bad.write_text("mesh: [unterminated")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(bad)


def test_load_config_top_level_not_a_mapping(tmp_path):
    bad = tmp_path / "config.yaml"
    bad.write_text("- just\n- a\n- list\n")
    with pytest.raises(ConfigError, match="must contain a top-level mapping"):
        load_config(bad)


def test_load_config_accepts_str_path(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_base_config()))
    cfg = load_config(str(config_path))
    assert cfg.irc.server == "irc.example.org"
