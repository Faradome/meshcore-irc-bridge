from __future__ import annotations

import asyncio
import logging
import signal
import subprocess
import sys

import pytest
import yaml

from meshcore_irc_bridge import cli


def _valid_config_dict() -> dict:
    return {
        "mesh": {"connection": {"type": "ble", "address": "AA:BB:CC:DD:EE:FF"}},
        "irc": {
            "server": "irc.example.org",
            "port": 6697,
            "nickname": "meshbot",
            "auth": {"mode": "none"},
        },
        "channels": [{"mesh_channel": 0, "irc_channel": "#general"}],
    }


# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------


def test_parse_args_defaults():
    args = cli._parse_args([])
    assert args.config == "config.yaml"
    assert args.log_level == "INFO"


def test_parse_args_overrides():
    args = cli._parse_args(["--config", "foo.yaml", "--log-level", "DEBUG"])
    assert args.config == "foo.yaml"
    assert args.log_level == "DEBUG"


def test_parse_args_rejects_unknown_log_level():
    with pytest.raises(SystemExit):
        cli._parse_args(["--log-level", "VERY_LOUD"])


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------


def test_main_returns_1_on_config_error(tmp_path, capsys):
    missing = tmp_path / "nope.yaml"
    assert cli.main(["--config", str(missing)]) == 1
    captured = capsys.readouterr()
    assert "meshcore-irc-bridge:" in captured.err
    assert "could not read config file" in captured.err


def test_main_runs_bridge_and_returns_0(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_valid_config_dict()))

    built: list[object] = []

    class FakeBridge:
        def __init__(self, config):
            built.append(config)

        async def run(self):
            return

    monkeypatch.setattr(cli, "Bridge", FakeBridge)

    assert cli.main(["--config", str(config_path)]) == 0
    assert len(built) == 1


def test_main_applies_requested_log_level(tmp_path, monkeypatch):
    # meshcore's own __init__.py calls logging.basicConfig() at import
    # time (before main() ever runs, since bridge.py imports from
    # meshcore at module scope) -- without force=True, main()'s own
    # basicConfig() call would silently no-op and --log-level would never
    # actually take effect.
    root = logging.getLogger()
    saved_level, saved_handlers = root.level, root.handlers[:]
    try:
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.safe_dump(_valid_config_dict()))

        class FakeBridge:
            def __init__(self, config):
                pass

            async def run(self):
                return

        monkeypatch.setattr(cli, "Bridge", FakeBridge)

        assert cli.main(["--config", str(config_path), "--log-level", "DEBUG"]) == 0
        assert root.level == logging.DEBUG
    finally:
        root.setLevel(saved_level)
        root.handlers[:] = saved_handlers


def test_main_handles_keyboard_interrupt(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_valid_config_dict()))

    class FakeBridge:
        def __init__(self, config):
            pass

        async def run(self):
            raise KeyboardInterrupt

    monkeypatch.setattr(cli, "Bridge", FakeBridge)

    assert cli.main(["--config", str(config_path)]) == 0


# ---------------------------------------------------------------------------
# _run(): signal handling
# ---------------------------------------------------------------------------


async def test_run_registers_signal_handlers_that_call_stop(monkeypatch):
    handlers = {}

    def fake_add_signal_handler(sig, callback):
        handlers[sig] = callback

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", fake_add_signal_handler)

    stop_calls = []

    class FakeBridge:
        async def run(self):
            handlers[signal.SIGINT]()
            handlers[signal.SIGINT]()  # a second signal must be a no-op
            await asyncio.sleep(0.01)  # let the scheduled stop() task run

        async def stop(self):
            stop_calls.append(1)

    await cli._run(FakeBridge())

    assert stop_calls == [1]
    assert set(handlers) == {signal.SIGINT, signal.SIGTERM}


def test_python_dash_m_entrypoint_shows_help():
    # Exercises __main__.py's `if __name__ == "__main__":` guard for real
    # (import-time coverage of a module alone can't reach it) -- `--help`
    # exits before touching any config/hardware, so it's a fast, safe
    # subprocess call.
    result = subprocess.run(
        [sys.executable, "-m", "meshcore_irc_bridge", "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert "meshcore-irc-bridge" in result.stdout


async def test_run_tolerates_platforms_without_signal_handler_support(monkeypatch):
    loop = asyncio.get_running_loop()

    def raise_not_implemented(sig, callback):
        raise NotImplementedError

    monkeypatch.setattr(loop, "add_signal_handler", raise_not_implemented)

    ran = []

    class FakeBridge:
        async def run(self):
            ran.append(1)

    await cli._run(FakeBridge())

    assert ran == [1]
