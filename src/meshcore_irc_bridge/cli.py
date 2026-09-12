"""Command-line entrypoint: `meshcore-irc-bridge --config config.yaml`."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from .bridge import Bridge
from .config import ConfigError, load_config

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="meshcore-irc-bridge",
        description=(
            "One-way bridge: relays MeshCore companion radio channel messages "
            "into IRC channels. IRC traffic is never relayed back to the mesh."
        ),
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="path to the bridge config YAML file (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="logging verbosity (default: %(default)s)",
    )
    return parser.parse_args(argv)


async def _run(bridge: Bridge) -> None:
    """Run `bridge` until SIGINT/SIGTERM (or `KeyboardInterrupt` on
    platforms without signal-handler support, e.g. Windows) requests a
    clean stop."""
    loop = asyncio.get_running_loop()
    stop_requested = False

    def _handle_signal() -> None:
        nonlocal stop_requested
        if stop_requested:
            return
        stop_requested = True
        logger.info("shutdown requested")
        asyncio.ensure_future(bridge.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # e.g. Windows: Ctrl+C still raises KeyboardInterrupt, which
            # propagates out of asyncio.run() normally -- see main()'s own
            # KeyboardInterrupt handling.
            pass

    await bridge.run()


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        # `meshcore`'s own __init__.py calls logging.basicConfig() at
        # import time (before this ever runs, since bridge.py imports
        # from meshcore at module scope) -- without force=True this call
        # is a silent no-op (per logging.basicConfig's own documented
        # behavior when the root logger already has handlers), so
        # --log-level and this format string would never actually apply.
        force=True,
    )

    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"meshcore-irc-bridge: {exc}", file=sys.stderr)
        return 1

    bridge = Bridge(config)
    try:
        asyncio.run(_run(bridge))
    except KeyboardInterrupt:
        return 0
    return 0
