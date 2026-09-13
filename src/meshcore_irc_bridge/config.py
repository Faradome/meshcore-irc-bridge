"""Bridge configuration: YAML load, validation, and ``${ENV_VAR}`` interpolation.

Plain dataclasses with validation in ``__post_init__`` and a hand-rolled YAML
loader -- no schema library dependency. Raises a clear ConfigError instead of
silently defaulting anything security sensitive.
"""

from __future__ import annotations

import logging
import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_VALID_MESH_KINDS = ("ble", "serial", "tcp")
_VALID_AUTH_MODES = ("sasl", "nickserv", "none")
_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# Generously below IRC's 512-byte line limit -- leaves headroom for
# "JOIN "/"PRIVMSG " plus the 400-byte content budget
# (formatting.MAX_LINE_BYTES) once combined with a destination this long,
# so a channel name can never on its own make every line sent to it too
# long to send (see ChannelMapping.__post_init__).
_MAX_IRC_CHANNEL_LEN = 50


class ConfigError(Exception):
    """Raised for any problem loading or validating the bridge config."""


@dataclass(frozen=True)
class MeshConnectionConfig:
    """How to reach the companion radio: exactly one of ble/serial/tcp.

    `port` is the serial device path; `tcp_port` is the TCP port number.
    """

    kind: str
    address: str | None = None  # ble
    port: str | None = None  # serial device path, e.g. /dev/ttyUSB0
    baudrate: int = 115200  # serial
    host: str | None = None  # tcp
    tcp_port: int = 5000  # tcp

    def __post_init__(self) -> None:
        if self.kind not in _VALID_MESH_KINDS:
            raise ConfigError(
                f"mesh.connection.type must be one of {_VALID_MESH_KINDS}, got {self.kind!r}"
            )
        if self.kind == "ble" and not self.address:
            raise ConfigError("mesh.connection.address is required when type is ble")
        if self.kind == "serial" and not self.port:
            raise ConfigError("mesh.connection.port is required when type is serial")
        if self.kind == "tcp" and not self.host:
            raise ConfigError("mesh.connection.host is required when type is tcp")


@dataclass(frozen=True)
class MeshConfig:
    """`auto_reconnect`/`max_reconnect_attempts` configure the `meshcore`
    library's *own* reconnect loop, which sits underneath this bridge's own
    (`bridge.py`'s `_run_mesh_supervisor`, exponential backoff from 1s up
    to 60s). On an unexpected disconnect, the library first retries up to
    `max_reconnect_attempts` times at its own flat ~1s interval (logged at
    DEBUG, not INFO) *before* it ever emits the event this bridge is
    watching for; only once the library gives up does this bridge's own
    supervisor take over. The two layers are independent and not meant to
    be tuned together -- this one bounds how hard the library retries a
    single drop, the bridge's own backoff is what actually governs
    long-outage behavior.
    """

    connection: MeshConnectionConfig
    auto_reconnect: bool = True
    max_reconnect_attempts: int = 5

    def __post_init__(self) -> None:
        if self.max_reconnect_attempts < 1:
            raise ConfigError("mesh.max_reconnect_attempts must be >= 1")


@dataclass(frozen=True)
class SaslConfig:
    username: str
    password: str

    def __post_init__(self) -> None:
        if not self.username:
            raise ConfigError("irc.auth.sasl.username is required when auth.mode is sasl")
        if not self.password:
            raise ConfigError("irc.auth.sasl.password is required when auth.mode is sasl")


@dataclass(frozen=True)
class NickservConfig:
    password: str
    join_wait_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not self.password:
            raise ConfigError(
                "irc.auth.nickserv.password is required when auth.mode is nickserv"
            )
        if self.join_wait_seconds < 0:
            raise ConfigError("irc.auth.nickserv.join_wait_seconds must be >= 0")


@dataclass(frozen=True)
class AuthConfig:
    mode: str
    sasl: SaslConfig | None = None
    nickserv: NickservConfig | None = None

    def __post_init__(self) -> None:
        if self.mode not in _VALID_AUTH_MODES:
            raise ConfigError(
                f"irc.auth.mode must be one of {_VALID_AUTH_MODES}, got {self.mode!r}"
            )
        if self.mode == "sasl" and self.sasl is None:
            raise ConfigError("irc.auth.sasl is required when auth.mode is sasl")
        if self.mode == "nickserv" and self.nickserv is None:
            raise ConfigError("irc.auth.nickserv is required when auth.mode is nickserv")


@dataclass(frozen=True)
class ReconnectConfig:
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.initial_delay_seconds <= 0:
            raise ConfigError("irc.reconnect.initial_delay_seconds must be > 0")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ConfigError(
                "irc.reconnect.max_delay_seconds must be >= initial_delay_seconds"
            )


@dataclass(frozen=True)
class IrcConfig:
    server: str
    port: int
    nickname: str
    auth: AuthConfig
    tls: bool = True
    username: str = ""
    realname: str = ""
    reconnect: ReconnectConfig = field(default_factory=ReconnectConfig)
    # A peer that goes away without closing the socket (netsplit, a
    # middlebox silently dropping the flow, the server process being
    # killed) is indistinguishable from an idle one: both produce nothing,
    # forever, and a plain blocking read never raises anything to trigger
    # a reconnect -- the OS can be left showing ESTABLISHED with no timer
    # armed. After `ping_idle_seconds` of silence the client sends its own
    # PING (a question the server must answer); if no byte at all arrives
    # within `ping_timeout_seconds` after that, the link is declared dead.
    # Worst-case detection time is ping_idle_seconds + ping_timeout_seconds.
    ping_idle_seconds: float = 120.0
    ping_timeout_seconds: float = 60.0
    # Minimum spacing between successive PRIVMSGs the queue-drain loop
    # sends. Without this, a burst -- a long multi-line mesh message
    # wrapped into many PRIVMSGs, or replaying everything queued during an
    # outage the moment IRC reconnects -- can exceed a server's own
    # flood-control thresholds, which then throttles or drops the
    # connection and turns one burst into a disconnect/reconnect/replay
    # loop. 0 disables pacing entirely.
    min_send_interval_seconds: float = 0.3

    def __post_init__(self) -> None:
        if not self.server:
            raise ConfigError("irc.server is required")
        if not (1 <= self.port <= 65535):
            raise ConfigError(f"irc.port must be between 1 and 65535, got {self.port}")
        if not self.nickname:
            raise ConfigError("irc.nickname is required")
        if not self.username:
            object.__setattr__(self, "username", self.nickname)
        if not self.realname:
            object.__setattr__(self, "realname", self.nickname)
        if self.ping_idle_seconds <= 0:
            raise ConfigError("irc.ping_idle_seconds must be > 0")
        if self.ping_timeout_seconds <= 0:
            raise ConfigError("irc.ping_timeout_seconds must be > 0")
        if self.min_send_interval_seconds < 0:
            raise ConfigError("irc.min_send_interval_seconds must be >= 0")
        if not self.tls and self.auth.mode in ("sasl", "nickserv"):
            # Silent until now: `_warn_if_world_or_group_readable` already
            # warns about secrets sitting readable on disk, but nothing
            # warned about them going out on the wire in the clear, which
            # is exactly what a plaintext `sasl`/`nickserv` connection does
            # (SASL PLAIN's base64 blob and IDENTIFY's password are both
            # trivially decodable/plaintext -- "encoded", not encrypted).
            logger.warning(
                "irc.tls is false while irc.auth.mode is %r: credentials will be sent "
                "unencrypted to %s:%d, readable by anyone on the network path",
                self.auth.mode,
                self.server,
                self.port,
            )


@dataclass(frozen=True)
class ChannelMapping:
    mesh_channel: int
    irc_channel: str

    def __post_init__(self) -> None:
        if self.mesh_channel < 0:
            raise ConfigError(f"channels[].mesh_channel must be >= 0, got {self.mesh_channel}")
        if not self.irc_channel or self.irc_channel[0] not in "#&":
            raise ConfigError(
                f"channels[].irc_channel must start with '#' or '&', got {self.irc_channel!r}"
            )
        # Both of these pass this far-too-permissive first check and then
        # wedge the bridge at runtime instead of failing fast here: a space
        # (or control character) makes format_line() reject every JOIN/
        # PRIVMSG for this channel forever (the IRC side can never become
        # ready, or -- for a channel that only breaks the longer PRIVMSG
        # line -- the same message is retried and fails every ~0.2s
        # indefinitely, head-of-line-blocking every other channel's queued
        # messages behind it). Reject both here instead, where a typo is
        # just a clear ConfigError at startup.
        if any(c in self.irc_channel for c in " \t\r\n\x00,"):
            raise ConfigError(
                "channels[].irc_channel must not contain whitespace, control "
                f"characters, or commas, got {self.irc_channel!r}"
            )
        if len(self.irc_channel) > _MAX_IRC_CHANNEL_LEN:
            raise ConfigError(
                f"channels[].irc_channel must be at most {_MAX_IRC_CHANNEL_LEN} "
                f"characters, got {len(self.irc_channel)}: {self.irc_channel!r}"
            )


@dataclass(frozen=True)
class BridgeConfig:
    mesh: MeshConfig
    irc: IrcConfig
    channels: tuple[ChannelMapping, ...]

    def __post_init__(self) -> None:
        if not self.channels:
            raise ConfigError(
                "channels must contain at least one mesh_channel -> irc_channel mapping"
            )
        seen: set[int] = set()
        for mapping in self.channels:
            if mapping.mesh_channel in seen:
                raise ConfigError(f"duplicate channels[].mesh_channel: {mapping.mesh_channel}")
            seen.add(mapping.mesh_channel)

    def irc_channel_for(self, mesh_channel: int) -> str | None:
        """Look up the configured IRC destination for a mesh channel index."""
        for mapping in self.channels:
            if mapping.mesh_channel == mesh_channel:
                return mapping.irc_channel
        return None


def _interpolate_env(value: Any) -> Any:
    """Recursively replace ``${VAR}`` in strings; a missing var is a ConfigError."""
    if isinstance(value, str):

        def _replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in os.environ:
                raise ConfigError(
                    f"config references environment variable {name!r}, which is not set"
                )
            return os.environ[name]

        return _ENV_VAR_PATTERN.sub(_replace, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(v) for v in value]
    return value


def _require(raw: Any, key: str, context: str) -> Any:
    if not isinstance(raw, dict):
        raise ConfigError(f"{context} must be a mapping")
    if key not in raw:
        raise ConfigError(f"{context}.{key} is required")
    return raw[key]


def _require_dict(raw: Any, key: str, context: str) -> dict[str, Any]:
    value = _require(raw, key, context)
    if not isinstance(value, dict):
        raise ConfigError(f"{context}.{key} must be a mapping")
    return value


def _coerce_int(value: Any, context: str) -> int:
    """Coerce a raw YAML value to `int`, raising `ConfigError` (not a bare
    `TypeError`/`ValueError`) on the wrong type -- e.g. a quoted `"6697"`
    string, `null`, or a mapping/list."""
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{context} must be an integer, got {value!r}") from exc


def _coerce_float(value: Any, context: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{context} must be a number, got {value!r}") from exc


def _build_mesh_config(raw: dict[str, Any]) -> MeshConfig:
    connection_raw = _require_dict(raw, "connection", "mesh")
    kind = _require(connection_raw, "type", "mesh.connection")

    # tcp's "port" key is a port *number*, serial's "port" key is a device
    # *path* -- both spelled `port` in YAML, disambiguated here by `kind`
    # before it reaches the dataclass's two separate fields (`port` /
    # `tcp_port`).
    tcp_port = 5000
    if kind == "tcp":
        tcp_port = _coerce_int(connection_raw.get("port", 5000), "mesh.connection.port")

    connection = MeshConnectionConfig(
        kind=kind,
        address=connection_raw.get("address"),
        port=connection_raw.get("port") if kind == "serial" else None,
        baudrate=_coerce_int(connection_raw.get("baudrate", 115200), "mesh.connection.baudrate"),
        host=connection_raw.get("host"),
        tcp_port=tcp_port,
    )
    return MeshConfig(
        connection=connection,
        auto_reconnect=raw.get("auto_reconnect", True),
        max_reconnect_attempts=_coerce_int(
            raw.get("max_reconnect_attempts", 5), "mesh.max_reconnect_attempts"
        ),
    )


def _build_auth_config(raw: dict[str, Any]) -> AuthConfig:
    mode = _require(raw, "mode", "irc.auth")
    sasl_raw = raw.get("sasl")
    sasl = None
    if sasl_raw is not None:
        if not isinstance(sasl_raw, dict):
            raise ConfigError("irc.auth.sasl must be a mapping")
        sasl = SaslConfig(
            username=sasl_raw.get("username", ""), password=sasl_raw.get("password", "")
        )
    nickserv_raw = raw.get("nickserv")
    nickserv = None
    if nickserv_raw is not None:
        if not isinstance(nickserv_raw, dict):
            raise ConfigError("irc.auth.nickserv must be a mapping")
        nickserv = NickservConfig(
            password=nickserv_raw.get("password", ""),
            join_wait_seconds=_coerce_float(
                nickserv_raw.get("join_wait_seconds", 5.0), "irc.auth.nickserv.join_wait_seconds"
            ),
        )
    return AuthConfig(mode=mode, sasl=sasl, nickserv=nickserv)


def _build_reconnect_config(raw: dict[str, Any] | None) -> ReconnectConfig:
    if raw is None:
        return ReconnectConfig()
    if not isinstance(raw, dict):
        raise ConfigError("irc.reconnect must be a mapping")
    return ReconnectConfig(
        initial_delay_seconds=_coerce_float(
            raw.get("initial_delay_seconds", 1.0), "irc.reconnect.initial_delay_seconds"
        ),
        max_delay_seconds=_coerce_float(
            raw.get("max_delay_seconds", 60.0), "irc.reconnect.max_delay_seconds"
        ),
    )


def _build_irc_config(raw: dict[str, Any]) -> IrcConfig:
    auth_raw = _require_dict(raw, "auth", "irc")
    return IrcConfig(
        server=_require(raw, "server", "irc"),
        port=_coerce_int(_require(raw, "port", "irc"), "irc.port"),
        nickname=_require(raw, "nickname", "irc"),
        auth=_build_auth_config(auth_raw),
        tls=raw.get("tls", True),
        username=raw.get("username", ""),
        realname=raw.get("realname", ""),
        reconnect=_build_reconnect_config(raw.get("reconnect")),
        ping_idle_seconds=_coerce_float(
            raw.get("ping_idle_seconds", 120.0), "irc.ping_idle_seconds"
        ),
        ping_timeout_seconds=_coerce_float(
            raw.get("ping_timeout_seconds", 60.0), "irc.ping_timeout_seconds"
        ),
        min_send_interval_seconds=_coerce_float(
            raw.get("min_send_interval_seconds", 0.3), "irc.min_send_interval_seconds"
        ),
    )


def _build_channels(raw: Any) -> tuple[ChannelMapping, ...]:
    if not isinstance(raw, list):
        raise ConfigError("channels must be a list")
    mappings = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            raise ConfigError(f"channels[{i}] must be a mapping")
        mappings.append(
            ChannelMapping(
                mesh_channel=_coerce_int(
                    _require(entry, "mesh_channel", f"channels[{i}]"),
                    f"channels[{i}].mesh_channel",
                ),
                irc_channel=_require(entry, "irc_channel", f"channels[{i}]"),
            )
        )
    return tuple(mappings)


def build_config(raw: dict[str, Any]) -> BridgeConfig:
    """Build and validate a `BridgeConfig` from an already-parsed dict.

    Split out from `load_config` so tests can exercise validation without
    touching the filesystem.
    """
    mesh_raw = _require_dict(raw, "mesh", "<config>")
    irc_raw = _require_dict(raw, "irc", "<config>")
    channels_raw = _require(raw, "channels", "<config>")
    return BridgeConfig(
        mesh=_build_mesh_config(mesh_raw),
        irc=_build_irc_config(irc_raw),
        channels=_build_channels(channels_raw),
    )


def _warn_if_world_or_group_readable(config_path: Path) -> None:
    """Log a warning if `config_path` grants group/other read access.

    The documented setup (`cp config.example.yaml config.yaml`) creates the
    file under the process umask -- typically world-readable (e.g. 0644) --
    and a config commonly holds plaintext SASL/NickServ passwords for
    anyone who skips the `${ENV_VAR}` interpolation option. Best-effort
    only -- never raises if the mode can't be read at all.
    """
    try:
        mode = config_path.stat().st_mode
    except OSError:
        return
    if mode & (stat.S_IRGRP | stat.S_IROTH):
        logger.warning(
            "config file %s is readable by group/other (mode %04o); if it "
            "contains plaintext secrets (SASL/NickServ passwords), run "
            "`chmod 600 %s` or use ${ENV_VAR} interpolation instead",
            config_path,
            stat.S_IMODE(mode),
            config_path,
        )


def load_config(path: str | Path) -> BridgeConfig:
    """Load, interpolate, and validate a bridge config file."""
    config_path = Path(path)
    try:
        raw_text = config_path.read_text()
    except OSError as exc:
        raise ConfigError(f"could not read config file {config_path}: {exc}") from exc

    _warn_if_world_or_group_readable(config_path)

    try:
        raw = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"config file {config_path} is not valid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ConfigError(f"config file {config_path} must contain a top-level mapping")

    interpolated = _interpolate_env(raw)
    return build_config(interpolated)
