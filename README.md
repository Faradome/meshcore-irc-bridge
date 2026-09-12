# meshcore-irc-bridge

A one-way bridge: it connects to a [MeshCore](https://meshcore.co.uk/) companion
radio, listens for channel messages, and relays them into IRC channels — one
mesh channel mapped to one IRC channel. It never sends anything back to the
mesh; IRC messages are received and ignored.

Built on the [`meshcore`](https://pypi.org/project/meshcore/) Python package
for the radio side (BLE, serial, or TCP companion connection). The IRC side is
a small hand-rolled asyncio client with first-class
[IRCv3 SASL](https://ircv3.net/specs/extensions/sasl-3.1) support, a NickServ
`IDENTIFY` fallback (with a configurable wait before joining channels), and
support for connecting with a fully unregistered nickname.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"    # from a checkout, until this is published
```

## Configure

Copy [`config.example.yaml`](config.example.yaml) to `config.yaml` and edit it:

```bash
cp config.example.yaml config.yaml
```

- `mesh.connection` — how to reach your companion radio: `type: ble` (with
  `address`), `type: serial` (with `port`/`baudrate`), or `type: tcp` (with
  `host`/`port`).
- `irc` — the IRC server and how to authenticate. `irc.auth.mode` is one of:
  - `sasl` — authenticate with `AUTHENTICATE PLAIN` before registration
    completes. Requires `irc.auth.sasl.username`/`password`.
  - `nickserv` — connect without SASL, wait for the welcome (`001`), send
    `PRIVMSG NickServ :IDENTIFY <password>`, then wait
    `irc.auth.nickserv.join_wait_seconds` before joining channels (long
    enough for services to apply your cloak/account before you join gated
    channels). Requires `irc.auth.nickserv.password`.
  - `none` — connect with a fully unregistered nickname and join immediately.

  The auth mode is fixed by config — if it fails (e.g. the server doesn't
  offer SASL, or the password is rejected), the bridge logs the failure and
  retries the *same* mode on reconnect rather than silently switching
  methods.

  Secrets support `${ENV_VAR}` interpolation so passwords don't need to sit
  in the YAML file in plaintext, e.g. `password: "${IRC_SASL_PASSWORD}"`.

- `channels` — the mesh-channel-to-IRC-channel mapping, e.g.:

  ```yaml
  channels:
    - mesh_channel: 0
      irc_channel: "#mesh-general"
    - mesh_channel: 1
      irc_channel: "#mesh-emergency"
  ```

### Setting up a channel on the radio itself

This bridge only *reads* channel messages — it never creates, renames, or
rekeys a channel on the companion radio. To set one up (or check what's
already configured), use [`meshcorectl`](https://github.com/Faradome/meshcorectl),
a companion CLI for MeshCore radios:

```bash
meshcorectl create channel 0 "General"   # create/rename channel 0
meshcorectl get channels                  # list what's configured on the radio
```

## Run

```bash
meshcore-irc-bridge --config config.yaml
# or
python -m meshcore_irc_bridge --config config.yaml
```

`--log-level` (default `INFO`) controls verbosity.

## Development

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest                # 100% line+branch coverage enforced
ruff check .
mypy
```

Tests never touch real hardware or a real IRC network: the mesh side is
exercised against a hardware-free double of `meshcore.MeshCore`
(`tests/fakes/meshcore_double.py`), and the IRC client is exercised against a
real (loopback-only) asyncio TCP server that scripts IRC protocol exchanges
(`tests/fakes/fake_irc_server.py`).

Real end-to-end verification against an actual radio and IRC network is out
of scope for the automated test suite — run the bridge against your own
setup once installed to confirm it end-to-end.

## License

[MIT](LICENSE)
