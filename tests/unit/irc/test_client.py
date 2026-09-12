"""IRCClient tests, driven over a real (loopback-only) socket against
FakeIRCServer -- real line framing and real registration round-trips, no
mocking of IRCClient's internals.
"""

from __future__ import annotations

import asyncio
import base64

import pytest

from meshcore_irc_bridge.config import AuthConfig, IrcConfig, NickservConfig, SaslConfig
from meshcore_irc_bridge.irc.client import IRCAuthError, IRCClient, IRCConnectionClosed
from tests.fakes.fake_irc_server import FakeIRCConnection, FakeIRCServer


def make_config(server: FakeIRCServer, **overrides) -> IrcConfig:
    defaults: dict = dict(
        server=server.host,
        port=server.port,
        nickname="meshbot",
        auth=AuthConfig(mode="none"),
        tls=False,
        username="mesh",
        realname="Mesh Bridge",
    )
    defaults.update(overrides)
    return IrcConfig(**defaults)


@pytest.fixture
async def server():
    srv = FakeIRCServer()
    await srv.start()
    try:
        yield srv
    finally:
        await srv.stop()


async def start_client(
    server: FakeIRCServer, config: IrcConfig, channels=("#general",), **client_kwargs
):
    """Create the client, run it as a background task, and hand back the
    server's view of the connection once it arrives."""
    client = IRCClient(config, channels, **client_kwargs)
    task = asyncio.create_task(client.run_until_disconnected())
    conn = await server.accept()
    return client, task, conn


async def _drain_registration_preamble(conn: FakeIRCConnection) -> None:
    assert await conn.recv_line() == "CAP LS 302"
    assert await conn.recv_line() == "NICK meshbot"
    assert await conn.recv_line() == "USER mesh 0 * :Mesh Bridge"


# ---------------------------------------------------------------------------
# SASL
# ---------------------------------------------------------------------------


async def test_sasl_success_then_joins_channels(server):
    config = make_config(
        server,
        auth=AuthConfig(mode="sasl", sasl=SaslConfig(username="myacct", password="hunter2")),
    )
    client, task, conn = await start_client(server, config, channels=["#a", "#b"])

    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example CAP meshbot LS :sasl=PLAIN multi-prefix")
    assert await conn.recv_line() == "CAP REQ :sasl"
    await conn.send_line(":irc.example CAP meshbot ACK :sasl")
    assert await conn.recv_line() == "AUTHENTICATE PLAIN"
    await conn.send_line("AUTHENTICATE +")

    auth_line = await conn.recv_line()
    assert auth_line.startswith("AUTHENTICATE ")
    blob = auth_line.removeprefix("AUTHENTICATE ")
    decoded = base64.b64decode(blob)
    assert decoded == b"\0myacct\0hunter2"

    await conn.send_line(":irc.example 903 meshbot :SASL authentication successful")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")

    assert await conn.recv_line() == "JOIN #a"
    assert await conn.recv_line() == "JOIN #b"
    await asyncio.sleep(0)  # let run_until_disconnected() set is_ready before we check it
    assert client.is_ready is True

    await client.stop()
    await task


async def test_sasl_cap_ls_continuation_lines_are_accumulated(server):
    config = make_config(
        server,
        auth=AuthConfig(mode="sasl", sasl=SaslConfig(username="a", password="b")),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)

    await conn.send_line(":irc.example CAP meshbot LS * :multi-prefix")
    await conn.send_line(":irc.example CAP meshbot LS :sasl=PLAIN")
    assert await conn.recv_line() == "CAP REQ :sasl"

    await conn.send_line(":irc.example CAP meshbot ACK :sasl")
    assert await conn.recv_line() == "AUTHENTICATE PLAIN"
    await conn.send_line("AUTHENTICATE +")
    await conn.recv_line()  # the base64 blob
    await conn.send_line(":irc.example 903 meshbot :ok")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")
    assert await conn.recv_line() == "JOIN #general"

    await client.stop()
    await task


async def test_sasl_not_advertised_is_a_hard_failure(server):
    config = make_config(
        server,
        auth=AuthConfig(mode="sasl", sasl=SaslConfig(username="a", password="b")),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)

    await conn.send_line(":irc.example CAP meshbot LS :multi-prefix away-notify")
    assert await conn.recv_line() == "CAP END"

    with pytest.raises(IRCAuthError, match="does not advertise"):
        await task


async def test_sasl_rejected_with_nak(server):
    config = make_config(
        server,
        auth=AuthConfig(mode="sasl", sasl=SaslConfig(username="a", password="b")),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)

    await conn.send_line(":irc.example CAP meshbot LS :sasl")
    assert await conn.recv_line() == "CAP REQ :sasl"
    await conn.send_line(":irc.example CAP meshbot NAK :sasl")
    assert await conn.recv_line() == "CAP END"

    with pytest.raises(IRCAuthError, match="rejected"):
        await task


async def test_sasl_authentication_failure_numeric(server):
    config = make_config(
        server,
        auth=AuthConfig(mode="sasl", sasl=SaslConfig(username="a", password="wrong")),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)

    await conn.send_line(":irc.example CAP meshbot LS :sasl")
    await conn.recv_line()  # CAP REQ :sasl
    await conn.send_line(":irc.example CAP meshbot ACK :sasl")
    await conn.recv_line()  # AUTHENTICATE PLAIN
    await conn.send_line("AUTHENTICATE +")
    await conn.recv_line()  # base64 blob
    await conn.send_line(":irc.example 904 meshbot :SASL authentication failed")
    assert await conn.recv_line() == "CAP END"

    with pytest.raises(IRCAuthError, match="904"):
        await task


async def test_sasl_mode_against_server_with_no_cap_support_is_a_hard_failure(server):
    # A pre-IRCv3 server that just ignores "CAP LS 302" entirely and
    # registers the client straight away must not be silently accepted
    # when SASL was explicitly required.
    config = make_config(
        server,
        auth=AuthConfig(mode="sasl", sasl=SaslConfig(username="a", password="b")),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example 001 meshbot :Welcome")

    with pytest.raises(IRCAuthError, match="without SASL negotiation"):
        await task


# ---------------------------------------------------------------------------
# nickserv
# ---------------------------------------------------------------------------


async def test_nickserv_identify_then_join_wait_then_join(server):
    config = make_config(
        server,
        auth=AuthConfig(
            mode="nickserv",
            nickserv=NickservConfig(password="hunter2", join_wait_seconds=0.15),
        ),
    )
    client, task, conn = await start_client(server, config, channels=["#a"])
    await _drain_registration_preamble(conn)

    await conn.send_line(":irc.example CAP meshbot LS :sasl")  # ignored; mode isn't sasl
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")

    assert await conn.recv_line() == "PRIVMSG NickServ :IDENTIFY hunter2"

    # A PING during the join-wait must still be answered promptly, proving
    # the wait doesn't blind-sleep through the server's keepalive.
    await conn.send_line("PING :duringwait")
    assert await conn.recv_line() == "PONG :duringwait"

    assert await conn.recv_line() == "JOIN #a"

    await client.stop()
    await task


async def test_nickserv_missing_replies_still_joins_after_the_wait(server):
    config = make_config(
        server,
        auth=AuthConfig(
            mode="nickserv", nickserv=NickservConfig(password="x", join_wait_seconds=0.05)
        ),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example 001 meshbot :Welcome")
    await conn.recv_line()  # PRIVMSG NickServ :IDENTIFY x

    # We deliberately never reply as NickServ -- the join-wait timeout,
    # not a matched reply, is what unblocks the join.
    assert await conn.recv_line() == "JOIN #general"

    await client.stop()
    await task


# ---------------------------------------------------------------------------
# unregistered ("none")
# ---------------------------------------------------------------------------


async def test_unregistered_mode_joins_immediately_after_welcome(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await start_client(server, config, channels=["#x", "#x", "#y"])
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example CAP meshbot LS :")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")

    # duplicate channels are de-duplicated, order preserved
    assert await conn.recv_line() == "JOIN #x"
    assert await conn.recv_line() == "JOIN #y"

    await asyncio.sleep(0)
    assert client.is_ready is True

    await client.stop()
    await task


# ---------------------------------------------------------------------------
# nick-in-use handling (applies regardless of auth mode)
# ---------------------------------------------------------------------------


async def test_nick_in_use_retries_with_underscore_then_succeeds(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await start_client(server, config)
    assert await conn.recv_line() == "CAP LS 302"
    assert await conn.recv_line() == "NICK meshbot"
    await conn.recv_line()  # USER ...

    await conn.send_line(":irc.example 433 * meshbot :Nickname is already in use")
    assert await conn.recv_line() == "NICK meshbot_"
    await conn.send_line(":irc.example 001 meshbot_ :Welcome")
    # CAP LS was never answered in this test, so registration completes via
    # 001 alone (fine for auth.mode "none") straight into joining.
    assert await conn.recv_line() == "JOIN #general"

    await client.stop()
    await task


async def test_nick_in_use_exhausts_retries(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await start_client(server, config)
    await conn.recv_line()  # CAP LS 302
    nick = "meshbot"
    await conn.recv_line()  # NICK meshbot
    await conn.recv_line()  # USER ...

    for _ in range(6):
        await conn.send_line(f":irc.example 433 * {nick} :Nickname is already in use")
        nick += "_"

    with pytest.raises(IRCAuthError, match="retries were exhausted"):
        await task


# ---------------------------------------------------------------------------
# registration edge cases
# ---------------------------------------------------------------------------


async def test_registration_responds_to_ping(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)

    await conn.send_line("PING :abc123")
    assert await conn.recv_line() == "PONG :abc123"

    await conn.send_line(":irc.example CAP meshbot LS :")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :hi")
    await conn.recv_line()  # JOIN

    await client.stop()
    await task


async def test_registration_error_command_raises(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)

    await conn.send_line("ERROR :Closing link: throttled")

    with pytest.raises(IRCAuthError, match="Closing link"):
        await task


async def test_registration_timeout(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, _conn = await start_client(server, config, registration_timeout=0.05)

    with pytest.raises(IRCAuthError, match="timed out"):
        await task


async def test_registration_ignores_unrecognized_command(server):
    # e.g. a pre-registration NOTICE ("*** Looking up your hostname...")
    # matches none of the handled commands and must simply be skipped.
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)

    await conn.send_line(":irc.example NOTICE * :*** Looking up your hostname...")
    await conn.send_line(":irc.example CAP meshbot LS :")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :hi")
    await conn.recv_line()  # JOIN

    await client.stop()
    await task


async def test_registration_ignores_unhandled_cap_subcommand(server):
    # cap-notify servers can send unsolicited "CAP <nick> NEW/DEL :..."
    # lines outside of the LS/ACK/NAK exchange this client drives.
    config = make_config(
        server,
        auth=AuthConfig(mode="sasl", sasl=SaslConfig(username="a", password="b")),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)

    await conn.send_line(":irc.example CAP meshbot NEW :account-notify")
    await conn.send_line(":irc.example CAP meshbot LS :sasl")
    assert await conn.recv_line() == "CAP REQ :sasl"
    await conn.send_line(":irc.example CAP meshbot ACK :sasl")
    await conn.recv_line()  # AUTHENTICATE PLAIN
    await conn.send_line("AUTHENTICATE +")
    await conn.recv_line()  # base64 blob
    await conn.send_line(":irc.example 903 meshbot :ok")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :hi")
    await conn.recv_line()  # JOIN

    await client.stop()
    await task


async def test_registration_ignores_spurious_authenticate_payload(server):
    # Only "AUTHENTICATE +" (the continue prompt) triggers our response;
    # anything else on that command is simply ignored.
    config = make_config(
        server,
        auth=AuthConfig(mode="sasl", sasl=SaslConfig(username="a", password="b")),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example CAP meshbot LS :sasl")
    assert await conn.recv_line() == "CAP REQ :sasl"
    await conn.send_line(":irc.example CAP meshbot ACK :sasl")
    await conn.recv_line()  # AUTHENTICATE PLAIN

    await conn.send_line("AUTHENTICATE somejunk")
    await conn.send_line("AUTHENTICATE +")
    await conn.recv_line()  # base64 blob
    await conn.send_line(":irc.example 903 meshbot :ok")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :hi")
    await conn.recv_line()  # JOIN

    await client.stop()
    await task


# ---------------------------------------------------------------------------
# steady state: keepalive, dropped inbound PRIVMSG, sending, disconnects
# ---------------------------------------------------------------------------


async def _get_to_ready(server, config, channels=("#general",), **kwargs):
    client, task, conn = await start_client(server, config, channels, **kwargs)
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example CAP meshbot LS :")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")
    for channel in dict.fromkeys(channels):
        assert await conn.recv_line() == f"JOIN {channel}"
    await asyncio.sleep(0)
    assert client.is_ready is True
    return client, task, conn


async def test_steady_state_answers_ping(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await _get_to_ready(server, config)

    await conn.send_line("PING :keepalive")
    assert await conn.recv_line() == "PONG :keepalive"

    await client.stop()
    await task


async def test_steady_state_drops_inbound_privmsg(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await _get_to_ready(server, config)

    await conn.send_line(":someone!u@h PRIVMSG meshbot :please relay this to the mesh")
    # Proven not to have crashed or replied by the fact a later PING still
    # gets answered normally.
    await conn.send_line("PING :still-alive")
    assert await conn.recv_line() == "PONG :still-alive"

    await client.stop()
    await task


async def test_send_privmsg_once_ready(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await _get_to_ready(server, config)

    await client.send_privmsg("#general", "hello from the mesh")
    assert await conn.recv_line() == "PRIVMSG #general :hello from the mesh"

    await client.stop()
    await task


async def test_send_privmsg_before_ready_raises():
    config = IrcConfig(
        server="127.0.0.1",
        port=1,
        nickname="meshbot",
        auth=AuthConfig(mode="none"),
        tls=False,
    )
    client = IRCClient(config, ["#general"])
    with pytest.raises(IRCConnectionClosed):
        await client.send_privmsg("#general", "hi")


async def test_stop_before_ever_connecting_is_a_noop():
    config = IrcConfig(
        server="127.0.0.1",
        port=1,
        nickname="meshbot",
        auth=AuthConfig(mode="none"),
        tls=False,
    )
    client = IRCClient(config, ["#general"])
    await client.stop()  # must not raise even though nothing ever connected
    assert client.is_ready is False


async def test_wait_while_handling_pings_returns_immediately_if_already_stopping():
    # A `stop()` racing in right as the NickServ join-wait begins must not
    # block for the full wait duration.
    config = IrcConfig(
        server="127.0.0.1",
        port=1,
        nickname="meshbot",
        auth=AuthConfig(mode="none"),
        tls=False,
    )
    client = IRCClient(config, ["#general"])
    client._stopping = True
    await asyncio.wait_for(client._wait_while_handling_pings(10), timeout=1)


async def test_nickserv_join_wait_ignores_non_ping_traffic(server):
    config = make_config(
        server,
        auth=AuthConfig(
            mode="nickserv", nickserv=NickservConfig(password="x", join_wait_seconds=0.2)
        ),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example CAP meshbot LS :")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")
    await conn.recv_line()  # PRIVMSG NickServ :IDENTIFY x

    await conn.send_line(":NickServ!services@services NOTICE meshbot :You are now identified")
    assert await conn.recv_line() == "JOIN #general"

    await client.stop()
    await task


async def test_unexpected_disconnect_during_steady_state_raises(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, conn = await _get_to_ready(server, config)

    conn.close()

    with pytest.raises(IRCConnectionClosed):
        await task
    assert client.is_ready is False


async def test_stop_returns_cleanly_from_steady_state(server):
    config = make_config(server, auth=AuthConfig(mode="none"))
    client, task, _conn = await _get_to_ready(server, config)

    await client.stop()
    await task  # must not raise
    assert client.is_ready is False


async def test_stop_during_nickserv_join_wait_returns_cleanly(server):
    # stop() closes the connection to unblock the wait; run_until_disconnected()
    # must return cleanly (no exception, and no attempt to JOIN on the now-
    # closed connection) rather than treating that as an unexpected drop.
    config = make_config(
        server,
        auth=AuthConfig(
            mode="nickserv", nickserv=NickservConfig(password="x", join_wait_seconds=5)
        ),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example CAP meshbot LS :")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")
    await conn.recv_line()  # PRIVMSG NickServ :IDENTIFY x

    await client.stop()
    await task  # must not raise
    assert client.is_ready is False


async def test_run_session_skips_join_when_stopped_during_the_wait(server):
    # Distinct from test_stop_during_nickserv_join_wait_returns_cleanly:
    # here `_stopping` flips true without the connection being closed, so
    # _wait_while_handling_pings falls out via its own deadline (not an
    # interrupted read) and _run_session must notice `_stopping` itself
    # before ever attempting to JOIN on what the caller considers a
    # cancelled session.
    config = make_config(
        server,
        auth=AuthConfig(
            mode="nickserv", nickserv=NickservConfig(password="x", join_wait_seconds=0.05)
        ),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example CAP meshbot LS :")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")
    await conn.recv_line()  # PRIVMSG NickServ :IDENTIFY x

    client._stopping = True

    await task  # returns cleanly; never sends JOIN
    assert client.is_ready is False


async def test_unexpected_disconnect_during_nickserv_join_wait_raises(server):
    config = make_config(
        server,
        auth=AuthConfig(
            mode="nickserv", nickserv=NickservConfig(password="x", join_wait_seconds=5)
        ),
    )
    client, task, conn = await start_client(server, config)
    await _drain_registration_preamble(conn)
    await conn.send_line(":irc.example CAP meshbot LS :")
    assert await conn.recv_line() == "CAP END"
    await conn.send_line(":irc.example 001 meshbot :Welcome")
    await conn.recv_line()  # PRIVMSG NickServ :IDENTIFY x

    conn.close()

    with pytest.raises(IRCConnectionClosed):
        await task
