"""Run events over HTTP: the flows POST each `PipelineRunEvent` to every relay URL.

The relays take no token (their boundary is the network), so the POST carries none.
Each URL is tried on its own, with one retry; one relay down or hanging never keeps
the event from the others, and nothing here ever raises into a Prefect hook.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

pytest.importorskip("httpx")
pytest.importorskip("celine.sdk.posture")

from celine.sdk.broker import PipelineRunEvent

from celine.utils.pipelines import context as context_module
from celine.utils.pipelines.context import flow_hooks
from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.pipeline_result import PipelineStatus
from celine.utils.pipelines.run_events import (
    build_run_event,
    emit_run_event,
    post_run_event,
)

EVENT_VARS = (
    "RUN_EVENTS_URLS",
    "RUN_EVENTS_TIMEOUT",
    "MQTT_EVENTS_ENABLED",
    "CELINE_ENV",
    "ENVIRONMENT",
    "CELINE_OIDC_CLIENT_SECRET",
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch, tmp_path):
    for name in EVENT_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CELINE_ENV", "dev")
    # AppBaseSettings reads .env files from the working directory.
    monkeypatch.chdir(tmp_path)


# ---------------------------------------------------------------------------
# Stub relays
# ---------------------------------------------------------------------------


class Relay:
    """A local HTTP server that records what it receives.

    ``statuses`` is the sequence of status codes it answers, the last one
    repeating. ``delay`` makes every answer wait that many seconds.
    """

    def __init__(self, statuses=(202,), delay: float = 0.0):
        self.statuses = list(statuses)
        self.delay = delay
        self.requests: list[dict] = []
        relay = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length)
                relay.requests.append(
                    {
                        "path": self.path,
                        "headers": dict(self.headers),
                        "body": json.loads(body),
                    }
                )
                if relay.delay:
                    time.sleep(relay.delay)
                status = (
                    relay.statuses.pop(0)
                    if len(relay.statuses) > 1
                    else relay.statuses[0]
                )
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/events"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def relays():
    made: list[Relay] = []

    def make(statuses=(202,), delay: float = 0.0) -> Relay:
        r = Relay(statuses, delay)
        made.append(r)
        return r

    yield make
    for r in made:
        r.close()


def a_closed_port_url() -> str:
    r = Relay()
    url = r.url
    r.close()
    return url


def an_event(status=PipelineStatus.COMPLETED) -> PipelineRunEvent:
    return build_run_event(
        namespace="demo3",
        flow="meters-flow",
        status=status,
        run_id="11111111-2222-3333-4444-555555555555",
        duration_ms=1234,
    )


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def test_urls_are_read_comma_separated(monkeypatch):
    monkeypatch.setenv(
        "RUN_EVENTS_URLS", "http://relay-prod:8080/events, https://relay-staging/events"
    )

    cfg = PipelineConfig()

    assert cfg.run_events_urls == [
        "http://relay-prod:8080/events",
        "https://relay-staging/events",
    ]


def test_no_urls_means_an_empty_list():
    assert PipelineConfig().run_events_urls == []


def test_an_empty_string_means_an_empty_list(monkeypatch):
    monkeypatch.setenv("RUN_EVENTS_URLS", "")

    assert PipelineConfig().run_events_urls == []


@pytest.mark.parametrize(
    "value",
    [
        "http://a/events,,http://b/events",
        "relay-prod:8080/events",
        "ftp://relay/events",
    ],
)
def test_a_malformed_url_list_is_refused_at_load(monkeypatch, value):
    monkeypatch.setenv("RUN_EVENTS_URLS", value)

    with pytest.raises(ValueError, match="RUN_EVENTS_URLS"):
        PipelineConfig()


def test_the_timeout_defaults_to_five_seconds():
    assert PipelineConfig().run_events_timeout == 5.0


def test_get_as_envs_writes_the_urls_back_comma_separated(monkeypatch):
    monkeypatch.setenv("RUN_EVENTS_URLS", "http://a/events,http://b/events")

    envs = PipelineConfig.get_as_envs(PipelineConfig())

    assert envs["RUN_EVENTS_URLS"] == "http://a/events,http://b/events"


def test_get_as_envs_leaves_out_an_empty_url_list():
    assert "RUN_EVENTS_URLS" not in PipelineConfig.get_as_envs(PipelineConfig())


# ---------------------------------------------------------------------------
# post_run_event
# ---------------------------------------------------------------------------


def test_the_body_is_the_event_field_for_field(relays):
    relay = relays()
    event = an_event()

    post_run_event(event, [relay.url], timeout=2)

    assert relay.requests[0]["body"] == event.model_dump(mode="json")
    assert PipelineRunEvent.model_validate(relay.requests[0]["body"]) == event


def test_no_authorization_header_is_sent(relays):
    relay = relays()

    post_run_event(an_event(), [relay.url], timeout=2)

    assert "Authorization" not in relay.requests[0]["headers"]


def test_every_url_receives_the_event_once(relays):
    a, b = relays(), relays()

    delivered = post_run_event(an_event(), [a.url, b.url], timeout=2)

    assert len(a.requests) == 1 and len(b.requests) == 1
    assert sorted(delivered) == sorted([a.url, b.url])


def test_a_5xx_is_retried_once(relays):
    relay = relays(statuses=(503, 202))

    delivered = post_run_event(an_event(), [relay.url], timeout=2)

    assert len(relay.requests) == 2
    assert delivered == [relay.url]


def test_a_url_that_fails_twice_is_not_delivered(relays):
    relay = relays(statuses=(500,))

    delivered = post_run_event(an_event(), [relay.url], timeout=2)

    assert len(relay.requests) == 2
    assert delivered == []


def test_a_4xx_is_not_retried(relays):
    relay = relays(statuses=(400,))

    delivered = post_run_event(an_event(), [relay.url], timeout=2)

    assert len(relay.requests) == 1
    assert delivered == []


def test_one_relay_down_does_not_stop_the_others(relays):
    up = relays()

    delivered = post_run_event(an_event(), [a_closed_port_url(), up.url], timeout=2)

    assert delivered == [up.url]
    assert len(up.requests) == 1


def test_a_hanging_relay_does_not_delay_the_others_beyond_the_bound(relays):
    hanging, up = relays(delay=3), relays()

    started = time.monotonic()
    delivered = post_run_event(an_event(), [hanging.url, up.url], timeout=0.5)
    elapsed = time.monotonic() - started

    assert delivered == [up.url]
    # Concurrent: two attempts of 0.5 s on the hanging relay, not more.
    assert elapsed < 2.0


def test_all_relays_down_returns_nothing_and_does_not_raise():
    assert (
        post_run_event(
            an_event(), [a_closed_port_url(), a_closed_port_url()], timeout=1
        )
        == []
    )


def test_no_urls_posts_nothing():
    assert post_run_event(an_event(), [], timeout=1) == []


# ---------------------------------------------------------------------------
# emit_run_event: the transport choice
# ---------------------------------------------------------------------------


def test_urls_set_selects_http(monkeypatch, relays):
    relay = relays()
    monkeypatch.setenv("RUN_EVENTS_URLS", relay.url)
    monkeypatch.setenv("MQTT_EVENTS_ENABLED", "false")
    mqtt_calls = []
    monkeypatch.setattr(
        "celine.utils.pipelines.run_events._emit_mqtt",
        lambda *a, **k: mqtt_calls.append(a) or True,
    )

    ok = emit_run_event(
        PipelineConfig(), "demo3", "meters-flow", PipelineStatus.COMPLETED, "r1"
    )

    assert ok is True
    assert relay.requests[0]["body"]["status"] == "completed"
    assert mqtt_calls == []


def test_urls_and_mqtt_both_set_http_wins_with_a_warning(monkeypatch, relays, caplog):
    relay = relays()
    monkeypatch.setenv("RUN_EVENTS_URLS", relay.url)
    monkeypatch.setenv("MQTT_EVENTS_ENABLED", "true")
    mqtt_calls = []
    monkeypatch.setattr(
        "celine.utils.pipelines.run_events._emit_mqtt",
        lambda *a, **k: mqtt_calls.append(a) or True,
    )

    with caplog.at_level("WARNING"):
        emit_run_event(
            PipelineConfig(), "demo3", "meters-flow", PipelineStatus.COMPLETED, "r1"
        )

    assert len(relay.requests) == 1
    assert mqtt_calls == []
    assert "RUN_EVENTS_URLS" in caplog.text


def test_mqtt_is_selected_only_when_no_url_is_set(monkeypatch, caplog):
    mqtt_calls = []
    monkeypatch.setattr(
        "celine.utils.pipelines.run_events._emit_mqtt",
        lambda *a, **k: mqtt_calls.append(a) or True,
    )

    with caplog.at_level("WARNING"):
        ok = emit_run_event(
            PipelineConfig(), "demo3", "meters-flow", PipelineStatus.COMPLETED, "r1"
        )

    assert ok is True
    assert len(mqtt_calls) == 1
    assert "deprecated" in caplog.text


def test_neither_transport_sends_nothing(monkeypatch):
    monkeypatch.setenv("MQTT_EVENTS_ENABLED", "false")
    mqtt_calls = []
    monkeypatch.setattr(
        "celine.utils.pipelines.run_events._emit_mqtt",
        lambda *a, **k: mqtt_calls.append(a) or True,
    )

    ok = emit_run_event(
        PipelineConfig(), "demo3", "meters-flow", PipelineStatus.COMPLETED, "r1"
    )

    assert ok is False
    assert mqtt_calls == []


def test_http_reports_false_when_no_relay_got_the_event(monkeypatch):
    monkeypatch.setenv("RUN_EVENTS_URLS", a_closed_port_url())
    monkeypatch.setenv("MQTT_EVENTS_ENABLED", "false")

    assert (
        emit_run_event(PipelineConfig(), "demo3", "f", PipelineStatus.COMPLETED, "r1")
        is False
    )


def test_the_event_carries_error_and_duration(monkeypatch, relays):
    relay = relays()
    monkeypatch.setenv("RUN_EVENTS_URLS", relay.url)
    monkeypatch.setenv("MQTT_EVENTS_ENABLED", "false")

    emit_run_event(
        PipelineConfig(),
        "demo3",
        "meters-flow",
        PipelineStatus.FAILED,
        "r1",
        error="boom",
        duration_ms=42,
    )

    body = relay.requests[0]["body"]
    assert (body["namespace"], body["flow"], body["status"], body["run_id"]) == (
        "demo3",
        "meters-flow",
        "failed",
        "r1",
    )
    assert body["error"] == "boom" and body["duration_ms"] == 42


# ---------------------------------------------------------------------------
# flow_hooks
# ---------------------------------------------------------------------------


class _Flow:
    name = "meters-flow"


class _State:
    message = "it broke"


@pytest.fixture
def recorded(monkeypatch):
    calls: list[dict] = []
    done = threading.Event()

    def record(cfg, namespace, flow, status, run_id, error=None, duration_ms=None):
        calls.append(
            {
                "namespace": namespace,
                "flow": flow,
                "status": status,
                "run_id": run_id,
                "error": error,
                "duration_ms": duration_ms,
            }
        )
        if len(calls) == 3:
            done.set()
        return True

    monkeypatch.setattr(context_module, "emit_run_event", record)
    return calls, done


def test_each_hook_sends_one_event_with_its_status(monkeypatch, recorded):
    calls, done = recorded
    monkeypatch.setenv("APP_NAME", "demo3")
    on_running, on_completion, on_failure = flow_hooks(PipelineConfig())

    on_running(_Flow(), None, None)
    on_completion(_Flow(), None, None)
    on_failure(_Flow(), None, _State())
    assert done.wait(2)

    by_status = {c["status"]: c for c in calls}
    assert set(by_status) == {
        PipelineStatus.STARTED,
        PipelineStatus.COMPLETED,
        PipelineStatus.FAILED,
    }
    assert {c["flow"] for c in calls} == {"meters-flow"}
    assert len({c["run_id"] for c in calls}) == 1
    assert by_status[PipelineStatus.COMPLETED]["duration_ms"] is not None
    assert by_status[PipelineStatus.FAILED]["duration_ms"] is not None
    assert by_status[PipelineStatus.FAILED]["error"] == "it broke"


def test_a_hanging_transport_never_holds_a_hook_past_the_join_timeout(monkeypatch):
    monkeypatch.setattr(context_module, "HOOK_JOIN_TIMEOUT", 0.2)
    monkeypatch.setattr(context_module, "emit_run_event", lambda *a, **k: time.sleep(5))
    _, on_completion, on_failure = flow_hooks(PipelineConfig())

    started = time.monotonic()
    on_completion(_Flow(), None, None)
    on_failure(_Flow(), None, _State())

    assert time.monotonic() - started < 1.5


def test_a_raising_transport_never_raises_into_the_hook(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("transport broke")

    monkeypatch.setattr(context_module, "emit_run_event", boom)
    on_running, on_completion, on_failure = flow_hooks(PipelineConfig())

    on_running(_Flow(), None, None)
    on_completion(_Flow(), None, None)
    on_failure(_Flow(), None, _State())


def test_the_dead_runner_stub_is_gone():
    from celine.utils.pipelines.pipeline_runner import PipelineRunner

    assert not hasattr(PipelineRunner, "_emit_mqtt_pipeline_event")
