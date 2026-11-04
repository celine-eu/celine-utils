"""Pipeline run events: the transport choice, and the HTTP transport.

A flow's hooks report ``started`` / ``completed`` / ``failed`` as a
``PipelineRunEvent``. Two transports carry it:

- **HTTP** (``RUN_EVENTS_URLS``): the event is POSTed as JSON to every URL in
  the list, one sync_tables relay per environment. The relays take no token:
  their boundary is the network (a ClusterIP Service whose NetworkPolicy admits
  only the flows' namespace). Each URL is tried on its own, concurrently, with
  one retry after a 5xx or a connection failure, so a relay that is down or
  hanging never keeps the event from the others.
- **MQTT** (``MQTT_EVENTS_ENABLED``, deprecated): the former transport, used only
  when no URL is set. See :mod:`celine.utils.pipelines.mqtt`.

With neither, no event is sent. Every failure is logged and reported as a
return value; nothing here raises into a Prefect hook.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import httpx
from celine.sdk.broker import PipelineRunEvent

if TYPE_CHECKING:
    from celine.utils.pipelines.pipeline_config import PipelineConfig
    from celine.utils.pipelines.pipeline_result import PipelineStatus

logger = logging.getLogger(__name__)

#: Attempts per relay URL: the first POST and one retry.
ATTEMPTS = 2


def build_run_event(
    namespace: str,
    flow: str | None,
    status: PipelineStatus | str,
    run_id: str,
    error: str | None = None,
    duration_ms: int | None = None,
) -> PipelineRunEvent:
    """Build the event exactly as the MQTT transport always has."""
    return PipelineRunEvent(
        namespace=namespace,
        flow=flow,
        status=str(status),
        run_id=run_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        duration_ms=duration_ms or 0,
        error=error,
    )


def _post_one(client: httpx.Client, url: str, body: dict) -> bool:
    for attempt in range(1, ATTEMPTS + 1):
        try:
            response = client.post(url, json=body)
        except httpx.HTTPError as exc:
            logger.warning(
                "Run event POST to %s failed (attempt %d): %s", url, attempt, exc
            )
            continue
        if response.is_success:
            return True
        if response.status_code < 500:
            logger.warning(
                "Run event refused by %s: HTTP %d", url, response.status_code
            )
            return False
        logger.warning(
            "Run event POST to %s: HTTP %d (attempt %d)",
            url,
            response.status_code,
            attempt,
        )
    return False


def post_run_event(
    event: PipelineRunEvent, urls: Sequence[str], timeout: float
) -> list[str]:
    """POST ``event`` to every URL concurrently, with no token.

    ``timeout`` bounds each attempt; a URL gets at most :data:`ATTEMPTS`.
    Returns the URLs that answered 2xx, in the order given. Never raises.
    """
    if not urls:
        return []
    body = event.model_dump(mode="json")
    try:
        with (
            httpx.Client(timeout=timeout) as client,
            ThreadPoolExecutor(
                max_workers=len(urls), thread_name_prefix="run-event"
            ) as pool,
        ):
            results = list(pool.map(lambda url: _post_one(client, url, body), urls))
    except Exception as exc:  # noqa: BLE001 — a hook must never raise
        logger.warning("Run event POST failed: %s", exc)
        return []
    return [url for url, ok in zip(urls, results) if ok]


def _emit_mqtt(
    cfg: PipelineConfig,
    namespace: str,
    flow: str | None,
    status: PipelineStatus,
    run_id: str,
    error: str | None = None,
    duration_ms: int | None = None,
) -> bool:
    from celine.utils.pipelines.mqtt import emit_pipeline_event

    return asyncio.run(
        emit_pipeline_event(
            cfg, namespace, flow, status, run_id, error=error, duration_ms=duration_ms
        )
    )


def emit_run_event(
    cfg: PipelineConfig,
    namespace: str,
    flow: str | None,
    status: PipelineStatus,
    run_id: str,
    error: str | None = None,
    duration_ms: int | None = None,
) -> bool:
    """Send one run event on the configured transport (synchronous).

    ``RUN_EVENTS_URLS`` set → HTTP, and MQTT is not used even when enabled.
    Otherwise ``MQTT_EVENTS_ENABLED`` → MQTT (deprecated). Neither → nothing.
    Returns True when at least one relay (or the broker) took the event.
    Must run off Prefect's event loop: the hooks call it in a thread of its own.
    """
    if cfg.run_events_urls:
        if cfg.mqtt_events_enabled and "mqtt_events_enabled" in cfg.model_fields_set:
            logger.warning(
                "RUN_EVENTS_URLS and MQTT_EVENTS_ENABLED are both set: run events go over HTTP only"
            )
        event = build_run_event(
            namespace, flow, status, run_id, error=error, duration_ms=duration_ms
        )
        delivered = post_run_event(event, cfg.run_events_urls, cfg.run_events_timeout)
        if len(delivered) < len(cfg.run_events_urls):
            logger.warning(
                "Run event %s/%s %s reached %d of %d relays",
                namespace,
                flow,
                status,
                len(delivered),
                len(cfg.run_events_urls),
            )
        return bool(delivered)

    if cfg.mqtt_events_enabled:
        logger.warning("MQTT run events are deprecated: set RUN_EVENTS_URLS instead")
        return _emit_mqtt(
            cfg, namespace, flow, status, run_id, error=error, duration_ms=duration_ms
        )

    return False
