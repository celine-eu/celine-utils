"""MQTT pipeline event publishing.

Provides pipeline-level event publishing via celine-sdk's MQTT broker.
Events are published to: celine/pipelines/runs/{namespace}

Each emit creates a fresh connection, publishes, then disconnects.  This
avoids event-loop lifetime issues: Prefect hooks run inside asyncio.run()
calls on temporary threads, so a cached broker created on one loop would
be dead by the time the next hook fires on a different loop.

All failures are logged but never propagate — pipeline execution continues.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from celine.sdk.auth import OidcClientCredentialsProvider
from celine.sdk.broker import BrokerMessage, MqttBroker, MqttConfig, PipelineRunEvent, QoS

if TYPE_CHECKING:
    from celine.utils.pipelines.pipeline import PipelineConfig, PipelineStatus

logger = logging.getLogger(__name__)


def cleanup_broker() -> None:
    """No-op — kept for backwards compatibility."""


async def emit_pipeline_event(
    cfg: "PipelineConfig",
    namespace: str,
    flow: str | None,
    status: "PipelineStatus",
    run_id: str,
    error: str | None = None,
    duration_ms: int | None = None,
) -> bool:
    """Emit a pipeline-level event to MQTT.

    Opens a fresh MQTT connection, publishes the event, then disconnects.
    Returns True on success, False on any failure.
    """
    if not cfg.mqtt_events_enabled:
        logger.warning("MQTT pipeline events disabled")
        return False

    if cfg.sdk is None:
        logger.warning("SDK config not found — MQTT events disabled")
        return False

    mqtt_cfg = cfg.sdk.mqtt
    oidc_cfg = cfg.sdk.oidc

    # Build token provider if OIDC is configured.
    token_provider = None
    if oidc_cfg.base_url and oidc_cfg.client_id and oidc_cfg.client_secret:
        token_provider = OidcClientCredentialsProvider(
            base_url=oidc_cfg.base_url,
            client_id=oidc_cfg.client_id,
            client_secret=oidc_cfg.client_secret,
            scope=oidc_cfg.scope,
            timeout=oidc_cfg.timeout,
            verify_ssl=oidc_cfg.verify_ssl,
        )

    config = MqttConfig(
        host=mqtt_cfg.host,
        port=mqtt_cfg.port,
        client_id=mqtt_cfg.client_id,
        username=mqtt_cfg.username,
        password=mqtt_cfg.password,
        use_tls=mqtt_cfg.use_tls,
        ca_certs=mqtt_cfg.ca_certs,
        certfile=mqtt_cfg.certfile,
        keyfile=mqtt_cfg.keyfile,
        keepalive=mqtt_cfg.keepalive,
        clean_session=mqtt_cfg.clean_session,
        reconnect_interval=1.0,
        max_reconnect_attempts=2,  # Fail fast — don't block pipeline teardown
        topic_prefix="",
        token_refresh_margin=mqtt_cfg.token_refresh_margin,
    )

    broker = MqttBroker(config=config, token_provider=token_provider)
    try:
        await broker.connect()

        payload = PipelineRunEvent(
            flow=flow,
            namespace=namespace,
            status=status,
            run_id=run_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            duration_ms=duration_ms or 0,
        )
        if error is not None:
            payload.error = error

        topic = f"celine/pipelines/runs/{namespace}"
        result = await broker.publish(
            BrokerMessage(
                topic=topic,
                payload=payload.model_dump(),
                qos=QoS.AT_LEAST_ONCE,
                retain=False,
            )
        )

        if result.success:
            logger.info("MQTT event published: %s flow=%s status=%s", topic, flow, status)
            return True
        else:
            logger.warning("MQTT publish failed: %s", result.error)
            return False

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning("Failed to emit MQTT pipeline event: %s", exc)
        return False
    finally:
        try:
            await broker.disconnect()
        except Exception:
            pass
