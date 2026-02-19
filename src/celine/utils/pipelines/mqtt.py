"""MQTT pipeline event publishing.

Provides pipeline-level event publishing via celine-sdk's MQTT broker.
Events are published to: celine/pipelines/{namespace}

All failures are logged but never propagate - pipeline execution continues.
"""

from __future__ import annotations

import asyncio
import atexit
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from celine.sdk.broker import MqttBroker, MqttConfig
from celine.sdk.auth import OidcClientCredentialsProvider
from celine.sdk.broker import BrokerMessage, QoS, MqttBroker
from celine.sdk.broker import PipelineRunEvent

if TYPE_CHECKING:
    from celine.utils.pipelines.pipeline import PipelineConfig, PipelineStatus

logger = logging.getLogger(__name__)

# Global broker instance (one per container/process)
_broker: "MqttBroker | None" = None
_broker_init_attempted: bool = False


async def _get_broker(cfg: "PipelineConfig") -> "MqttBroker | None":
    """
    Get or initialize the global MQTT broker.

    Returns None if:
    - mqtt_events_enabled is False
    - celine-sdk is not installed
    - Connection fails
    """
    global _broker, _broker_init_attempted

    if _broker is not None:
        return _broker

    if _broker_init_attempted:
        return None

    _broker_init_attempted = True

    if not cfg.mqtt_events_enabled:
        logger.warning("MQTT pipeline events disabled")
        return None

    try:

        if cfg.sdk is None:
            raise Exception("SDK config not found")

        mqtt_cfg = cfg.sdk.mqtt
        oidc_cfg = cfg.sdk.oidc

        # Build token provider if OIDC is configured
        token_provider = None
        if oidc_cfg.base_url and oidc_cfg.client_id and oidc_cfg.client_secret:
            token_provider = OidcClientCredentialsProvider(
                base_url=oidc_cfg.base_url,
                client_id=oidc_cfg.client_id,
                client_secret=oidc_cfg.client_secret,
                scope=oidc_cfg.scope,
                timeout=oidc_cfg.timeout,
            )
            logger.debug("MQTT: OIDC token provider configured")

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
            reconnect_interval=1.0,  # Fast retry
            max_reconnect_attempts=2,  # Fail fast - don't block pipeline
            topic_prefix="",  # We handle prefix in topic construction
            token_refresh_margin=mqtt_cfg.token_refresh_margin,
        )

        _broker = MqttBroker(config=config, token_provider=token_provider)

        await _broker.connect()

        logger.info(
            "MQTT broker connected: %s:%d",
            mqtt_cfg.host,
            mqtt_cfg.port,
        )

        # Register cleanup on exit
        atexit.register(cleanup_broker)

        return _broker

    except ImportError:
        logger.warning(
            "celine-sdk not installed, MQTT pipeline events disabled. "
            "Install with: pip install celine-sdk"
        )
        return None
    except Exception:
        logger.exception("Failed to initialize MQTT broker, events disabled")
        return None


def cleanup_broker():
    """Disconnect broker on process exit."""
    global _broker
    if _broker is None:
        return

    broker = _broker
    _broker = None  # Clear first so a second call is a no-op

    async def _disconnect():
        try:
            await broker.disconnect()
        except Exception:
            pass  # Swallow MqttCodeError / unexpected disconnection on teardown

    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Schedule and forget — we can't await here
            loop.create_task(_disconnect())
        else:
            loop.run_until_complete(_disconnect())
    except Exception:
        pass


async def emit_pipeline_event(
    cfg: "PipelineConfig",
    namespace: str,
    status: PipelineStatus,
    run_id: str,
    error: str | None = None,
    duration_ms: int | None = None,
) -> bool:
    """
    Emit a pipeline-level event to MQTT.
    ...
    """
    broker = await _get_broker(cfg)
    if broker is None:
        return False

    try:

        topic = f"celine/pipelines/runs/{namespace}"

        payload = PipelineRunEvent(
            pipeline=namespace,
            status=status,
            run_id=run_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            duration_ms=0,
        )

        if error is not None:
            payload.error = error

        if duration_ms is not None:
            payload.duration_ms = duration_ms

        message = BrokerMessage(
            topic=topic,
            payload=payload.model_dump(),
            qos=QoS.EXACTLY_ONCE,
            retain=False,
        )

        result = await broker.publish(message)
        if result.success:
            logger.debug("MQTT event sent: %s -> %s", topic, status)
            return True
        else:
            logger.warning("MQTT publish failed: %s", result.error)
            return False

    except Exception:
        logger.exception("Failed to emit MQTT pipeline event")
        return False
