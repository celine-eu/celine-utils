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

if TYPE_CHECKING:
    from celine.sdk.broker import MqttBroker
    from celine.utils.pipelines.pipeline_config import PipelineConfig

logger = logging.getLogger(__name__)

# Global broker instance (one per container/process)
_broker: "MqttBroker | None" = None
_broker_init_attempted: bool = False


def _get_broker(cfg: "PipelineConfig") -> "MqttBroker | None":
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
        from celine.sdk.broker import MqttBroker, MqttConfig
        from celine.sdk.auth import OidcClientCredentialsProvider

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

        # Connect synchronously
        asyncio.get_event_loop().run_until_complete(_broker.connect())

        logger.info(
            "MQTT broker connected: %s:%d",
            mqtt_cfg.host,
            mqtt_cfg.port,
        )

        # Register cleanup on exit
        atexit.register(_cleanup_broker)

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


def _cleanup_broker():
    """Disconnect broker on process exit."""
    global _broker
    if _broker is not None:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_broker.disconnect())
            else:
                loop.run_until_complete(_broker.disconnect())
        except Exception:
            pass
        _broker = None


def emit_pipeline_event(
    cfg: "PipelineConfig",
    namespace: str,
    status: str,
    run_id: str,
    error: str | None = None,
    duration_ms: int | None = None,
) -> bool:
    """
    Emit a pipeline-level event to MQTT.

    Topic: celine/pipelines/status/{namespace}

    Args:
        cfg: Pipeline configuration
        namespace: Pipeline namespace (typically app_name)
        status: Event status (START, COMPLETE, FAIL, ABORT)
        run_id: Unique pipeline run identifier
        error: Error message (for FAIL/ABORT)
        duration_ms: Pipeline duration in milliseconds (for COMPLETE/FAIL/ABORT)

    Returns:
        True if published successfully, False otherwise.
        Never raises - failures are logged.
    """
    broker = _get_broker(cfg)
    if broker is None:
        return False

    try:
        from celine.sdk.broker import BrokerMessage, QoS

        topic = f"celine/pipelines/status/{namespace}"

        payload = {
            "pipeline": namespace,
            "status": status,
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "duration_ms": 0,
        }

        if error is not None:
            payload["error"] = error

        if duration_ms is not None:
            payload["duration_ms"] = duration_ms

        message = BrokerMessage(
            topic=topic,
            payload=payload,
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
        )

        # Publish synchronously
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If already in async context, create task
            future = asyncio.ensure_future(broker.publish(message))
            # Can't wait, fire and forget
            logger.debug("MQTT event queued: %s -> %s", topic, status)
            return True
        else:
            result = loop.run_until_complete(broker.publish(message))
            if result.success:
                logger.debug("MQTT event sent: %s -> %s", topic, status)
                return True
            else:
                logger.warning("MQTT publish failed: %s", result.error)
                return False

    except Exception:
        logger.exception("Failed to emit MQTT pipeline event")
        return False
