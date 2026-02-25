from contextlib import asynccontextmanager
from os import path
import time
from types import FrameType
from typing import Any
from uuid import uuid4
import inspect

from celine.utils.pipelines.mqtt import cleanup_broker, emit_pipeline_event
from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.pipeline_result import PipelineStatus
from celine.utils.pipelines.utils import get_namespace


def _resolve_callable_from_frame(f: FrameType) -> Any | None:
    name = f.f_code.co_name

    obj = f.f_globals.get(name)
    if obj is not None:
        return obj

    self_obj = f.f_locals.get("self")
    if self_obj is not None:
        return getattr(self_obj, name, None)

    cls_obj = f.f_locals.get("cls")
    if cls_obj is not None:
        return getattr(cls_obj, name, None)

    return None


def infer_flow_from_flows_dir(
    *,
    marker: str = f"{path.sep}flows{path.sep}",
    max_hops: int = 80,
) -> str | None:
    f: FrameType | None = inspect.currentframe()
    try:
        f = f.f_back if f else None
        hops = 0
        while f and hops < max_hops:
            if marker in f.f_code.co_filename:
                obj = _resolve_callable_from_frame(f)
                name = getattr(obj, "name", None) if obj is not None else None
                if isinstance(name, str) and name:
                    return name
                return f.f_code.co_name
            f = f.f_back
            hops += 1
        return None
    finally:
        del f


def flow_hooks(cfg: PipelineConfig):
    """
    Returns (on_running, on_completion, on_failure) Prefect state-change hooks
    for use on a @flow definition via on_running/on_completion/on_failure.

    Each hook is async and all MQTT failures are swallowed — the pipeline
    is never affected.

    run_id and start_time are captured once when mqtt_flow_hooks() is called,
    which happens at module import time. This is fine: Prefect spawns a fresh
    process (pod) per flow run, so there is exactly one run_id per process.

    Usage::

        from celine.utils.pipelines.pipeline import PipelineConfig, mqtt_flow_hooks

        _cfg = PipelineConfig()
        _on_running, _on_completion, _on_failure = mqtt_flow_hooks(_cfg)

        @flow(
            name="my-flow",
            on_running=[_on_running],
            on_completion=[_on_completion],
            on_failure=[_on_failure],
        )
        def my_flow(config=None):
            ...
    """
    run_id = str(uuid4())
    start_time = time.time()
    namespace = get_namespace(cfg.app_name)

    async def on_running(flow, flow_run, state):
        try:
            await emit_pipeline_event(
                cfg,
                namespace,
                flow.name,
                PipelineStatus.STARTED,
                run_id,
            )
        except Exception:
            pass

    async def on_completion(flow, flow_run, state):
        try:
            await emit_pipeline_event(
                cfg,
                namespace,
                flow.name,
                PipelineStatus.COMPLETED,
                run_id,
                duration_ms=int((time.time() - start_time) * 1000),
            )
        except Exception:
            pass
        finally:
            cleanup_broker()

    async def on_failure(flow, flow_run, state):
        try:
            await emit_pipeline_event(
                cfg,
                namespace,
                flow.name,
                PipelineStatus.FAILED,
                run_id,
                error=state.message,
                duration_ms=int((time.time() - start_time) * 1000),
            )
        except Exception:
            pass
        finally:
            cleanup_broker()

    return on_running, on_completion, on_failure
