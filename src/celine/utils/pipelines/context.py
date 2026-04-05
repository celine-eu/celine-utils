from contextlib import asynccontextmanager
from os import path
import asyncio
import threading
import time
import inspect
from types import FrameType
from typing import Any
from uuid import uuid4

from celine.utils.pipelines.mqtt import emit_pipeline_event
from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.pipeline_result import PipelineStatus
from celine.utils.pipelines.utils import get_namespace


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _emit_in_thread(cfg, namespace, flow_name, status, run_id, **kwargs):
    """
    Run emit_pipeline_event in a brand-new daemon thread with its own event
    loop. This is the only safe way to call async MQTT code from a Prefect
    sync flow hook.

    WHY: Prefect's sync flow hooks are called via run_coro_as_sync(), which
    posts work to a shared portal event loop in a background thread. If our
    hook is async, it gets scheduled on that same portal loop. When
    broker.connect() then tries to schedule asyncio.sleep() on that loop,
    it deadlocks — the loop is blocked waiting for the hook to finish, but
    the hook is waiting for the loop to run the sleep.

    By spawning a fresh thread with asyncio.run(), we get a completely
    independent event loop. No shared state, no deadlock possible.
    The thread is daemon so it never prevents process exit.
    """

    def _run():
        try:
            asyncio.run(
                emit_pipeline_event(cfg, namespace, flow_name, status, run_id, **kwargs)
            )
        except Exception:
            pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    # Fire and forget — hook returns immediately, flow is not delayed.


def _emit_in_thread_and_wait(
    cfg, namespace, flow_name, status, run_id, timeout=12, **kwargs
):
    """
    Same as _emit_in_thread but joins up to `timeout` seconds.
    Used for completion/failure hooks where we want to flush before the
    pod terminates.
    """

    def _run():
        try:
            asyncio.run(
                emit_pipeline_event(cfg, namespace, flow_name, status, run_id, **kwargs)
            )
        except Exception:
            pass

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=timeout)


# ---------------------------------------------------------------------------
# pipeline_context — for non-Prefect usage (CLI, scripts, tests)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# mqtt_flow_hooks — for Prefect @flow usage
# ---------------------------------------------------------------------------


def flow_hooks(cfg: PipelineConfig):
    """
    Returns (on_running, on_completion, on_failure) SYNC Prefect state-change
    hooks that emit MQTT events without ever touching Prefect's event loop.

    Usage::

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

    def on_running(flow, flow_run, state):
        _emit_in_thread(cfg, namespace, flow.name, PipelineStatus.STARTED, run_id)

    def on_completion(flow, flow_run, state):
        _emit_in_thread_and_wait(
            cfg,
            namespace,
            flow.name,
            PipelineStatus.COMPLETED,
            run_id,
            duration_ms=int((time.time() - start_time) * 1000),
        )

    def on_failure(flow, flow_run, state):
        _emit_in_thread_and_wait(
            cfg,
            namespace,
            flow.name,
            PipelineStatus.FAILED,
            run_id,
            error=state.message,
            duration_ms=int((time.time() - start_time) * 1000),
        )

    return on_running, on_completion, on_failure
