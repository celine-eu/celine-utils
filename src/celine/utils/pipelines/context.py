import inspect
import logging
import threading
import time
from os import path
from types import FrameType
from typing import Any
from uuid import uuid4

from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.pipeline_result import PipelineStatus
from celine.utils.pipelines.run_events import emit_run_event
from celine.utils.pipelines.utils import get_namespace

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


#: Seconds the completion and failure hooks wait for their event to go out
#: before the pod may terminate. Above two HTTP attempts at the default 5 s.
HOOK_JOIN_TIMEOUT = 12.0


def _emit_in_thread(cfg, namespace, flow_name, status, run_id, **kwargs):
    """
    Send a run event (``emit_run_event``) in a brand-new daemon thread.

    WHY: Prefect's sync flow hooks are called via run_coro_as_sync(), which
    posts work to a shared portal event loop in a background thread. The MQTT
    transport is async: scheduled on that portal loop, broker.connect() would
    deadlock it. ``emit_run_event`` runs the MQTT path with asyncio.run() inside
    this thread, so it gets an independent event loop; the HTTP path is plain
    synchronous code. The thread is daemon so it never prevents process exit,
    and it swallows every error: a hook never raises.
    """

    def _run():
        try:
            emit_run_event(cfg, namespace, flow_name, status, run_id, **kwargs)
        except Exception:
            logger.warning(
                "Run event %s for %s not sent", status, flow_name, exc_info=True
            )

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    # Fire and forget — hook returns immediately, flow is not delayed.
    return t


def _emit_in_thread_and_wait(cfg, namespace, flow_name, status, run_id, **kwargs):
    """
    Same as _emit_in_thread but joins up to ``HOOK_JOIN_TIMEOUT`` seconds.
    Used for completion/failure hooks where we want to flush before the
    pod terminates.
    """
    t = _emit_in_thread(cfg, namespace, flow_name, status, run_id, **kwargs)
    t.join(timeout=HOOK_JOIN_TIMEOUT)


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
# flow_hooks — for Prefect @flow usage
# ---------------------------------------------------------------------------


def flow_hooks(cfg: PipelineConfig):
    """
    Returns (on_running, on_completion, on_failure) SYNC Prefect state-change
    hooks that send run events (HTTP to ``RUN_EVENTS_URLS``, else the
    deprecated MQTT transport; see ``run_events``) without ever touching
    Prefect's event loop.

    Usage::

        _cfg = PipelineConfig()
        _on_running, _on_completion, _on_failure = flow_hooks(_cfg)

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
