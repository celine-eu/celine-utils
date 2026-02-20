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

    # Module-level (most common for flows)
    obj = f.f_globals.get(name)
    if obj is not None:
        return obj

    # Method flows (if you ever do them)
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
        f = f.f_back if f else None  # out of this helper
        hops = 0
        while f and hops < max_hops:
            if marker in f.f_code.co_filename:
                # Prefer Prefect-configured name if present on wrapper
                obj = _resolve_callable_from_frame(f)
                name = getattr(obj, "name", None) if obj is not None else None
                if isinstance(name, str) and name:
                    return name
                return f.f_code.co_name  # fallback: medallion_flow
            f = f.f_back
            hops += 1
        return None
    finally:
        del f
        
@asynccontextmanager
async def pipeline_context(
    cfg: "PipelineConfig", flow: str | None = None, namespace: str | None = None, run_id: str | None = None
):
    """
    Context manager that guarantees START and COMPLETE/FAIL are emitted even
    when Prefect tears down the worker thread (which skips atexit handlers).

    Emits START on entry, COMPLETE on clean exit, FAIL if an exception
    propagates. Disconnects the broker explicitly on exit so the last publish
    is flushed before the thread dies.

    Usage::

        from celine.utils.pipelines.mqtt import pipeline_publisher

        with pipeline_publisher(cfg, namespace=namespace, run_id=run_id):
            run_tasks()
    """
    start_time = time.time()
    failed = False

    if flow is None:
        flow = infer_flow_from_flows_dir()

    namespace = namespace or get_namespace(cfg.app_name)
    run_id = run_id or str(uuid4())
    results: dict = {
        "status": PipelineStatus.COMPLETED,
        "namespace": namespace,
        "flow": flow,
        }

    await emit_pipeline_event(
        cfg, 
        namespace, 
        flow, 
        PipelineStatus.STARTED, 
        run_id
    )


    try:
        yield results
    except Exception as exc:
        failed = True
        results["status"] = PipelineStatus.FAILED
        duration_ms = int((time.time() - start_time) * 1000)
        await emit_pipeline_event(
            cfg,
            namespace,
            flow,
            PipelineStatus.FAILED,
            run_id,
            error=str(exc),
            duration_ms=duration_ms,
        )
        raise
    finally:
        if not failed:
            duration_ms = int((time.time() - start_time) * 1000)
            await emit_pipeline_event(
                cfg,
                namespace,
                flow,
                PipelineStatus.COMPLETED,
                run_id,
                duration_ms=duration_ms,
            )

        # Disconnect explicitly — don't rely on atexit, Prefect won't call it.
        cleanup_broker()
