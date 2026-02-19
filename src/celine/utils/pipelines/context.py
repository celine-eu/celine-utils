from contextlib import asynccontextmanager
import time
from uuid import uuid4

from celine.utils.pipelines.mqtt import cleanup_broker, emit_pipeline_event
from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.pipeline_result import PipelineStatus
from celine.utils.pipelines.utils import get_namespace


@asynccontextmanager
async def pipeline_context(
    cfg: "PipelineConfig", namespace: str | None = None, run_id: str | None = None
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

    namespace = namespace or get_namespace(cfg.app_name)
    run_id = run_id or str(uuid4())
    results: dict = {"status": PipelineStatus.COMPLETED}

    await emit_pipeline_event(cfg, namespace, PipelineStatus.STARTED, run_id)

    try:
        yield results
    except Exception as exc:
        failed = True
        results["status"] = PipelineStatus.FAILED
        duration_ms = int((time.time() - start_time) * 1000)
        await emit_pipeline_event(
            cfg,
            namespace,
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
                PipelineStatus.COMPLETED,
                run_id,
                duration_ms=duration_ms,
            )

        # Disconnect explicitly — don't rely on atexit, Prefect won't call it.
        cleanup_broker()
