"""Source qualification child. Run only in the isolated integration stack."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

from durable_workflow import CancellationPolicy, Client, Worker, activity, workflow
from durable_workflow.errors import WorkflowCancelled


log = logging.getLogger("qualification.cascade")


class ObservedClient(Client):
    """Observe the isolated fixture without logging HTTP bodies or headers."""

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        if not kwargs.get("worker"):
            return await super()._request(method, path, **kwargs)
        body = kwargs.get("json")
        poll_id = body.get("poll_request_id") if isinstance(body, dict) else None
        started = time.monotonic()
        log.debug("request start method=%s path=%s poll_id=%s", method, path, poll_id)
        try:
            response = await super()._request(method, path, **kwargs)
        except BaseException as error:
            log.debug("request error path=%s poll_id=%s elapsed=%.3f type=%s",
                      path, poll_id, time.monotonic() - started, type(error).__name__)
            raise
        task = response.get("task") if isinstance(response, dict) else None
        log.debug("request end path=%s poll_id=%s elapsed=%.3f task_id=%s status=%s delivered=%s",
                  path, poll_id, time.monotonic() - started,
                  task.get("task_id") if isinstance(task, dict) else None,
                  response.get("poll_status") if isinstance(response, dict) else None,
                  response.get("delivered") if isinstance(response, dict) else None)
        return response


async def observe_capacity(worker: Worker) -> None:
    previous = None
    while True:
        state = (
            worker._workflow_reserved,
            worker._workflow_inflight,
            tuple(sorted(task.get_coro().__qualname__ for task in worker._in_flight)),
        )
        if state != previous:
            log.debug("workflow state reserved=%s inflight=%s tasks=%s", *state)
            previous = state
        await asyncio.sleep(1)


@workflow.defn(name="tests.polyglot-cancellation-child")
class CancellationChild:
    def run(self, ctx: Any, remote_queue: str) -> Any:
        try:
            yield ctx.schedule_activity(
                "tests.polyglot-cancellation-remote", [], queue=remote_queue,
                cancellation_policy=CancellationPolicy.WAIT_CANCELLATION_COMPLETED,
            )
        except WorkflowCancelled as error:
            with ctx.cancellation_shield():
                yield ctx.local_activity("tests.polyglot-cancellation-child-cleanup", [
                    error.request_id, ctx.cancellation_context.to_dict(),
                ])
            return error.request_id
        raise AssertionError("the permanently blocked activity must not complete")


@activity.defn(name="tests.polyglot-cancellation-child-cleanup")
def child_cleanup(request_id: str, context: dict[str, Any]) -> str:
    Path(os.environ["DW_CASCADE_DIRECTORY"], "python-cleanup.json").write_text(
        json.dumps(context), encoding="utf-8",
    )
    return request_id


async def main() -> None:
    # Retain SDK poll/admission observations without enabling HTTP wire logging.
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.WARNING,
                        format="%(asctime)s UTC %(levelname)s %(name)s:%(message)s")
    logging.getLogger("durable_workflow.worker").setLevel(logging.DEBUG)
    log.setLevel(logging.DEBUG)
    queue = sys.argv[1]
    os.environ["DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION"] = "1.20"
    shutdown = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, shutdown.set)
    async with ObservedClient(
        os.environ["DURABLE_WORKFLOW_SERVER_URL"],
        token=os.environ["DURABLE_WORKFLOW_AUTH_TOKEN"], namespace="default",
    ) as client:
        worker = Worker(
            client, task_queue=queue, worker_id=f"{queue}-python",
            workflows=[CancellationChild], activities=[child_cleanup],
            capabilities=["cooperative_cancellation"], poll_timeout=1,
            max_concurrent_workflow_tasks=1, max_concurrent_activity_tasks=1,
        )
        running = asyncio.create_task(worker.run())
        stopping = asyncio.create_task(shutdown.wait())
        observing = asyncio.create_task(observe_capacity(worker))
        try:
            await asyncio.wait({running, stopping}, return_when=asyncio.FIRST_COMPLETED)
            if running.done():
                await running
            else:
                await worker.stop()
                await asyncio.wait_for(running, timeout=10)
        finally:
            stopping.cancel()
            observing.cancel()
            await asyncio.gather(stopping, observing, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
