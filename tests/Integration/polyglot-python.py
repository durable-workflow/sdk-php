"""Source qualification child. Run only in the isolated integration stack."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any

from durable_workflow import CancellationPolicy, Client, Worker, activity, workflow
from durable_workflow.errors import WorkflowCancelled


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
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("durable_workflow.worker").setLevel(logging.DEBUG)
    queue = sys.argv[1]
    os.environ["DURABLE_WORKFLOW_WORKER_PROTOCOL_VERSION"] = "1.20"
    shutdown = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, shutdown.set)
    async with Client(
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
        try:
            await asyncio.wait({running, stopping}, return_when=asyncio.FIRST_COMPLETED)
            if running.done():
                await running
            else:
                await worker.stop()
                await asyncio.wait_for(running, timeout=10)
        finally:
            stopping.cancel()
            await asyncio.gather(stopping, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
