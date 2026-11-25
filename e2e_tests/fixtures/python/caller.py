"""Runs CallerWorkflow once.

Reads a CallInput as JSON on stdin and prints the Workflow result as JSON on
stdout.
"""

import asyncio
import json
import os
import sys
import uuid

from callers import CallerWorkflow, CallInput
from temporalio.client import Client
from temporalio.worker import Worker


async def main() -> None:
    call = CallInput(**json.load(sys.stdin))
    client = await Client.connect(
        os.environ["TEMPORAL_ADDRESS"], namespace=os.environ["TEMPORAL_NAMESPACE"]
    )
    task_queue = f"caller-python-{uuid.uuid4()}"
    async with Worker(client, task_queue=task_queue, workflows=[CallerWorkflow]):
        output = await client.execute_workflow(
            CallerWorkflow.run, call, id=task_queue, task_queue=task_queue
        )
    json.dump(output, sys.stdout)


if __name__ == "__main__":
    asyncio.run(main())
