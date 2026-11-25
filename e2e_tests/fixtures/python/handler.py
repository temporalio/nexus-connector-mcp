"""Worker for the e2e contract service.

Prints READY when the Worker runs.
"""

import argparse
import asyncio
import os
from datetime import timedelta

import nexusrpc
import nexusrpc.handler

from contract import (
    SERVICE,
    EchoWorkflow,
    Input,
    KeeperWorkflow,
    Output,
    SlowWorkflow,
    echo_activity,
)
from stdin_watch import exit_on_stdin_close
from temporalio import nexus, nexus_mcp
from temporalio.client import Client, WorkflowUpdateStage
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.worker import Worker


@nexusrpc.service(name=SERVICE)
@nexus_mcp.service
class ContractService:
    """The contract service definition."""

    sync_echo: nexusrpc.Operation[Input, Output]
    workflow_run: nexusrpc.Operation[Input, Output]
    start_workflow: nexusrpc.Operation[Input, Output]
    start_activity: nexusrpc.Operation[Input, Output]
    start_update: nexusrpc.Operation[Input, Output]
    fail: nexusrpc.Operation[Input, Output]
    slow: nexusrpc.Operation[Input, Output]


@nexusrpc.handler.service_handler(service=ContractService)
@nexus_mcp.service_handler
class ContractHandler:
    """The contract service handler."""

    def __init__(self, task_queue: str) -> None:
        self._task_queue = task_queue

    @nexus_mcp.tool(description="Echo the message in a sync operation.")
    @nexusrpc.handler.sync_operation
    async def sync_echo(
        self, ctx: nexusrpc.handler.StartOperationContext, input: Input
    ) -> Output:
        return Output(message=input.message, kind="sync")

    @nexus_mcp.tool(description="Echo the message in a Workflow run.")
    @nexus.workflow_run_operation
    async def workflow_run(
        self, ctx: nexus.WorkflowRunOperationContext, input: Input
    ) -> nexus.WorkflowHandle[Output]:
        return await ctx.start_workflow(EchoWorkflow.run, input, id=ctx.request_id)

    @nexus_mcp.tool(description="Echo the message in a started Workflow.")
    @nexus.temporal_operation
    async def start_workflow(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: Input,
    ) -> nexus.TemporalOperationResult[Output]:
        return await client.start_workflow(EchoWorkflow.run, input, id=ctx.request_id)

    @nexus_mcp.tool(description="Echo the message in an Activity.")
    @nexus.temporal_operation
    async def start_activity(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: Input,
    ) -> nexus.TemporalOperationResult[Output]:
        return await client.start_activity(
            echo_activity,
            input,
            id=ctx.request_id,
            task_queue=self._task_queue,
            start_to_close_timeout=timedelta(minutes=1),
        )

    @nexus_mcp.tool(description="Echo the message in a Workflow update.")
    @nexus.temporal_operation
    async def start_update(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: Input,
    ) -> nexus.TemporalOperationResult[Output]:
        return await client.start_workflow_update(
            keeper_id(self._task_queue),
            "echo",
            input,
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
            result_type=Output,
        )

    @nexus_mcp.tool(description="Fail the operation.")
    @nexusrpc.handler.sync_operation
    async def fail(
        self, ctx: nexusrpc.handler.StartOperationContext, input: Input
    ) -> Output:
        raise nexusrpc.OperationError(
            f"failed: {input.message}", state=nexusrpc.OperationErrorState.FAILED
        )

    @nexus_mcp.tool(description="Run a Workflow for an hour.")
    @nexus.workflow_run_operation
    async def slow(
        self, ctx: nexus.WorkflowRunOperationContext, input: Input
    ) -> nexus.WorkflowHandle[Output]:
        return await ctx.start_workflow(
            SlowWorkflow.run, input, id=f"slow-{input.message}"
        )


def keeper_id(task_queue: str) -> str:
    """Return the ID of the KeeperWorkflow that the update operation targets."""
    return f"contract-keeper-{task_queue}"


async def main() -> None:
    exit_on_stdin_close()
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-queue", required=True)
    args = parser.parse_args()

    client = await Client.connect(
        os.environ["TEMPORAL_ADDRESS"], namespace=os.environ["TEMPORAL_NAMESPACE"]
    )
    worker = Worker(
        client,
        task_queue=args.task_queue,
        nexus_service_handlers=[ContractHandler(args.task_queue)],
        workflows=[EchoWorkflow, KeeperWorkflow, SlowWorkflow],
        activities=[echo_activity],
    )
    async with worker:
        await client.start_workflow(
            KeeperWorkflow.run,
            id=keeper_id(args.task_queue),
            task_queue=args.task_queue,
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        print("READY", flush=True)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
