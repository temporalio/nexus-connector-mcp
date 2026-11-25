"""Types, Workflows, and the Activity of the e2e contract service.

The Workflows are in their own module, so the Workflow sandbox can import
them.
"""

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import activity, workflow

SERVICE = "contract"


@dataclass
class Input:
    """Input of every operation."""

    message: str


@dataclass
class Output:
    """Output of every echo operation. kind is the primitive that ran."""

    message: str
    kind: str


@workflow.defn(name="EchoWorkflow")
class EchoWorkflow:
    """Echoes its input."""

    @workflow.run
    async def run(self, input: Input) -> Output:
        """Echo the message."""
        return Output(message=input.message, kind="workflow")


@workflow.defn(name="KeeperWorkflow")
class KeeperWorkflow:
    """Runs until it is cancelled. Its update echoes the input."""

    @workflow.run
    async def run(self) -> None:
        """Wait forever."""
        await workflow.wait_condition(lambda: False)

    @workflow.update(name="echo")
    async def echo(self, input: Input) -> Output:
        """Echo the message."""
        return Output(message=input.message, kind="update")


@workflow.defn(name="SlowWorkflow")
class SlowWorkflow:
    """Runs for an hour."""

    @workflow.run
    async def run(self, input: Input) -> Output:
        """Sleep for an hour."""
        await asyncio.sleep(timedelta(hours=1).total_seconds())
        return Output(message=input.message, kind="workflow")


@activity.defn(name="echo_activity")
async def echo_activity(input: Input) -> Output:
    """Echo the message."""
    return Output(message=input.message, kind="activity")
