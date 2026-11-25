"""The Workflow that calls tools with NexusMCPClient.

It is in its own module, so the Workflow sandbox can import it.
"""

from dataclasses import dataclass
from typing import Any

from temporalio import workflow
from temporalio.nexus_mcp.workflow import NexusMCPClient


@dataclass
class CallInput:
    """The tool call to make."""

    endpoint: str
    service: str
    tool: str
    arguments: dict[str, Any]


@workflow.defn(name="CallerWorkflow")
class CallerWorkflow:
    """Lists the tools of a Nexus service and calls one."""

    @workflow.run
    async def run(self, input: CallInput) -> dict[str, Any]:
        """List the tools, then call input.tool."""
        client = NexusMCPClient(service=input.service, endpoint=input.endpoint)
        tools = sorted(tool.name for tool in await client.list_tools())
        result = await client.call_tool(input.tool, input.arguments)
        return {
            "tools": tools,
            "result": result.model_dump(mode="json", by_alias=True, exclude_none=True),
        }
