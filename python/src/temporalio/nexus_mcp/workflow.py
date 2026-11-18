"""Call Nexus-backed MCP tools from Workflow code.

AI SDK bindings delegate ``list_tools`` and ``call_tool`` to
:class:`NexusMCPClient`. The bindings live in ``temporalio/ai-integrations``.
"""

from collections.abc import Mapping
from typing import Any

from mcp.types import CallToolResult, Tool

__all__ = ["NexusMCPClient"]


class NexusMCPClient:
    """MCP client that calls tools through Workflow Nexus operations."""

    def __init__(self, *, service: str, endpoint: str) -> None:
        """Create a client for one Nexus service.

        Args:
            service: The Nexus service name.
            endpoint: The Nexus endpoint name.
        """
        raise NotImplementedError(
            "temporalio.nexus_mcp.workflow.NexusMCPClient is not implemented"
        )

    async def list_tools(self) -> list[Tool]:
        """List the tools of the Nexus service.

        Returns:
            The MCP tool definitions.
        """
        raise NotImplementedError

    async def call_tool(
        self, name: str, arguments: Mapping[str, Any] | None = None
    ) -> CallToolResult:
        """Call a tool and wait for its result.

        Args:
            name: The tool name.
            arguments: The tool arguments.

        Returns:
            The MCP tool result.
        """
        raise NotImplementedError
