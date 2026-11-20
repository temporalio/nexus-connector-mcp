"""Call Nexus-backed MCP tools from Workflow code.

AI SDK bindings delegate ``list_tools`` and ``call_tool`` to
:class:`NexusMCPClient`. The bindings live in ``temporalio/ai-integrations``.
"""

import json
from collections.abc import Mapping
from typing import Any

from mcp import types
from mcp.shared.exceptions import MCPError

from temporalio import workflow
from temporalio.exceptions import FailureError, is_cancelled_exception

__all__ = ["NexusMCPClient"]

_LIST_TOOLS_OPERATION = "list_tools"
# A result with only this key is already an MCP tool result. The outbound
# proxy returns upstream results in this envelope.
_RESULT_ENVELOPE_KEY = "io.temporal/mcpCallToolResult"


class NexusMCPClient:
    """MCP client that calls tools through Workflow Nexus operations.

    ``list_tools`` runs the ``list_tools`` operation of the service.
    ``call_tool`` runs the Nexus operation that has the tool name. The
    Workflow waits for each operation durably, so a long-running tool needs no
    polling. Workflow cancellation cancels the operation.

    Use one client per Workflow execution. Do not share it across Workflows.
    """

    def __init__(self, *, service: str, endpoint: str) -> None:
        """Create a client for one Nexus service.

        Args:
            service: The Nexus service name.
            endpoint: The Nexus endpoint name.

        Raises:
            ValueError: If service or endpoint is empty.
        """
        if not service.strip() or not endpoint.strip():
            raise ValueError("service and endpoint are required")
        self._service = service
        self._endpoint = endpoint

    async def list_tools(self) -> list[types.Tool]:
        """List the tools of the Nexus service.

        Returns:
            The MCP tool definitions.

        Raises:
            ValueError: If the service returns a tool without a name, or the
                same tool name more than once.
        """
        manifest = types.ListToolsResult.model_validate(
            await self._execute(_LIST_TOOLS_OPERATION, None)
        )
        names: set[str] = set()
        for tool in manifest.tools:
            if not tool.name:
                raise ValueError(
                    f"Service {self._service!r} returned a tool without a name"
                )
            if tool.name in names:
                raise ValueError(
                    f"Service {self._service!r} returned tool {tool.name!r} more than once"
                )
            names.add(tool.name)
        return list(manifest.tools)

    async def call_tool(
        self, name: str, arguments: Mapping[str, Any] | None = None
    ) -> types.CallToolResult:
        """Call a tool and wait for its result.

        The tool name is the operation name. An operation that the service
        does not have fails, and the failure becomes a tool error.

        Args:
            name: The tool name.
            arguments: The tool arguments.

        Returns:
            The MCP tool result. A failed operation returns a result with
            ``is_error`` set.

        Raises:
            MCPError: If the name is empty or ``list_tools``. The code is
                ``INVALID_PARAMS``.
            CancelledError: If the operation or the Workflow is cancelled.
        """
        if not name or name == _LIST_TOOLS_OPERATION:
            raise MCPError(types.INVALID_PARAMS, f"Unknown tool {name!r}")
        try:
            result = await self._execute(name, dict(arguments or {}))
        except Exception as exc:
            if is_cancelled_exception(exc):
                raise
            return _error_result(_failure_message(exc))
        return _to_call_tool_result(result)

    async def _execute(self, operation: str, argument: Any) -> Any:
        client = workflow.create_nexus_client(
            service=self._service, endpoint=self._endpoint
        )
        return await client.execute_operation(operation, argument)


def _to_call_tool_result(value: Any) -> types.CallToolResult:
    """Map a Nexus operation result to an MCP tool result.

    The inbound connector uses the same rules:

    - A dict with only the ``io.temporal/mcpCallToolResult`` key becomes the
      tool result in it.
    - A dict becomes structured content plus its JSON text.
    - A string becomes text content.
    - ``None`` becomes empty text content.
    - Other values become their JSON text.

    Args:
        value: The operation result.

    Returns:
        The MCP tool result.
    """
    if isinstance(value, dict):
        if set(value) == {_RESULT_ENVELOPE_KEY}:  # pyright: ignore[reportUnknownArgumentType]
            return types.CallToolResult.model_validate(value[_RESULT_ENVELOPE_KEY])
        structured: dict[str, Any] = dict(value)  # pyright: ignore[reportUnknownArgumentType]
        return types.CallToolResult(
            content=[
                types.TextContent(type="text", text=json.dumps(structured, indent=2))
            ],
            structured_content=structured,
        )
    if isinstance(value, str):
        text = value
    elif value is None:
        text = ""
    else:
        text = json.dumps(value)
    return types.CallToolResult(content=[types.TextContent(type="text", text=text)])


def _failure_message(exc: BaseException) -> str:
    """Return the message of the innermost cause of exc.

    A Nexus operation error wraps the handler failure in one or more causes.
    The outer messages describe the operation and the failure types, not the
    tool error.
    """
    innermost = exc
    while innermost.__cause__ is not None:
        innermost = innermost.__cause__
    if isinstance(innermost, FailureError) and innermost.message:
        return innermost.message
    return str(innermost)


def _error_result(message: str) -> types.CallToolResult:
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=message)], is_error=True
    )
