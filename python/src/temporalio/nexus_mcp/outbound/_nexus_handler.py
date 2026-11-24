"""Nexus service that wraps an upstream MCP server.

This turns an MCP server into a Nexus service with Nexus operation
handlers for each tool, alongside a list_tools Nexus operation
handler to list all tools available upstream.

TODO(long-nt-tran): investigate smart type inference here based on
in/out schemas + past tool calls. This would allow us to provide
smartly typed APIs given an upstream MCP spec.
"""

from typing import Any

import nexusrpc
import nexusrpc.handler

from temporalio import nexus

from ._config import LIST_TOOLS_OPERATION, Upstream
from ._mcp_client import list_tools


def service_handler(upstream: Upstream) -> object:
    """Return the Nexus service of an upstream.

    list_tools calls the upstream tools/list.
    """

    @nexusrpc.handler.service_handler(name=upstream.service)
    class _ProxyService:
        @nexusrpc.handler.sync_operation(name=LIST_TOOLS_OPERATION)
        async def list_tools(
            self, ctx: nexusrpc.handler.StartOperationContext, input: None
        ) -> dict[str, Any]:
            return {"tools": await _handle_list_tools(upstream)}

    return _ProxyService()


async def _handle_list_tools(upstream: Upstream) -> list[dict[str, Any]]:
    """Run discovery for each call.

    A failure gives an unavailable handler error. The log and the error leave
    out the cause, because upstream errors can hold credentials and private
    URLs.

    Raises:
        nexusrpc.HandlerError: If the upstream call fails.
    """
    # TODO(long-nt-tran): cache the last successful tool list, and use it before
    # calling upstream (similar to what MCP SDK does).
    try:
        return list((await list_tools(upstream)).values())
    # Any upstream error makes the tool list unavailable.
    except Exception:  # noqa: BLE001
        # The Temporal Nexus logger adds the service and operation to the log.
        nexus.logger.warning(
            "Could not discover upstream MCP tools for service %s", upstream.service
        )
        raise nexusrpc.HandlerError(
            "upstream tool list is not available",
            type=nexusrpc.HandlerErrorType.UNAVAILABLE,
        ) from None
