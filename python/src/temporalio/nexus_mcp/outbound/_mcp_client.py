"""Upstream MCP calls, handling actual HTTP calls and parsing results.

This module intentionally does not know about Temporal. The Temporal
Nexus handler delegates interfacing with the upstream MCP server to
this module.
"""

from typing import Any

from mcp.types import Tool

from ._config import Upstream, is_tool_name


async def list_tools(upstream: Upstream) -> dict[str, dict[str, Any]]:
    """Read all tool-list pages of the upstream using the MCP SDK.

    Raises:
        ValueError: If the upstream returns an empty or reserved tool name, or
            the same tool name more than once.
    """
    tools: dict[str, dict[str, Any]] = {}
    # Get a session to the upstream MCP server through the provided client
    # factory. For now, we open and close the connection on every call, which
    # should be ok since these are request-response type interactions, not
    # long-lived.
    async with upstream.client_factory() as client:
        cursor: str | None = None
        # We use the MCP SDK's list_tools(...) with cache_mode="bypass", which:
        # - skips the MCP client cache, and
        # - reads one page of the upstream MCP server's tools via a
        #   "tools/list" call. The loop reads every page.
        while True:
            page = await client.list_tools(cursor=cursor, cache_mode="bypass")
            for value in page.tools:
                tool = Tool.model_validate(value)
                if not is_tool_name(tool.name):
                    raise ValueError(
                        "upstream returned an empty or reserved tool name: "
                        f"{tool.name!r}"
                    )
                if tool.name in tools:
                    raise ValueError(
                        f"upstream returned tool {tool.name!r} more than once"
                    )
                tools[tool.name] = tool.model_dump(
                    mode="json", by_alias=True, exclude_none=True
                )
            cursor = page.next_cursor
            if not cursor:
                return tools
