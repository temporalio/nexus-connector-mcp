"""Nexus handler APIs that expose operations as MCP tools.

Do not import this module in Workflow code.
"""

from collections.abc import Callable, Mapping
from typing import Any, Literal, TypeVar, overload

from mcp.types import ToolAnnotations

__all__ = ["service_handler", "tool"]

HandlerT = TypeVar("HandlerT", bound=type)
OperationT = TypeVar("OperationT", bound=Callable[..., Any])


@overload
def service_handler(cls: HandlerT, /) -> HandlerT: ...


@overload
def service_handler(
    *, expose: Literal["marked"] = "marked"
) -> Callable[[HandlerT], HandlerT]: ...


def service_handler(
    cls: HandlerT | None = None, /, *, expose: Literal["marked"] = "marked"
) -> HandlerT | Callable[[HandlerT], HandlerT]:
    """Implement the ``list_tools`` operation on a Nexus service handler.

    Apply it below ``@nexusrpc.handler.service_handler``. Use it bare or with
    arguments.

    Args:
        cls: The Nexus service handler class.
        expose: Which operations to list as tools. ``"marked"`` lists only
            operations marked with :func:`tool`.

    Returns:
        The service handler class, or a decorator that returns it.
    """
    raise NotImplementedError(
        "temporalio.nexus_mcp.handler.service_handler is not implemented"
    )


def tool(
    *,
    title: str | None = None,
    description: str | None = None,
    annotations: ToolAnnotations | None = None,
    meta: Mapping[str, Any] | None = None,
) -> Callable[[OperationT], OperationT]:
    """Mark a Nexus operation as an MCP tool.

    The tool name is the registered Nexus operation name. Apply it above the
    Nexus operation decorator.

    Args:
        title: Display label for the tool.
        description: Tool description. When unset or empty, the handler
            docstring is used if present.
        annotations: MCP tool annotations.
        meta: MCP ``_meta`` for the tool.

    Returns:
        A decorator that marks the operation.
    """
    raise NotImplementedError("temporalio.nexus_mcp.handler.tool is not implemented")
