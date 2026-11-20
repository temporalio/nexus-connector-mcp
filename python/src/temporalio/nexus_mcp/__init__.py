"""Expose Temporal Nexus operations as MCP tools.

This module is safe to import in Workflow code. Handler APIs are in
:mod:`temporalio.nexus_mcp.handler`. The Workflow client is in
:mod:`temporalio.nexus_mcp.workflow`. The upstream MCP proxy is in
:mod:`temporalio.nexus_mcp.outbound`.
"""

from typing import Any, TypeVar

import nexusrpc

__all__ = ["service"]

ServiceT = TypeVar("ServiceT", bound=type)

_LIST_TOOLS_OPERATION = "list_tools"


def service(cls: ServiceT) -> ServiceT:
    """Add the ``list_tools`` operation to a Nexus service definition.

    Apply it below ``@nexusrpc.service``. ``list_tools`` takes no input and
    returns ``{"tools": [...]}``, an MCP ``tools/list`` result.

    Args:
        cls: The Nexus service definition class.

    Returns:
        The service definition with a ``list_tools`` operation.

    Raises:
        TypeError: If it is applied above ``@nexusrpc.service``, or the class
            already declares ``list_tools``.
    """
    if nexusrpc.get_service_definition(cls) is not None:
        raise TypeError(
            f"{cls.__name__}: apply @nexus_mcp.service below @nexusrpc.service"
        )
    if _LIST_TOOLS_OPERATION in vars(cls) or _LIST_TOOLS_OPERATION in getattr(
        cls, "__annotations__", {}
    ):
        raise TypeError(
            f"{cls.__name__} must not declare {_LIST_TOOLS_OPERATION}; "
            "@nexus_mcp.service adds it"
        )
    setattr(
        cls,
        _LIST_TOOLS_OPERATION,
        nexusrpc.Operation[None, dict[str, Any]](
            name=_LIST_TOOLS_OPERATION,
            input_type=type(None),
            output_type=dict[str, Any],
        ),
    )
    return cls
