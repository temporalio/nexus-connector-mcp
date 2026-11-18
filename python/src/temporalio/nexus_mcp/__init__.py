"""Expose Temporal Nexus operations as MCP tools.

This module is safe to import in Workflow code. Handler APIs are in
:mod:`temporalio.nexus_mcp.handler`. The Workflow client is in
:mod:`temporalio.nexus_mcp.workflow`. The upstream MCP proxy is in
:mod:`temporalio.nexus_mcp.outbound`.
"""

from typing import TypeVar

__all__ = ["service"]

ServiceT = TypeVar("ServiceT", bound=type)


def service(cls: ServiceT) -> ServiceT:
    """Add the ``list_tools`` operation to a Nexus service definition.

    Apply it below ``@nexusrpc.service``.

    Args:
        cls: The Nexus service definition class.

    Returns:
        The service definition with a ``list_tools`` operation.
    """
    raise NotImplementedError("temporalio.nexus_mcp.service is not implemented")
