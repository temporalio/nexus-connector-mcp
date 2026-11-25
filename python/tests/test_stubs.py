"""The public API is declared but not implemented."""

import pytest

from temporalio import nexus_mcp
from temporalio.nexus_mcp import handler, workflow


class _Service:
    pass


def test_service_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        nexus_mcp.service(_Service)


def test_service_handler_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        handler.service_handler(_Service)


def test_tool_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        handler.tool(title="Example")


def test_nexus_mcp_client_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        workflow.NexusMCPClient(service="example", endpoint="example-endpoint")
