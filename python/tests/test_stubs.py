"""The public API is declared but not implemented."""

from contextlib import AbstractAsyncContextManager

import pytest
from mcp.client import Client

from temporalio import nexus_mcp
from temporalio.nexus_mcp import handler, outbound


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


def _client_factory() -> AbstractAsyncContextManager[Client]:
    raise NotImplementedError


def test_mcp_proxy_plugin_not_implemented() -> None:
    options = outbound.Options(
        upstreams=[outbound.Upstream(service="example", client_factory=_client_factory)]
    )
    with pytest.raises(NotImplementedError):
        outbound.MCPProxyPlugin(options=options)
