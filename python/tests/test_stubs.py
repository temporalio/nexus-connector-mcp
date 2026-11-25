"""The public API is declared but not implemented."""

from contextlib import AbstractAsyncContextManager

import pytest
from mcp.client import Client

from temporalio.nexus_mcp import outbound


def _client_factory() -> AbstractAsyncContextManager[Client]:
    raise NotImplementedError


def test_mcp_proxy_plugin_not_implemented() -> None:
    options = outbound.Options(
        upstreams=[outbound.Upstream(service="example", client_factory=_client_factory)]
    )
    with pytest.raises(NotImplementedError):
        outbound.MCPProxyPlugin(options=options)
