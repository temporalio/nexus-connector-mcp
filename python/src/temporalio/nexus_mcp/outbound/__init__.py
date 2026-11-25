"""Run calls to upstream MCP servers via Standalone Activities.

:class:`MCPProxyPlugin` registers one Nexus service per :class:`Upstream`.
Each service has a ``list_tools`` operation that calls the upstream
``tools/list``.
"""

from ._config import ClientFactory, FailureClassification, Options, Upstream
from ._plugin import MCPProxyPlugin

__all__ = [
    "ClientFactory",
    "FailureClassification",
    "MCPProxyPlugin",
    "Options",
    "Upstream",
]
