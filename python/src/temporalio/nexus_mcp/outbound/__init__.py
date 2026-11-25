"""Run calls to upstream MCP servers via Standalone Activities.

:class:`MCPProxyPlugin` registers one Nexus service per :class:`Upstream`.
Each service lists the upstream tools and runs each tool call as a standalone
Activity.

Each service has two handlers:

- ``list_tools`` calls the upstream ``tools/list``.
- A dynamic handler takes every other operation. The operation name is the
  upstream tool name. It starts a standalone Activity that calls the upstream
  ``tools/call``, and returns the Activity as an async Nexus operation.
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
