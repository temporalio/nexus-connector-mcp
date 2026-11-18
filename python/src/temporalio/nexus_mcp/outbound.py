"""Run calls to upstream MCP servers as Temporal Activities.

:class:`MCPProxyPlugin` registers one Nexus service per :class:`Upstream`.
Each service lists the upstream tools and runs each tool call as a standalone
Activity.
"""

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Any

from mcp.client import Client

import temporalio.plugin

__all__ = [
    "ClientFactory",
    "FailureClassification",
    "MCPProxyPlugin",
    "Options",
    "Upstream",
]

ClientFactory = Callable[[], AbstractAsyncContextManager[Client]]
"""Connects to an upstream MCP server over HTTP or stdio.

The factory owns the transport, destination, and credentials.
"""


@dataclass(frozen=True)
class FailureClassification:
    """How a failed upstream call is reported to Temporal.

    Attributes:
        type: A stable failure type.
        retryable: Whether the Activity retry policy applies.
    """

    type: str
    retryable: bool


@dataclass(frozen=True, kw_only=True)
class Upstream:
    """One upstream MCP server.

    Attributes:
        service: The Nexus service name for this upstream.
        client_factory: Connects to the upstream.
        activity_options_overrides: Maps an upstream tool name to
            ``TemporalNexusClient.start_activity`` keyword options for that
            tool. Omitted keys use the proxy defaults. Connector-owned keys
            are rejected: Activity ID, task queue, conflict and reuse
            policies, and RPC settings.
        classify_error: Optionally overrides the built-in classification of
            a failed upstream call. Cancellation skips this hook.
    """

    service: str
    client_factory: ClientFactory
    activity_options_overrides: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict[str, Mapping[str, Any]]
    )
    classify_error: (
        Callable[[BaseException, FailureClassification], FailureClassification] | None
    ) = None


@dataclass(frozen=True, kw_only=True)
class Options:
    """Configuration for :class:`MCPProxyPlugin`.

    Attributes:
        upstreams: The upstream MCP servers. Each one maps to its own Nexus
            service.
    """

    upstreams: Sequence[Upstream]


class MCPProxyPlugin(temporalio.plugin.SimplePlugin):
    """Worker plugin that registers the proxy Nexus services and Activities."""

    def __init__(self, *, options: Options) -> None:
        """Create the plugin.

        Args:
            options: The proxy configuration.
        """
        super().__init__("temporalio.nexus_mcp.outbound.MCPProxyPlugin")
        raise NotImplementedError(
            "temporalio.nexus_mcp.outbound.MCPProxyPlugin is not implemented"
        )
