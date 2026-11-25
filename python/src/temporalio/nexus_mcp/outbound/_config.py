"""Public configuration types of the outbound proxy."""

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import Any

from mcp.client import Client

ClientFactory = Callable[[], AbstractAsyncContextManager[Client]]
"""Connects to an upstream MCP server over HTTP or stdio.

The factory owns the transport, destination, and credentials.
"""

LIST_TOOLS_OPERATION = "list_tools"

# Names with this prefix are reserved for Temporal.
_RESERVED_PREFIX = "__temporal_"


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
            ``TemporalNexusClient.start_activity`` kwargs options for that
            tool. Omitted keys use the proxy defaults. Connector-owned keys
            are rejected: Activity ID, task queue, conflict and reuse
            policies, and RPC settings. Not implemented yet.
        classify_error: Optionally overrides the built-in classification of
            a failed upstream call. Cancellation skips this hook. Not
            implemented yet.
    """

    service: str
    client_factory: ClientFactory
    activity_options_overrides: Mapping[str, Mapping[str, Any]] = field(
        default_factory=dict[str, Mapping[str, Any]]
    )
    classify_error: (
        Callable[[BaseException, FailureClassification], FailureClassification] | None
    ) = None

    def __post_init__(self) -> None:
        """Validate the upstream.

        Raises:
            ValueError: If the service name is not valid.
            TypeError: If the client factory is not callable.
        """
        if not self.service.strip() or self.service.startswith(_RESERVED_PREFIX):
            raise ValueError(
                f"Upstream service {self.service!r} must be non-empty and not reserved"
            )
        if not callable(self.client_factory):
            raise TypeError(
                f"Upstream {self.service!r}: client_factory must be callable"
            )


@dataclass(frozen=True, kw_only=True)
class Options:
    """Configuration for :class:`MCPProxyPlugin`.

    Attributes:
        upstreams: The upstream MCP servers. Each one maps to its own Nexus
            service.
    """

    upstreams: Sequence[Upstream]


def is_tool_name(name: str) -> bool:
    """Report whether name can be an upstream tool name.

    ``list_tools`` and names that start with ``__temporal_`` are reserved.
    """
    return (
        bool(name.strip())
        and name != LIST_TOOLS_OPERATION
        and not name.startswith(_RESERVED_PREFIX)
    )
