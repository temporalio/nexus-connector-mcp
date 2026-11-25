"""Public configuration types of the outbound proxy."""

from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from mcp.client import Client

ClientFactory = Callable[[], AbstractAsyncContextManager[Client]]
"""Connects to an upstream MCP server over HTTP or stdio.

The factory owns the transport, destination, and credentials.
"""

LIST_TOOLS_OPERATION = "list_tools"

# Names with this prefix are reserved for Temporal.
_RESERVED_PREFIX = "__temporal_"

# start_activity keyword options that an override can set. The proxy sets the
# Activity ID, task queue, conflict and reuse policies, result type, and RPC
# settings.
_ALLOWED_ACTIVITY_OPTIONS = frozenset(
    {
        "start_to_close_timeout",
        "schedule_to_close_timeout",
        "schedule_to_start_timeout",
        "heartbeat_timeout",
        "start_delay",
        "retry_policy",
        "priority",
        "search_attributes",
        "summary",
    }
)


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
            tool. Omitted or None keys use the proxy defaults. Connector-owned keys
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

    def __post_init__(self) -> None:
        """Validate the upstream and copy the overrides.

        Raises:
            ValueError: If the service name, a tool name, or an option value is
                not valid.
            TypeError: If the client factory is not callable, or an override
                sets a connector-owned option.
        """
        if not self.service.strip() or self.service.startswith(_RESERVED_PREFIX):
            raise ValueError(
                f"Upstream service {self.service!r} must be non-empty and not reserved"
            )
        if not callable(self.client_factory):
            raise TypeError(
                f"Upstream {self.service!r}: client_factory must be callable"
            )
        # Copy, so later changes by the caller do not change the proxy.
        overrides = {
            name: dict(options)
            for name, options in self.activity_options_overrides.items()
        }
        object.__setattr__(self, "activity_options_overrides", overrides)
        for tool, options in overrides.items():
            if not is_tool_name(tool):
                raise ValueError(
                    f"Upstream {self.service!r}: tool name {tool!r} in "
                    "activity_options_overrides must be non-empty and not reserved"
                )
            for name, value in options.items():
                if name not in _ALLOWED_ACTIVITY_OPTIONS:
                    raise TypeError(
                        f"Upstream {self.service!r}: tool {tool!r}: the proxy sets {name!r}, "
                        "or start_activity does not support it"
                    )
                if isinstance(value, timedelta) and value < timedelta():
                    raise ValueError(
                        f"Upstream {self.service!r}: tool {tool!r}: {name} must not be negative"
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
