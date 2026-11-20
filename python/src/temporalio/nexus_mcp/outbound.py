"""Run calls to upstream MCP servers as Temporal Activities.

:class:`MCPProxyPlugin` registers one Nexus service per :class:`Upstream`.
Each service lists the upstream tools and runs each tool call as a standalone
Activity.

Each service has two handlers:

- ``list_tools`` calls the upstream ``tools/list`` directly. It returns the
  last successful tool list when discovery fails.
- A dynamic handler takes every other operation. The operation name is the
  upstream tool name. It starts a standalone Activity that calls the upstream
  ``tools/call``, and returns the Activity as an async Nexus operation.
"""

import asyncio
import hashlib
import logging
import random
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, NoReturn

import anyio
import httpx2
import nexusrpc
import nexusrpc.handler
import pydantic
from mcp.client import Client
from mcp.shared.exceptions import MCPError
from mcp.types import CONNECTION_CLOSED, REQUEST_TIMEOUT, Tool

import temporalio.plugin
from temporalio import activity
from temporalio.client import (
    ActivityExecutionStatus,
    ActivityFailureError,
    ActivityHandle,
)
from temporalio.common import (
    ActivityIDConflictPolicy,
    ActivityIDReusePolicy,
    RetryPolicy,
)
from temporalio.exceptions import (
    ActivityAlreadyStartedError,
    ApplicationError,
    is_cancelled_exception,
)
from temporalio.nexus import (
    TemporalNexusClient,
    TemporalOperationResult,
    TemporalStartOperationContext,
    temporal_operation,
)
from temporalio.nexus import info as nexus_info
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Worker

__all__ = [
    "ClientFactory",
    "FailureClassification",
    "MCPProxyPlugin",
    "Options",
    "Upstream",
]

_logger = logging.getLogger(__name__)

ClientFactory = Callable[[], AbstractAsyncContextManager[Client]]
"""Connects to an upstream MCP server over HTTP or stdio.

The factory owns the transport, destination, and credentials. The proxy enters
it once for each discovery and once for each tool call attempt, and exits it
after that call. A stdio factory starts one process for each call.
"""

_LIST_TOOLS_OPERATION = "list_tools"
# Names with this prefix are reserved for Temporal.
_RESERVED_PREFIX = "__temporal_"
# A result with only this key is already an MCP tool result. The inbound
# connector and the Workflow client keep its content, _meta, and isError.
_RESULT_ENVELOPE_KEY = "io.temporal/mcpCallToolResult"
_DEFAULT_START_TO_CLOSE_TIMEOUT = timedelta(minutes=1)
# Limits one discovery, and the connection setup of one tool call.
_SETUP_TIMEOUT = timedelta(seconds=10)
_MIN_DISCOVERY_RETRY_DELAY = 1.0
_MAX_DISCOVERY_RETRY_DELAY = 60.0
_MAX_HEARTBEAT_INTERVAL = 3.0
_ACTIVITY_ID_PREFIX = "mcp-outbound-"
# The default Temporal limit for IDs, in bytes.
_MAX_ACTIVITY_ID_BYTES = 1000

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

# Failure types of the built-in classification.
_FAILURE_AUTHENTICATION = "UpstreamAuthenticationError"
_FAILURE_CONFIGURATION = "ProxyConfigurationError"
_FAILURE_PROTOCOL = "UpstreamProtocolError"
_FAILURE_CONNECTION = "UpstreamConnectionError"
_FAILURE_RATE_LIMIT = "UpstreamRateLimitError"
_FAILURE_HTTP = "UpstreamHTTPError"
_FAILURE_OTHER = "UpstreamError"


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
        for name in overrides:
            if not _is_tool_name(name):
                raise ValueError(
                    f"Upstream {self.service!r}: tool name {name!r} in "
                    "activity_options_overrides must be non-empty and not reserved"
                )
            _options_for(self, name, None)


@dataclass(frozen=True, kw_only=True)
class Options:
    """Configuration for :class:`MCPProxyPlugin`.

    Attributes:
        upstreams: The upstream MCP servers. Each one maps to its own Nexus
            service.
    """

    upstreams: Sequence[Upstream]


class MCPProxyPlugin(temporalio.plugin.SimplePlugin):
    """Worker plugin that registers the proxy Nexus services and Activities.

    Workers that share the plugin share its tool lists. Factories and hooks
    must support concurrent calls.
    """

    def __init__(self, *, options: Options) -> None:
        """Create the plugin.

        Args:
            options: The proxy configuration.

        Raises:
            ValueError: If there is no upstream, or two upstreams have the same
                service name.
        """
        upstreams = tuple(options.upstreams)
        if not upstreams:
            raise ValueError("Options.upstreams needs at least one Upstream")
        services = [u.service for u in upstreams]
        if len(set(services)) != len(services):
            raise ValueError("Each Upstream needs a distinct service name")
        self._catalogs = {u.service: _Catalog(u) for u in upstreams}
        activities = [_call_activity(u) for u in upstreams]
        handlers = [
            _service_handler(u, self._catalogs[u.service], fn)
            for u, fn in zip(upstreams, activities, strict=True)
        ]
        super().__init__(
            "temporalio.nexus_mcp.outbound.MCPProxyPlugin",
            activities=activities,
            nexus_service_handlers=handlers,
        )

    async def run_worker(
        self, worker: Worker, next: Callable[[Worker], Awaitable[None]]
    ) -> None:
        """Run discovery in the background while the Worker runs.

        The Worker starts while an upstream is down. Discovery stops when the
        Worker stops.

        Args:
            worker: The Worker.
            next: The next run step.
        """
        tasks = [
            asyncio.create_task(c.refresh_and_log()) for c in self._catalogs.values()
        ]
        try:
            await super().run_worker(worker, next)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def _is_tool_name(name: str) -> bool:
    return (
        bool(name.strip())
        and name != _LIST_TOOLS_OPERATION
        and not name.startswith(_RESERVED_PREFIX)
    )


def _options_for(
    upstream: Upstream, tool: str, annotations: Mapping[str, Any] | None
) -> dict[str, Any]:
    """Return the start_activity options of a new call to a tool.

    It applies the proxy defaults, then the tool override. The default retry
    policy depends on the tool annotations. A tool with
    ``destructiveHint=true``, ``readOnlyHint=false``, and
    ``idempotentHint=false`` gets one attempt. Other tools use the Temporal
    default retry policy.
    """
    annotations = annotations or {}
    options: dict[str, Any] = {
        "start_to_close_timeout": _DEFAULT_START_TO_CLOSE_TIMEOUT
    }
    if (
        annotations.get("readOnlyHint") is not True
        and annotations.get("destructiveHint") is True
        and annotations.get("idempotentHint") is not True
    ):
        options["retry_policy"] = RetryPolicy(maximum_attempts=1)
    override = upstream.activity_options_overrides.get(tool, {})
    for name, value in override.items():
        if name not in _ALLOWED_ACTIVITY_OPTIONS:
            raise TypeError(
                f"Upstream {upstream.service!r}: tool {tool!r}: the proxy sets {name!r}, "
                "or start_activity does not support it"
            )
        if isinstance(value, timedelta) and value < timedelta():
            raise ValueError(
                f"Upstream {upstream.service!r}: tool {tool!r}: {name} must not be negative"
            )
    options.update(override)
    return options


def _activity_id(endpoint: str, service: str, operation: str, request_id: str) -> str:
    """Return ``mcp-outbound-{endpoint}-{service}-{operation}-{request_id}``.

    The Nexus request ID is unique, so the ID needs no escaping. An ID over
    the Temporal limit keeps its first bytes and gets the SHA-256 of the full
    ID, so it stays unique and within the limit.
    """
    raw = f"{_ACTIVITY_ID_PREFIX}{endpoint}-{service}-{operation}-{request_id}"
    data = raw.encode()
    if len(data) <= _MAX_ACTIVITY_ID_BYTES:
        return raw
    # 65 bytes hold "-" and the hex hash.
    head = data[: _MAX_ACTIVITY_ID_BYTES - 65].decode(errors="ignore")
    return f"{head}-{hashlib.sha256(data).hexdigest()}"


class _Catalog:
    """The last complete tool list of one upstream.

    A published list is never changed, so readers share it without copies.
    """

    def __init__(self, upstream: Upstream) -> None:
        self.upstream = upstream
        self.tools: dict[str, dict[str, Any]] = {}
        self.discovered = False
        # Lets one discovery run at a time.
        self._lock = asyncio.Lock()
        self._retry_at = 0.0
        self._retry_delay = 0.0

    async def refresh(self) -> None:
        """Read all tool-list pages of the upstream.

        It replaces the tool list only when discovery succeeds. After a
        failure, it backs off with jitter, and calls during the backoff return
        without discovery.
        """
        async with self._lock:
            if time.monotonic() < self._retry_at:
                return
            try:
                async with asyncio.timeout(_SETUP_TIMEOUT.total_seconds()):
                    tools = await self._read_tools()
            except Exception:
                self._retry_delay = min(
                    max(_MIN_DISCOVERY_RETRY_DELAY, self._retry_delay * 2),
                    _MAX_DISCOVERY_RETRY_DELAY,
                )
                self._retry_at = time.monotonic() + random.uniform(
                    self._retry_delay / 2, self._retry_delay
                )
                raise
            self.tools, self.discovered = tools, True
            self._retry_at = self._retry_delay = 0.0

    async def _read_tools(self) -> dict[str, dict[str, Any]]:
        tools: dict[str, dict[str, Any]] = {}
        async with self.upstream.client_factory() as client:
            cursor: str | None = None
            while True:
                page = await client.list_tools(cursor=cursor, cache_mode="bypass")
                for value in page.tools:
                    tool = Tool.model_validate(value)
                    if not _is_tool_name(tool.name):
                        raise ValueError(
                            "upstream returned an empty or reserved tool name"
                        )
                    if tool.name in tools:
                        raise ValueError(
                            f"upstream returned tool {tool.name!r} more than once"
                        )
                    tools[tool.name] = tool.model_dump(
                        mode="json", by_alias=True, exclude_none=True
                    )
                cursor = page.next_cursor
                if not cursor:
                    return tools

    async def refresh_and_log(self) -> None:
        """Run refresh and log a failure.

        The log leaves out the error, because upstream errors can hold
        credentials and private URLs.
        """
        try:
            await self.refresh()
        except Exception:
            _logger.warning(
                "Could not discover upstream MCP tools for service %s; using the last successful tool list",
                self.upstream.service,
            )

    async def list_tools(self) -> list[dict[str, Any]]:
        """Run discovery and return the last successful tool list.

        Raises:
            nexusrpc.HandlerError: If discovery never succeeded.
        """
        await self.refresh_and_log()
        if not self.discovered:
            _raise_unavailable()
        return list(self.tools.values())


def _raise_unavailable() -> NoReturn:
    raise nexusrpc.HandlerError(
        "upstream tool list is not available",
        type=nexusrpc.HandlerErrorType.UNAVAILABLE,
    )


@dataclass
class _CallInput:
    """Input of the call Activity.

    Temporal records it, so it holds no destination or credentials.
    """

    tool: str
    arguments: dict[str, Any] = field(default_factory=dict[str, Any])


def _service_handler(
    upstream: Upstream,
    catalog: _Catalog,
    call: Callable[..., Awaitable[dict[str, Any]]],
) -> object:
    """Return the Nexus service handler of an upstream.

    Discovery changes the catalog, not the handlers.
    """

    @nexusrpc.handler.service_handler(name=upstream.service)
    class _ProxyService:
        @nexusrpc.handler.sync_operation(name=_LIST_TOOLS_OPERATION)
        async def list_tools(
            self, ctx: nexusrpc.handler.StartOperationContext, input: None
        ) -> dict[str, Any]:
            return {"tools": await catalog.list_tools()}

        # Cancellation of a tool call uses the default handler, which cancels
        # the Activity by its operation token without a tool lookup.
        @temporal_operation(dynamic=True)
        async def call_tool(
            self,
            ctx: TemporalStartOperationContext,
            client: TemporalNexusClient,
            input: nexusrpc.LazyValue,
        ) -> TemporalOperationResult[Any]:
            try:
                arguments: object = await input.consume(as_type=dict)
            except (ValueError, TypeError):
                raise nexusrpc.HandlerError(
                    "tool arguments must be a JSON object",
                    type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                ) from None
            return await _start_tool(upstream, catalog, call, ctx, client, arguments)

    return _ProxyService()


async def _start_tool(
    upstream: Upstream,
    catalog: _Catalog,
    call: Callable[..., Awaitable[dict[str, Any]]],
    ctx: TemporalStartOperationContext,
    client: TemporalNexusClient,
    arguments: object,
) -> TemporalOperationResult[Any]:
    """Start the call Activity for the tool named by the Nexus operation.

    A retried start has the same Activity ID. It reattaches to the Activity
    before it reads the tool list, so it works after the tool is removed or
    while discovery fails. A running Activity keeps its recorded options. A
    closed Activity returns its outcome: Temporal answers a start with the
    same request ID with the first response, so a new start would return a
    token whose callback already ran.
    """
    name = ctx.operation
    if not _is_tool_name(name):
        raise nexusrpc.HandlerError(
            "invalid or reserved tool name", type=nexusrpc.HandlerErrorType.NOT_FOUND
        )
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        raise nexusrpc.HandlerError(
            "tool arguments must be a JSON object",
            type=nexusrpc.HandlerErrorType.BAD_REQUEST,
        )
    activity_id = _activity_id(nexus_info().endpoint, ctx.service, name, ctx.request_id)
    handle = client.client.get_activity_handle(activity_id)
    options: dict[str, Any]
    try:
        existing = await handle.describe()
    except RPCError as err:
        if err.status != RPCStatusCode.NOT_FOUND:
            raise
        options = _new_call_options(upstream, catalog, name)
    else:
        if existing.status != ActivityExecutionStatus.RUNNING:
            return await _closed_result(handle)
        options = {
            "start_to_close_timeout": existing.start_to_close_timeout,
            "schedule_to_close_timeout": existing.schedule_to_close_timeout,
        }
    try:
        return await client.start_activity(
            call,
            _CallInput(tool=name, arguments=dict(arguments)),  # pyright: ignore[reportUnknownArgumentType]
            id=activity_id,
            id_conflict_policy=ActivityIDConflictPolicy.USE_EXISTING,
            id_reuse_policy=ActivityIDReusePolicy.REJECT_DUPLICATE,
            **options,
        )
    except ActivityAlreadyStartedError:
        # The Activity closed between describe and the start.
        return await _closed_result(handle)


def _new_call_options(
    upstream: Upstream, catalog: _Catalog, name: str
) -> dict[str, Any]:
    if not catalog.discovered:
        _raise_unavailable()
    tool = catalog.tools.get(name)
    if tool is None:
        raise nexusrpc.HandlerError(
            "unknown upstream tool", type=nexusrpc.HandlerErrorType.NOT_FOUND
        )
    return _options_for(upstream, name, tool.get("annotations"))


async def _closed_result(handle: ActivityHandle[Any]) -> TemporalOperationResult[Any]:
    """Return the outcome of a closed Activity as a sync result."""
    try:
        return TemporalOperationResult.sync(await handle.result())
    except ActivityFailureError as err:
        if is_cancelled_exception(err):
            raise nexusrpc.OperationError(
                "upstream call cancelled", state=nexusrpc.OperationErrorState.CANCELED
            ) from None
        # Keep the failure type. The message can hold upstream data, so it
        # stays out.
        message = "upstream call failed"
        if isinstance(err.cause, ApplicationError) and err.cause.type:
            message += f": {err.cause.type}"
        raise nexusrpc.OperationError(
            message, state=nexusrpc.OperationErrorState.FAILED
        ) from None


def _activity_name(service: str) -> str:
    return f"mcp-outbound.{service}.call_upstream_tool"


def _call_activity(upstream: Upstream) -> Callable[..., Awaitable[dict[str, Any]]]:
    """Return the call Activity of an upstream.

    It calls the upstream ``tools/call`` and returns the tool result in the
    result envelope. A tool result with ``isError`` is a result, not a
    failure.
    """

    @activity.defn(name=_activity_name(upstream.service))
    async def call_upstream_tool(call: _CallInput) -> dict[str, Any]:
        info = activity.info()
        heartbeat = None
        if info.heartbeat_timeout:
            interval = min(
                _MAX_HEARTBEAT_INTERVAL, info.heartbeat_timeout.total_seconds() / 3
            )
            heartbeat = asyncio.create_task(_heartbeat(interval))
        try:
            return await _call_upstream(upstream, call, _remaining_time(info))
        except BaseException as err:
            leaf = _first_leaf(err)
            # Cancellation skips classification.
            if not isinstance(leaf, Exception) or is_cancelled_exception(leaf):
                raise leaf from None
            classification = _classify(upstream, leaf)
            # The failure holds only the type. Upstream messages, response
            # bodies, and URLs can hold private data, and Temporal records the
            # failure.
            raise ApplicationError(
                classification.type,
                type=classification.type,
                non_retryable=not classification.retryable,
                next_retry_delay=_retry_after(leaf),
            ) from None
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)

    return call_upstream_tool


async def _call_upstream(
    upstream: Upstream, call: _CallInput, timeout: float | None
) -> dict[str, Any]:
    """Open a session, call the tool, and close the session.

    The Activity deadline limits the call. Connection setup has its own
    shorter deadline.
    """
    async with asyncio.timeout(timeout), AsyncExitStack() as stack:
        async with asyncio.timeout(_SETUP_TIMEOUT.total_seconds()):
            client = await stack.enter_async_context(upstream.client_factory())
        result = await client.call_tool(call.tool, call.arguments)
    # The proxy does not support upstream tasks or input requests yet, so it
    # rejects them. It does not treat them as a complete tool result.
    if result.result_type != "complete":
        raise _ProtocolError
    return {
        _RESULT_ENVELOPE_KEY: result.model_dump(
            mode="json", by_alias=True, exclude_none=True
        )
    }


class _ProtocolError(Exception):
    """An upstream result that the proxy cannot use."""

    def __init__(self) -> None:
        super().__init__("invalid or incomplete upstream MCP result")


def _first_leaf(err: BaseException) -> BaseException:
    """Return the first exception in an exception group.

    A cancellation in the group comes first, so it does not become a
    retryable upstream failure.
    """
    if isinstance(err, BaseExceptionGroup):
        leaves = [_first_leaf(child) for child in err.exceptions]  # pyright: ignore[reportUnknownVariableType, reportUnknownMemberType]
        for leaf in leaves:
            if isinstance(leaf, asyncio.CancelledError):
                return leaf
        return leaves[0]
    return err


def _default_classification(err: BaseException) -> FailureClassification:
    """Return the built-in classification of a failed upstream call."""
    if isinstance(err, MCPError):
        if err.error.code in (CONNECTION_CLOSED, REQUEST_TIMEOUT):
            return FailureClassification(_FAILURE_CONNECTION, True)
        return FailureClassification(_FAILURE_PROTOCOL, False)
    if isinstance(err, httpx2.HTTPStatusError):
        status = err.response.status_code
        if status in (401, 403):
            return FailureClassification(_FAILURE_AUTHENTICATION, False)
        if status == 429:
            return FailureClassification(_FAILURE_RATE_LIMIT, True)
        return FailureClassification(_FAILURE_HTTP, status >= 500 or status == 408)
    if isinstance(err, PermissionError):
        return FailureClassification(_FAILURE_AUTHENTICATION, False)
    if isinstance(err, (FileNotFoundError, NotADirectoryError, IsADirectoryError)):
        return FailureClassification(_FAILURE_CONFIGURATION, False)
    if isinstance(
        err, (_ProtocolError, pydantic.ValidationError, ValueError, TypeError)
    ):
        return FailureClassification(_FAILURE_PROTOCOL, False)
    if isinstance(
        err,
        (
            ConnectionError,
            TimeoutError,
            OSError,
            httpx2.TransportError,
            anyio.EndOfStream,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ),
    ):
        return FailureClassification(_FAILURE_CONNECTION, True)
    return FailureClassification(_FAILURE_OTHER, False)


def _classify(upstream: Upstream, err: Exception) -> FailureClassification:
    """Apply Upstream.classify_error to the built-in classification.

    A hook that raises or returns a result without a type gives a
    non-retryable configuration failure.
    """
    classification = _default_classification(err)
    if upstream.classify_error is None:
        return classification
    try:
        result = upstream.classify_error(err, classification)
    except Exception:
        return FailureClassification(_FAILURE_CONFIGURATION, False)
    if not isinstance(result, FailureClassification) or not result.type:  # pyright: ignore[reportUnnecessaryIsInstance]
        return FailureClassification(_FAILURE_CONFIGURATION, False)
    return result


def _retry_after(err: Exception) -> timedelta | None:
    """Return the Retry-After delay of an HTTP error, if it has one."""
    if not isinstance(err, httpx2.HTTPStatusError):
        return None
    value = err.response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return timedelta(seconds=max(0.0, float(value)))
    except ValueError:
        pass
    try:
        return max(timedelta(), parsedate_to_datetime(value) - datetime.now(UTC))
    except (ValueError, TypeError):
        return None


def _remaining_time(info: activity.Info) -> float | None:
    """Return the seconds until the Activity deadline, or None."""
    deadlines: list[datetime] = []
    if info.start_to_close_timeout and info.started_time:
        deadlines.append(info.started_time + info.start_to_close_timeout)
    if info.schedule_to_close_timeout and info.scheduled_time:
        deadlines.append(info.scheduled_time + info.schedule_to_close_timeout)
    if not deadlines:
        return None
    return max(0.0, (min(deadlines) - datetime.now(UTC)).total_seconds())


async def _heartbeat(interval: float) -> None:
    """Heartbeat until cancelled.

    Heartbeats let the Worker receive cancellation and detect a lost Worker
    before the attempt timeout.
    """
    while True:
        activity.heartbeat()
        await asyncio.sleep(interval)
