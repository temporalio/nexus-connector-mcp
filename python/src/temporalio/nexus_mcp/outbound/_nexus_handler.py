"""Nexus service that wraps an upstream MCP server.

This turns an MCP server into a Nexus service with Nexus operation
handlers for each tool, alongside a list_tools Nexus operation
handler to list all tools available upstream. It also defines the
Activity that runs each tool call.

TODO(long-nt-tran): investigate smart type inference here based on
in/out schemas + past tool calls. This would allow us to provide
smartly typed APIs given an upstream MCP spec.
"""

import asyncio
import hashlib
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import anyio
import httpx2
import nexusrpc
import nexusrpc.handler
import pydantic
from mcp.shared.exceptions import MCPError
from mcp.types import CONNECTION_CLOSED, REQUEST_TIMEOUT

from temporalio import activity
from temporalio.client import (
    ActivityFailureError,
    ActivityHandle,
)
from temporalio.common import (
    ActivityIDConflictPolicy,
    ActivityIDReusePolicy,
    RawValue,
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

from ._config import (
    LIST_TOOLS_OPERATION,
    FailureClassification,
    Upstream,
    is_tool_name,
)
from ._mcp_client import ProtocolError, call_tool
from ._tool_cache import ToolCache, ToolListUnavailableError, ToolNotFoundError

# The proxy default for each Activity attempt.
_DEFAULT_START_TO_CLOSE_TIMEOUT = timedelta(minutes=1)
# Starts every Activity ID that the proxy generates.
_ACTIVITY_ID_PREFIX = "mcp-outbound-"
# The default Temporal limit for IDs, in bytes.
_MAX_ACTIVITY_ID_BYTES = 1000
# Limits the heartbeat interval, in seconds, of a call Activity that has a
# heartbeat timeout.
_MAX_HEARTBEAT_INTERVAL = 3.0
# Failure types of the built-in classification.
_FAILURE_AUTHENTICATION = "UpstreamAuthenticationError"
_FAILURE_CONFIGURATION = "ProxyConfigurationError"
_FAILURE_PROTOCOL = "UpstreamProtocolError"
_FAILURE_CONNECTION = "UpstreamConnectionError"
_FAILURE_OTHER = "UpstreamError"


def service_handler(
    upstream: Upstream,
    cache: ToolCache,
    call: Callable[..., Awaitable[dict[str, Any]]],
) -> object:
    """Return the Nexus service of an upstream.

    Discovery changes the tool cache, not the handlers.
    """

    @nexusrpc.handler.service_handler(name=upstream.service)
    class _ProxyService:
        @nexusrpc.handler.sync_operation(name=LIST_TOOLS_OPERATION)
        async def list_tools(
            self, ctx: nexusrpc.handler.StartOperationContext, input: None
        ) -> dict[str, Any]:
            try:
                return {"tools": await cache.list_tools()}
            except ToolListUnavailableError as err:
                raise nexusrpc.HandlerError(
                    str(err), type=nexusrpc.HandlerErrorType.UNAVAILABLE
                ) from None

        # Calling an upstream tool is handled by a dynamic handler, since we
        # don't know the names of the tools provided by an upstream server.
        # This dynamic handler allows the client to call a tool as a named
        # Nexus operation.
        #
        # The dynamic handler will then handle all of these tool calls and
        # delegate to _start_tool(...) which is an SAA-backed call.
        #
        # The reason we want client to be able to use a first-class Nexus
        # operation for tool call is for future Nexus operation features to
        # apply to those tool calls (vs. having a single 'call_tool' Nexus
        # operation that client invokes, in which case they lose
        # operation-level granularity provided by Nexus).
        #
        # Cancellation of a tool call uses the default handler, which cancels
        # the Activity by its operation token without a tool lookup.
        @temporal_operation(dynamic=True)
        async def call_tool(
            self,
            ctx: TemporalStartOperationContext,
            client: TemporalNexusClient,
            input: RawValue,
        ) -> TemporalOperationResult[Any]:
            # A dynamic handler gets the raw payload. The data converter of the
            # client decodes it, so a custom converter applies.
            try:
                arguments: object = (
                    client.client.data_converter.payload_converter.from_payload(
                        input.payload, dict
                    )
                )
            except (ValueError, TypeError):
                raise nexusrpc.HandlerError(
                    "tool arguments must be a JSON object",
                    type=nexusrpc.HandlerErrorType.BAD_REQUEST,
                ) from None
            # This is an async SAA-backed call, where the activity is the HTTP
            # call upstream.
            return await _start_tool(cache, call, ctx, client, arguments)

    return _ProxyService()


async def _start_tool(
    cache: ToolCache,
    call: Callable[..., Awaitable[dict[str, Any]]],
    ctx: TemporalStartOperationContext,
    client: TemporalNexusClient,
    arguments: object,
) -> TemporalOperationResult[Any]:
    """Start the call Activity for the tool named by the Nexus operation.

    A retried start uses the same Activity ID. USE_EXISTING attaches it to a
    running Activity. If the Activity already closed, the start fails with
    ActivityAlreadyStartedError, and _start_tool returns the Activity outcome.

    The tool must be in the tool cache. A cache miss refreshes the cache once,
    because the tool can be new upstream, or this Worker has not listed the
    tools yet. A cache hit makes no upstream call.
    """
    name = ctx.operation
    if not is_tool_name(name):
        raise nexusrpc.HandlerError(
            "invalid or reserved tool name", type=nexusrpc.HandlerErrorType.NOT_FOUND
        )
    # A call without input has no arguments. It becomes {}, as JSON null does.
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        raise nexusrpc.HandlerError(
            "tool arguments must be a JSON object",
            type=nexusrpc.HandlerErrorType.BAD_REQUEST,
        )
    try:
        await cache.find_tool(name)
    except ToolNotFoundError as err:
        raise nexusrpc.HandlerError(
            str(err), type=nexusrpc.HandlerErrorType.NOT_FOUND
        ) from None
    except ToolListUnavailableError as err:
        raise nexusrpc.HandlerError(
            str(err), type=nexusrpc.HandlerErrorType.UNAVAILABLE
        ) from None
    activity_id = _activity_id(nexus_info().endpoint, ctx.service, name, ctx.request_id)
    options: dict[str, Any] = {
        "id": activity_id,
        "start_to_close_timeout": _DEFAULT_START_TO_CLOSE_TIMEOUT,
        "id_conflict_policy": ActivityIDConflictPolicy.USE_EXISTING,
        "id_reuse_policy": ActivityIDReusePolicy.REJECT_DUPLICATE,
    }
    try:
        return await client.start_activity(
            call,
            _CallToolInput(tool=name, arguments=dict(arguments)),  # pyright: ignore[reportUnknownArgumentType]
            **options,
        )
    except ActivityAlreadyStartedError:
        # A retried start of an Activity that already closed. Return its
        # outcome.
        return await _closed_result(client.client.get_activity_handle(activity_id))


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


def _activity_name(service: str) -> str:
    """Return the name of the call Activity of an upstream."""
    return f"mcp-outbound.{service}.call_upstream_tool"


@dataclass
class _CallToolInput:
    """Input of the call Activity.

    Temporal records it, so it holds no destination or credentials.
    """

    tool: str
    arguments: dict[str, Any] = field(default_factory=dict[str, Any])


def call_tool_activity(upstream: Upstream) -> Callable[..., Awaitable[dict[str, Any]]]:
    """Return the call Activity of an upstream.

    It calls the upstream ``tools/call`` and returns the tool result in the
    result envelope. A tool result with ``isError`` is a result, not a
    failure.
    """

    @activity.defn(name=_activity_name(upstream.service))
    async def call_upstream_tool(call: _CallToolInput) -> dict[str, Any]:
        try:
            async with _heartbeats():
                return await call_tool(
                    upstream, call.tool, call.arguments, _remaining_time()
                )
        # Catch all, so an exception group from the MCP client gives its first
        # leaf. Cancellation and non-Exception errors are raised again below.
        except BaseException as err:  # noqa: BLE001
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
            ) from None

    return call_upstream_tool


@asynccontextmanager
async def _heartbeats() -> AsyncGenerator[None]:
    """Heartbeat while the call runs, if the Activity has a heartbeat timeout.

    Heartbeats let the Worker receive cancellation and detect a lost Worker
    before the attempt timeout. Leaving the context stops the heartbeats.

    NOTE: by default we do not heartbeat, unless the user overrides this
    setting intentionally.
    """
    # Heartbeating is off by default
    timeout = activity.info().heartbeat_timeout
    if not timeout:
        yield
        return

    interval = max(min(_MAX_HEARTBEAT_INTERVAL, timeout.total_seconds() / 3), 0.001)

    async def heartbeat() -> None:
        while True:
            await asyncio.sleep(interval)
            activity.heartbeat()

    task = asyncio.create_task(heartbeat())
    try:
        yield
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def _remaining_time() -> float | None:
    """Return the seconds until the Activity deadline, or None."""
    info = activity.info()
    deadlines: list[datetime] = []
    if info.start_to_close_timeout and info.started_time:
        deadlines.append(info.started_time + info.start_to_close_timeout)
    if info.schedule_to_close_timeout and info.scheduled_time:
        deadlines.append(info.scheduled_time + info.schedule_to_close_timeout)
    if not deadlines:
        return None
    return max(0.0, (min(deadlines) - datetime.now(UTC)).total_seconds())


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
    """Return the built-in classification of a failed upstream call.

    For now, only connection-related failures are retryable.
    """
    if isinstance(err, MCPError):
        if err.error.code in (CONNECTION_CLOSED, REQUEST_TIMEOUT):
            return FailureClassification(_FAILURE_CONNECTION, True)
        return FailureClassification(_FAILURE_PROTOCOL, False)
    if isinstance(err, httpx2.HTTPStatusError):
        status = err.response.status_code
        if status in (401, 403):
            return FailureClassification(_FAILURE_AUTHENTICATION, False)
        # TODO(long-nt-tran): retry HTTP 429, 408, and 5xx responses, and use
        # the Retry-After header as the next retry delay.
        return FailureClassification(_FAILURE_OTHER, False)
    if isinstance(err, PermissionError):
        return FailureClassification(_FAILURE_AUTHENTICATION, False)
    if isinstance(err, (FileNotFoundError, NotADirectoryError, IsADirectoryError)):
        return FailureClassification(_FAILURE_CONFIGURATION, False)
    if isinstance(
        err, (ProtocolError, pydantic.ValidationError, ValueError, TypeError)
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
        # For now, this is the only retryable class of errors (connection-based
        # failures).
        return FailureClassification(_FAILURE_CONNECTION, True)
    return FailureClassification(_FAILURE_OTHER, False)


def _classify(upstream: Upstream, err: Exception) -> FailureClassification:
    """Classify a failed upstream call.

    Use a user-provided upstream.classify_error(...) to classify if we can
    retry this error. If none is provided, fall back to
    _default_classification(...).
    """
    classification = _default_classification(err)
    if upstream.classify_error is None:
        return classification
    try:
        result = upstream.classify_error(err, classification)
    # classify_error is user code. Any error it raises is a configuration
    # failure.
    except Exception:  # noqa: BLE001
        return FailureClassification(_FAILURE_CONFIGURATION, False)
    if not isinstance(result, FailureClassification) or not result.type:  # pyright: ignore[reportUnnecessaryIsInstance]
        return FailureClassification(_FAILURE_CONFIGURATION, False)
    return result
