"""Tests for temporalio.nexus_mcp.outbound."""

import asyncio
import dataclasses
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import httpx2
import nexusrpc
import pytest
from mcp.client import Client
from mcp.shared.exceptions import MCPError
from mcp.types import (
    CONNECTION_CLOSED,
    INVALID_PARAMS,
    CallToolResult,
    ImageContent,
    ListToolsResult,
    TextContent,
    Tool,
    ToolAnnotations,
)
from nexusrpc.handler._core import ServiceHandler  # pyright: ignore[reportPrivateUsage]

from temporalio.client import ActivityExecutionStatus
from temporalio.common import (
    ActivityIDConflictPolicy,
    ActivityIDReusePolicy,
    Priority,
    RetryPolicy,
)
from temporalio.exceptions import ActivityAlreadyStartedError, ApplicationError
from temporalio.nexus import TemporalOperationResult, TemporalStartOperationContext
from temporalio.nexus_mcp import outbound
from temporalio.nexus_mcp.outbound import (
    FailureClassification,
    MCPProxyPlugin,
    Options,
    Upstream,
)
from temporalio.service import RPCError, RPCStatusCode
from temporalio.testing import ActivityEnvironment


class FakeUpstream:
    """An MCP client double. tools/list returns two pages."""

    def __init__(self) -> None:
        self.tools = [
            Tool(
                name="a",
                input_schema={"type": "object"},
                output_schema={
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                },
                annotations=ToolAnnotations(destructive_hint=True),
                _meta={"source": "upstream"},
            ),
            Tool(name="b", input_schema={"type": "object"}),
        ]
        self.result = CallToolResult(content=[TextContent(type="text", text="ok")])
        self.error: BaseException | None = None
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self.entered = 0

    async def list_tools(
        self, *, cursor: str | None = None, cache_mode: str = "use"
    ) -> ListToolsResult:
        assert cache_mode == "bypass"
        if self.error:
            raise self.error
        if cursor is None:
            return ListToolsResult(tools=self.tools[:1], next_cursor="page-2")
        return ListToolsResult(tools=self.tools[1:])

    async def call_tool(
        self, name: str, arguments: dict[str, Any] | None = None
    ) -> CallToolResult:
        self.calls.append((name, arguments))
        if self.error:
            raise self.error
        return self.result


class _Session(AbstractAsyncContextManager[Client]):
    """Enters the fake upstream as an MCP client."""

    def __init__(self, fake: FakeUpstream) -> None:
        self.fake = fake

    async def __aenter__(self) -> Client:
        self.fake.entered += 1
        return cast(Client, cast(object, self.fake))

    async def __aexit__(self, *exc_info: object) -> None:
        return None


def factory(fake: FakeUpstream) -> Callable[[], AbstractAsyncContextManager[Client]]:
    return lambda: _Session(fake)


def setup_proxy(
    **options: Any,
) -> tuple[FakeUpstream, Upstream, MCPProxyPlugin]:
    fake = FakeUpstream()
    upstream = Upstream(service="weather", client_factory=factory(fake), **options)
    return fake, upstream, MCPProxyPlugin(options=Options(upstreams=[upstream]))


def catalog(plugin: MCPProxyPlugin) -> Any:
    return plugin._catalogs["weather"]  # pyright: ignore[reportPrivateUsage]


def operation_handler(plugin: MCPProxyPlugin, name: str) -> Any:
    handlers = cast(list[object], plugin.nexus_service_handlers)
    return ServiceHandler.from_user_instance(handlers[0]).get_operation_handler(name)


def context(operation: str = "a") -> TemporalStartOperationContext:
    return TemporalStartOperationContext(
        service="weather",
        operation=operation,
        request_id="r1",
        headers={},
        task_cancellation=MagicMock(),
    )


class Input:
    """A LazyValue double."""

    def __init__(self, value: object) -> None:
        self.value = value

    async def consume(self, as_type: type | None = None) -> object:
        return self.value


def nexus_client(existing: object = None) -> tuple[Any, Any]:
    """Return a TemporalNexusClient double and its Activity handle.

    describe returns existing, or NOT_FOUND when existing is None.
    """
    handle = SimpleNamespace(
        describe=AsyncMock(return_value=existing),
        result=AsyncMock(return_value="earlier result"),
    )
    if existing is None:
        handle.describe.side_effect = RPCError("missing", RPCStatusCode.NOT_FOUND, b"")
    client = SimpleNamespace(
        client=SimpleNamespace(get_activity_handle=MagicMock(return_value=handle)),
        start_activity=AsyncMock(
            return_value=TemporalOperationResult[Any].async_token("token")
        ),
    )
    return client, handle


@pytest.fixture(autouse=True)
def nexus_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "temporalio.nexus_mcp.outbound.nexus_info",
        lambda: SimpleNamespace(endpoint="weather-endpoint"),
    )


def activity_env(**info: Any) -> ActivityEnvironment:
    env = ActivityEnvironment()
    now = datetime.now(UTC)
    env.info = dataclasses.replace(
        env.info, started_time=now, scheduled_time=now, **info
    )
    return env


def call_fn(plugin: MCPProxyPlugin) -> Callable[..., Awaitable[dict[str, Any]]]:
    activities = cast(list[Callable[..., Awaitable[dict[str, Any]]]], plugin.activities)
    return activities[0]


def call_input(tool: str = "a", arguments: dict[str, Any] | None = None) -> Any:
    return outbound._CallInput(tool=tool, arguments=arguments or {})  # pyright: ignore[reportPrivateUsage]


# Configuration


def test_plugin_validates_options() -> None:
    fake = FakeUpstream()
    with pytest.raises(ValueError, match="at least one Upstream"):
        MCPProxyPlugin(options=Options(upstreams=[]))
    upstream = Upstream(service="weather", client_factory=factory(fake))
    with pytest.raises(ValueError, match="distinct service name"):
        MCPProxyPlugin(options=Options(upstreams=[upstream, upstream]))
    for service in ["", " ", "__temporal_x"]:
        with pytest.raises(ValueError, match="not reserved"):
            Upstream(service=service, client_factory=factory(fake))
    with pytest.raises(TypeError, match="callable"):
        Upstream(service="weather", client_factory=cast(Any, None))
    with pytest.raises(ValueError, match="not reserved"):
        Upstream(
            service="weather",
            client_factory=factory(fake),
            activity_options_overrides={"list_tools": {}},
        )
    with pytest.raises(ValueError, match="must not be negative"):
        Upstream(
            service="weather",
            client_factory=factory(fake),
            activity_options_overrides={
                "a": {"heartbeat_timeout": timedelta(seconds=-1)}
            },
        )

    plugin = MCPProxyPlugin(
        options=Options(
            upstreams=[
                upstream,
                Upstream(service="other", client_factory=factory(fake)),
            ]
        )
    )
    assert plugin.name() == "temporalio.nexus_mcp.outbound.MCPProxyPlugin"
    activities = cast(list[Any], plugin.activities)
    assert [fn.__temporal_activity_definition.name for fn in activities] == [
        "mcp-outbound.weather.call_upstream_tool",
        "mcp-outbound.other.call_upstream_tool",
    ]


@pytest.mark.parametrize(
    "key",
    [
        "id",
        "task_queue",
        "id_conflict_policy",
        "id_reuse_policy",
        "rpc_metadata",
        "rpc_timeout",
        "result_type",
    ],
)
def test_connector_owned_options_are_rejected(key: str) -> None:
    with pytest.raises(TypeError, match="the proxy sets"):
        Upstream(
            service="weather",
            client_factory=factory(FakeUpstream()),
            activity_options_overrides={"a": {key: None}},
        )


def test_overrides_are_copied() -> None:
    overrides: dict[str, dict[str, Any]] = {"a": {"summary": "first"}}
    _, upstream, _ = setup_proxy(activity_options_overrides=overrides)
    overrides["a"]["summary"] = "changed"
    assert upstream.activity_options_overrides["a"]["summary"] == "first"


def test_activity_options_defaults_and_overrides() -> None:
    _, upstream, _ = setup_proxy(
        activity_options_overrides={
            "a": {
                "start_to_close_timeout": timedelta(hours=1),
                "heartbeat_timeout": timedelta(seconds=30),
                "retry_policy": RetryPolicy(maximum_attempts=5),
                "priority": Priority(priority_key=2),
                "summary": "weather",
            }
        }
    )
    options_for = outbound._options_for  # pyright: ignore[reportPrivateUsage]
    assert options_for(upstream, "b", None) == {
        "start_to_close_timeout": timedelta(minutes=1)
    }
    options = options_for(upstream, "a", {"destructiveHint": True})
    assert options["start_to_close_timeout"] == timedelta(hours=1)
    assert options["heartbeat_timeout"] == timedelta(seconds=30)
    assert options["retry_policy"].maximum_attempts == 5
    assert options["priority"].priority_key == 2
    assert options["summary"] == "weather"


@pytest.mark.parametrize(
    ("annotations", "attempts"),
    [
        ({"destructiveHint": True}, 1),
        ({"destructiveHint": True, "idempotentHint": True}, None),
        ({"destructiveHint": True, "readOnlyHint": True}, None),
        ({}, None),
        (None, None),
    ],
)
def test_annotations_select_retry_policy(
    annotations: dict[str, Any] | None, attempts: int | None
) -> None:
    _, upstream, _ = setup_proxy()
    options = outbound._options_for(upstream, "a", annotations)  # pyright: ignore[reportPrivateUsage]
    policy = options.get("retry_policy")
    assert (policy.maximum_attempts if policy else None) == attempts


def test_activity_id() -> None:
    activity_id = outbound._activity_id  # pyright: ignore[reportPrivateUsage]
    assert activity_id("ep", "svc", "tool", "req") == "mcp-outbound-ep-svc-tool-req"
    long = activity_id("ep", "svc", "tool", "x" * 2000)
    assert len(long.encode()) <= 1000
    assert long.startswith("mcp-outbound-ep-svc-tool-x")
    assert long != activity_id("ep", "svc", "tool", "x" * 2001)
    multibyte = activity_id("ep", "svc", "tool", "é" * 600)
    assert len(multibyte.encode()) <= 1000
    multibyte.encode().decode()


# Discovery


async def test_catalog_reads_all_pages_and_keeps_metadata() -> None:
    fake, _, plugin = setup_proxy()
    tools = await catalog(plugin).list_tools()
    assert [t["name"] for t in tools] == ["a", "b"]
    assert tools[0]["annotations"] == {"destructiveHint": True}
    assert tools[0]["_meta"] == {"source": "upstream"}
    assert tools[0]["outputSchema"]["type"] == "object"
    assert fake.entered == 1


async def test_catalog_follows_changes_and_keeps_list_on_failure() -> None:
    fake, _, plugin = setup_proxy()
    c = catalog(plugin)
    await c.refresh()
    fake.tools = [Tool(name="c", input_schema={"type": "object"})]
    tools = await c.list_tools()
    assert [t["name"] for t in tools] == ["c"]

    fake.error = OSError("private-token")
    tools = await c.list_tools()
    assert [t["name"] for t in tools] == ["c"]
    # The backoff skips the next discovery.
    entered = fake.entered
    await c.list_tools()
    assert fake.entered == entered


@pytest.mark.parametrize(
    "tools",
    [
        [Tool(name="list_tools", input_schema={})],
        [Tool(name="__temporal_x", input_schema={})],
        [Tool(name="dup", input_schema={}), Tool(name="dup", input_schema={})],
    ],
)
async def test_catalog_rejects_bad_lists_and_keeps_previous(tools: list[Tool]) -> None:
    fake, _, plugin = setup_proxy()
    c = catalog(plugin)
    await c.refresh()
    previous = c.tools
    fake.tools = tools
    with pytest.raises(ValueError):
        await c.refresh()
    assert c.tools is previous


async def test_catalog_without_discovery_is_unavailable() -> None:
    fake, _, plugin = setup_proxy()
    fake.error = OSError("offline")
    with pytest.raises(nexusrpc.HandlerError) as err:
        await catalog(plugin).list_tools()
    assert err.value.type == nexusrpc.HandlerErrorType.UNAVAILABLE


async def test_catalog_backoff_doubles_and_resets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [1000.0]
    monkeypatch.setattr("temporalio.nexus_mcp.outbound.time.monotonic", lambda: now[0])
    monkeypatch.setattr(
        "temporalio.nexus_mcp.outbound.random.uniform", lambda low, high: high
    )
    fake, _, plugin = setup_proxy()
    c = catalog(plugin)
    fake.error = OSError("offline")
    for delay in [1, 2, 4, 8, 16, 32, 60, 60]:
        with pytest.raises(OSError):
            await c.refresh()
        assert c._retry_delay == delay
        await c.refresh()  # During the backoff: no discovery, no error.
        now[0] += delay
    fake.error = None
    await c.refresh()
    assert c._retry_delay == 0
    assert c.discovered


async def test_list_tools_operation_uses_catalog() -> None:
    _, _, plugin = setup_proxy()
    handler = operation_handler(plugin, "list_tools")
    result = await handler.start(context("list_tools"), None)
    assert [t["name"] for t in result.value["tools"]] == ["a", "b"]


async def test_initial_discovery_stops_with_the_worker() -> None:
    fake, _, plugin = setup_proxy()
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def slow_list(
        *, cursor: str | None = None, cache_mode: str = "use"
    ) -> ListToolsResult:
        entered.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return ListToolsResult(tools=[])

    fake.list_tools = slow_list  # type: ignore[method-assign]

    async def run(_: Any) -> None:
        await entered.wait()

    await plugin.run_worker(cast(Any, None), run)
    assert cancelled.is_set()


# Tool calls


async def test_new_start_uses_options_and_stable_identity() -> None:
    _, _, plugin = setup_proxy(
        activity_options_overrides={
            "a": {
                "priority": Priority(priority_key=2),
                "start_delay": timedelta(seconds=1),
            }
        }
    )
    await catalog(plugin).refresh()
    client, _ = nexus_client()
    await operation_handler(plugin, "a").start_operation(
        context(), client, Input({"x": 1})
    )
    args, kwargs = client.start_activity.call_args
    assert args[0] is call_fn(plugin)
    assert args[1] == call_input("a", {"x": 1})
    assert kwargs["id"] == "mcp-outbound-weather-endpoint-weather-a-r1"
    assert kwargs["id_conflict_policy"] == ActivityIDConflictPolicy.USE_EXISTING
    assert kwargs["id_reuse_policy"] == ActivityIDReusePolicy.REJECT_DUPLICATE
    assert kwargs["start_to_close_timeout"] == timedelta(minutes=1)
    assert kwargs["retry_policy"].maximum_attempts == 1  # destructiveHint
    assert kwargs["priority"].priority_key == 2
    assert kwargs["start_delay"] == timedelta(seconds=1)


async def test_null_arguments_become_empty_object() -> None:
    _, _, plugin = setup_proxy()
    await catalog(plugin).refresh()
    client, _ = nexus_client()
    await operation_handler(plugin, "b").start_operation(
        context("b"), client, Input(None)
    )
    assert client.start_activity.call_args.args[1] == call_input("b", {})


@pytest.mark.parametrize("value", [[], [1], "text", 7, False])
async def test_non_object_arguments_are_bad_requests(value: object) -> None:
    _, _, plugin = setup_proxy()
    client, _ = nexus_client()
    with pytest.raises(nexusrpc.HandlerError) as err:
        await operation_handler(plugin, "a").start_operation(
            context(), client, Input(value)
        )
    assert err.value.type == nexusrpc.HandlerErrorType.BAD_REQUEST
    client.start_activity.assert_not_called()


@pytest.mark.parametrize("name", ["", "__temporal_internal"])
async def test_invalid_tool_names_are_not_found(name: str) -> None:
    _, _, plugin = setup_proxy()
    client, _ = nexus_client()
    with pytest.raises(nexusrpc.HandlerError) as err:
        await operation_handler(plugin, name).start_operation(
            context(name), client, Input({})
        )
    assert err.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    client.start_activity.assert_not_called()


async def test_unknown_tool_and_missing_discovery() -> None:
    fake, _, plugin = setup_proxy()
    client, _ = nexus_client()
    fake.error = OSError("offline")
    with pytest.raises(nexusrpc.HandlerError) as err:
        await operation_handler(plugin, "a").start_operation(
            context(), client, Input({})
        )
    assert err.value.type == nexusrpc.HandlerErrorType.UNAVAILABLE

    fake.error = None
    await catalog(plugin).refresh()
    with pytest.raises(nexusrpc.HandlerError) as err:
        await operation_handler(plugin, "missing").start_operation(
            context("missing"), client, Input({})
        )
    assert err.value.type == nexusrpc.HandlerErrorType.NOT_FOUND
    client.start_activity.assert_not_called()


async def test_running_start_reattaches_before_reading_discovery() -> None:
    _, _, plugin = setup_proxy()
    existing = SimpleNamespace(
        status=ActivityExecutionStatus.RUNNING,
        start_to_close_timeout=timedelta(minutes=5),
        schedule_to_close_timeout=None,
    )
    client, _ = nexus_client(existing)
    # The tool is not in a tool list, because discovery never ran.
    await operation_handler(plugin, "removed").start_operation(
        context("removed"), client, Input({})
    )
    assert not catalog(plugin).discovered
    kwargs = client.start_activity.call_args.kwargs
    assert kwargs["start_to_close_timeout"] == timedelta(minutes=5)


async def test_closed_activity_returns_its_result() -> None:
    _, _, plugin = setup_proxy()
    client, handle = nexus_client(
        SimpleNamespace(status=ActivityExecutionStatus.COMPLETED)
    )
    result = await operation_handler(plugin, "removed").start_operation(
        context("removed"), client, Input({})
    )
    assert result.value == "earlier result"
    client.start_activity.assert_not_called()

    # The Activity closed between describe and the start.
    await catalog(plugin).refresh()
    client, handle = nexus_client()
    client.start_activity.side_effect = ActivityAlreadyStartedError("closed", "call")
    result = await operation_handler(plugin, "a").start_operation(
        context(), client, Input({})
    )
    assert result.value == "earlier result"
    handle.result.assert_awaited_once()


# Call Activity


async def test_activity_returns_full_result_in_envelope() -> None:
    fake, _, plugin = setup_proxy()
    fake.result = CallToolResult(
        content=[
            TextContent(type="text", text="detail"),
            ImageContent(type="image", data="", mime_type="image/png"),
        ],
        structured_content={"x": 1},
        is_error=True,
    )
    result = await activity_env().run(call_fn(plugin), call_input("a", {"x": 1}))
    payload = result["io.temporal/mcpCallToolResult"]
    assert payload["isError"] is True
    assert payload["structuredContent"] == {"x": 1}
    assert [c["type"] for c in payload["content"]] == ["text", "image"]
    assert fake.calls == [("a", {"x": 1})]


async def test_activity_sends_empty_content_array() -> None:
    fake, _, plugin = setup_proxy()
    fake.result = CallToolResult(content=[])
    result = await activity_env().run(call_fn(plugin), call_input())
    assert result["io.temporal/mcpCallToolResult"]["content"] == []


async def test_activity_rejects_incomplete_results() -> None:
    fake, _, plugin = setup_proxy()
    fake.result = CallToolResult.model_validate(
        {"content": [], "resultType": "input_required"}
    )
    with pytest.raises(ApplicationError) as err:
        await activity_env().run(call_fn(plugin), call_input())
    assert err.value.type == "UpstreamProtocolError"
    assert err.value.non_retryable


@pytest.mark.parametrize(
    ("failure", "failure_type", "retryable"),
    [
        (PermissionError("private-token"), "UpstreamAuthenticationError", False),
        (FileNotFoundError("private-path"), "ProxyConfigurationError", False),
        (MCPError(CONNECTION_CLOSED, "private"), "UpstreamConnectionError", True),
        (MCPError(INVALID_PARAMS, "private"), "UpstreamProtocolError", False),
        (ConnectionResetError("private"), "UpstreamConnectionError", True),
        (httpx2.ConnectError("private"), "UpstreamConnectionError", True),
        (
            ExceptionGroup("group", [OSError("private")]),
            "UpstreamConnectionError",
            True,
        ),
        (RuntimeError("private"), "UpstreamError", False),
    ],
)
async def test_activity_failures_hold_only_the_type(
    failure: Exception, failure_type: str, retryable: bool
) -> None:
    fake, _, plugin = setup_proxy()
    fake.error = failure
    with pytest.raises(ApplicationError) as err:
        await activity_env().run(call_fn(plugin), call_input())
    assert err.value.type == failure_type
    assert err.value.non_retryable is not retryable
    assert "private" not in str(err.value)


@pytest.mark.parametrize(
    ("status", "failure_type", "retryable"),
    [
        (401, "UpstreamAuthenticationError", False),
        (429, "UpstreamRateLimitError", True),
        (503, "UpstreamHTTPError", True),
        (400, "UpstreamHTTPError", False),
    ],
)
async def test_http_status_classification_and_retry_after(
    status: int, failure_type: str, retryable: bool
) -> None:
    request = httpx2.Request("POST", "https://upstream.example.com/mcp?token=private")
    response = httpx2.Response(status, headers={"Retry-After": "120"}, request=request)
    fake, _, plugin = setup_proxy()
    fake.error = httpx2.HTTPStatusError("private", request=request, response=response)
    with pytest.raises(ApplicationError) as err:
        await activity_env().run(call_fn(plugin), call_input())
    assert err.value.type == failure_type
    assert err.value.non_retryable is not retryable
    assert err.value.next_retry_delay == timedelta(seconds=120)


async def test_classify_error_hook() -> None:
    def classify(
        err: BaseException, default: FailureClassification
    ) -> FailureClassification:
        assert default.type == "UpstreamError"
        return FailureClassification("CustomFailure", True)

    fake, _, plugin = setup_proxy(classify_error=classify)
    fake.error = RuntimeError("private")
    with pytest.raises(ApplicationError) as err:
        await activity_env().run(call_fn(plugin), call_input())
    assert err.value.type == "CustomFailure"
    assert not err.value.non_retryable

    def broken(
        err: BaseException, default: FailureClassification
    ) -> FailureClassification:
        raise RuntimeError("private-hook-value")

    fake, _, plugin = setup_proxy(classify_error=broken)
    fake.error = RuntimeError("private")
    with pytest.raises(ApplicationError) as err:
        await activity_env().run(call_fn(plugin), call_input())
    assert err.value.type == "ProxyConfigurationError"
    assert err.value.non_retryable
    assert "private" not in str(err.value)


async def test_cancellation_skips_classification() -> None:
    called: list[bool] = []

    def classify(
        err: BaseException, default: FailureClassification
    ) -> FailureClassification:
        called.append(True)
        return default

    fake, _, plugin = setup_proxy(classify_error=classify)
    fake.error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await activity_env().run(call_fn(plugin), call_input())
    assert not called


async def test_activity_deadline_stops_the_upstream_call() -> None:
    fake, _, plugin = setup_proxy()

    async def slow_call(
        name: str, arguments: dict[str, Any] | None = None
    ) -> CallToolResult:
        await asyncio.sleep(60)
        return fake.result

    fake.call_tool = slow_call  # type: ignore[method-assign]
    env = activity_env(start_to_close_timeout=timedelta(milliseconds=50))
    with pytest.raises(ApplicationError) as err:
        await env.run(call_fn(plugin), call_input())
    assert err.value.type == "UpstreamConnectionError"


async def test_heartbeats_when_heartbeat_timeout_is_set() -> None:
    fake, _, plugin = setup_proxy()

    async def slow_call(
        name: str, arguments: dict[str, Any] | None = None
    ) -> CallToolResult:
        await asyncio.sleep(0.1)
        return fake.result

    fake.call_tool = slow_call  # type: ignore[method-assign]
    beats: list[object] = []
    env = activity_env(heartbeat_timeout=timedelta(milliseconds=30))
    env.on_heartbeat = lambda *details: beats.append(details)
    await env.run(call_fn(plugin), call_input())
    assert len(beats) >= 2

    beats.clear()
    env = activity_env()
    env.on_heartbeat = lambda *details: beats.append(details)
    await env.run(call_fn(plugin), call_input())
    assert beats == []
