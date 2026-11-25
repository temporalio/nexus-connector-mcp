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
    INVALID_PARAMS,
    CallToolResult,
    ImageContent,
    ListToolsResult,
    TextContent,
    Tool,
    ToolAnnotations,
)
from nexusrpc.handler._core import ServiceHandler  # pyright: ignore[reportPrivateUsage]

from temporalio.common import ActivityIDConflictPolicy, ActivityIDReusePolicy, RawValue
from temporalio.converter import DataConverter
from temporalio.exceptions import ActivityAlreadyStartedError, ApplicationError
from temporalio.nexus import TemporalOperationResult, TemporalStartOperationContext
from temporalio.nexus_mcp.outbound import (  # pyright: ignore[reportPrivateUsage]
    FailureClassification,
    MCPProxyPlugin,
    Options,
    Upstream,
    _nexus_handler,
)
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


async def list_tools(plugin: MCPProxyPlugin) -> list[dict[str, Any]]:
    """Run the list_tools operation of the plugin service and return the tools.

    It calls the operation handler directly, so the tests need no Worker and no
    Temporal server.
    """
    handler = operation_handler(plugin, "list_tools")
    result = await handler.start(context("list_tools"), None)
    # The result has only the tools. MCP SDKs reject an empty cacheScope.
    assert list(result.value) == ["tools"]
    return result.value["tools"]


class Input:
    """A LazyValue double. The dynamic handler consumes it as a RawValue."""

    def __init__(self, value: object) -> None:
        self.value = value

    async def consume(self, as_type: type | None = None) -> object:
        assert as_type is RawValue
        return RawValue(DataConverter.default.payload_converter.to_payload(self.value))


def nexus_client() -> tuple[Any, Any]:
    """Return a TemporalNexusClient double and its Activity handle."""
    handle = SimpleNamespace(result=AsyncMock(return_value="earlier result"))
    client = SimpleNamespace(
        client=SimpleNamespace(
            get_activity_handle=MagicMock(return_value=handle),
            data_converter=DataConverter.default,
        ),
        start_activity=AsyncMock(
            return_value=TemporalOperationResult[Any].async_token("token")
        ),
    )
    return client, handle


@pytest.fixture(autouse=True)
def nexus_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "temporalio.nexus_mcp.outbound._nexus_handler.nexus_info",
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


def call_tool_input(tool: str = "a", arguments: dict[str, Any] | None = None) -> Any:
    return _nexus_handler._CallToolInput(tool=tool, arguments=arguments or {})  # pyright: ignore[reportPrivateUsage]


# Configuration


def test_plugin_validates_options() -> None:
    fake = FakeUpstream()
    with pytest.raises(ValueError, match="at least one Upstream"):
        MCPProxyPlugin(options=Options(upstreams=[]))
    upstream = Upstream(service="weather", client_factory=factory(fake))
    with pytest.raises(ValueError, match="Duplicate Upstream service 'weather'"):
        MCPProxyPlugin(options=Options(upstreams=[upstream, upstream]))
    for service in ["", " ", "__temporal_x"]:
        with pytest.raises(ValueError, match="not reserved"):
            Upstream(service=service, client_factory=factory(fake))
    with pytest.raises(TypeError, match="callable"):
        Upstream(service="weather", client_factory=cast(Any, None))

    plugin = MCPProxyPlugin(
        options=Options(
            upstreams=[
                upstream,
                Upstream(service="other", client_factory=factory(fake)),
            ]
        )
    )
    assert plugin.name() == "temporalio.nexus_mcp.outbound.MCPProxyPlugin"
    assert len(cast(list[object], plugin.nexus_service_handlers)) == 2
    activities = cast(list[Any], plugin.activities)
    assert [fn.__temporal_activity_definition.name for fn in activities] == [
        "mcp-outbound.weather.call_upstream_tool",
        "mcp-outbound.other.call_upstream_tool",
    ]


def test_options_that_are_not_implemented_are_rejected() -> None:
    def classify(
        err: BaseException, default: FailureClassification
    ) -> FailureClassification:
        return default

    not_implemented: list[dict[str, Any]] = [
        {"activity_options_overrides": {"a": {}}},
        {"classify_error": classify},
    ]
    for options in not_implemented:
        with pytest.raises(NotImplementedError, match="not implemented yet"):
            setup_proxy(**options)


def test_activity_id() -> None:
    activity_id = _nexus_handler._activity_id  # pyright: ignore[reportPrivateUsage]
    assert activity_id("ep", "svc", "tool", "req") == "mcp-outbound-ep-svc-tool-req"
    long = activity_id("ep", "svc", "tool", "x" * 2000)
    assert len(long.encode()) <= 1000
    assert long.startswith("mcp-outbound-ep-svc-tool-x")
    assert long != activity_id("ep", "svc", "tool", "x" * 2001)
    multibyte = activity_id("ep", "svc", "tool", "é" * 600)
    assert len(multibyte.encode()) <= 1000
    multibyte.encode().decode()


# Tool list


async def test_list_tools_reads_all_pages_and_keeps_metadata() -> None:
    fake, _, plugin = setup_proxy()
    tools = await list_tools(plugin)
    assert [t["name"] for t in tools] == ["a", "b"]
    assert tools[0]["annotations"] == {"destructiveHint": True}
    assert tools[0]["_meta"] == {"source": "upstream"}
    assert tools[0]["outputSchema"]["type"] == "object"
    assert fake.entered == 1


async def test_list_tools_calls_the_upstream_each_time() -> None:
    fake, _, plugin = setup_proxy()
    await list_tools(plugin)
    fake.tools = [Tool(name="c", input_schema={"type": "object"})]
    assert [t["name"] for t in await list_tools(plugin)] == ["c"]
    assert fake.entered == 2


@pytest.mark.parametrize(
    "tools",
    [
        [Tool(name="list_tools", input_schema={})],
        [Tool(name="__temporal_x", input_schema={})],
        [Tool(name="dup", input_schema={}), Tool(name="dup", input_schema={})],
    ],
)
async def test_list_tools_rejects_bad_lists(tools: list[Tool]) -> None:
    fake, _, plugin = setup_proxy()
    fake.tools = tools
    with pytest.raises(nexusrpc.HandlerError) as err:
        await list_tools(plugin)
    assert err.value.type == nexusrpc.HandlerErrorType.UNAVAILABLE


async def test_list_tools_failure_is_unavailable_and_hides_the_error() -> None:
    fake, _, plugin = setup_proxy()
    fake.error = OSError("private-token")
    with pytest.raises(nexusrpc.HandlerError) as err:
        await list_tools(plugin)
    assert err.value.type == nexusrpc.HandlerErrorType.UNAVAILABLE
    assert "private" not in str(err.value)
    assert err.value.__cause__ is None


# Tool calls


async def test_new_start_uses_default_options_and_stable_identity() -> None:
    _, _, plugin = setup_proxy()
    client, _ = nexus_client()
    await operation_handler(plugin, "a").start_operation(
        context(), client, Input({"x": 1})
    )
    args, kwargs = client.start_activity.call_args
    assert args[0] is call_fn(plugin)
    assert args[1] == call_tool_input("a", {"x": 1})
    assert kwargs == {
        "id": "mcp-outbound-weather-endpoint-weather-a-r1",
        "id_conflict_policy": ActivityIDConflictPolicy.USE_EXISTING,
        "id_reuse_policy": ActivityIDReusePolicy.REJECT_DUPLICATE,
        "start_to_close_timeout": timedelta(minutes=1),
    }


async def test_tool_call_does_not_read_the_tool_list() -> None:
    fake, _, plugin = setup_proxy()
    client, _ = nexus_client()
    # The upstream does not list this tool. The upstream call fails for it.
    await operation_handler(plugin, "missing").start_operation(
        context("missing"), client, Input({})
    )
    assert fake.entered == 0
    assert client.start_activity.call_args.args[1] == call_tool_input("missing", {})


async def test_null_arguments_become_empty_object() -> None:
    _, _, plugin = setup_proxy()
    client, _ = nexus_client()
    await operation_handler(plugin, "b").start_operation(
        context("b"), client, Input(None)
    )
    assert client.start_activity.call_args.args[1] == call_tool_input("b", {})


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


async def test_retried_start_of_closed_activity_returns_its_result() -> None:
    _, _, plugin = setup_proxy()
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
    result = await activity_env().run(call_fn(plugin), call_tool_input("a", {"x": 1}))
    payload = result["io.temporal/mcpCallToolResult"]
    assert payload["isError"] is True
    assert payload["structuredContent"] == {"x": 1}
    assert [c["type"] for c in payload["content"]] == ["text", "image"]
    assert fake.calls == [("a", {"x": 1})]


async def test_activity_sends_empty_content_array() -> None:
    fake, _, plugin = setup_proxy()
    fake.result = CallToolResult(content=[])
    result = await activity_env().run(call_fn(plugin), call_tool_input())
    assert result["io.temporal/mcpCallToolResult"]["content"] == []


@pytest.mark.parametrize(
    "failure",
    [
        PermissionError("private-token"),
        MCPError(INVALID_PARAMS, "private"),
        httpx2.ConnectError("private"),
        ExceptionGroup("group", [OSError("private")]),
        RuntimeError("private"),
    ],
)
async def test_activity_failures_hold_only_the_type(failure: Exception) -> None:
    fake, _, plugin = setup_proxy()
    fake.error = failure
    with pytest.raises(ApplicationError) as err:
        await activity_env().run(call_fn(plugin), call_tool_input())
    assert err.value.type == "UpstreamError"
    assert err.value.non_retryable
    assert "private" not in str(err.value)


async def test_activity_rejects_incomplete_results() -> None:
    fake, _, plugin = setup_proxy()
    fake.result = CallToolResult.model_validate(
        {"content": [], "resultType": "input_required"}
    )
    with pytest.raises(ApplicationError) as err:
        await activity_env().run(call_fn(plugin), call_tool_input())
    assert err.value.type == "UpstreamError"
    assert err.value.non_retryable


async def test_activity_cancellation_is_raised() -> None:
    fake, _, plugin = setup_proxy()
    fake.error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await activity_env().run(call_fn(plugin), call_tool_input())


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
        await env.run(call_fn(plugin), call_tool_input())
    assert err.value.type == "UpstreamError"


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
    await env.run(call_fn(plugin), call_tool_input())
    assert len(beats) >= 2

    beats.clear()
    env = activity_env()
    env.on_heartbeat = lambda *details: beats.append(details)
    await env.run(call_fn(plugin), call_tool_input())
    assert beats == []
