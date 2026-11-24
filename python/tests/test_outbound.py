"""Tests for temporalio.nexus_mcp.outbound."""

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any, cast
from unittest.mock import MagicMock

import nexusrpc
import pytest
from mcp.client import Client
from mcp.types import (
    ListToolsResult,
    Tool,
    ToolAnnotations,
)
from nexusrpc.handler._core import ServiceHandler  # pyright: ignore[reportPrivateUsage]

from temporalio.nexus_mcp.outbound import (
    FailureClassification,
    MCPProxyPlugin,
    Options,
    Upstream,
)


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
        self.error: BaseException | None = None
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


def start_context(operation: str) -> nexusrpc.handler.StartOperationContext:
    return nexusrpc.handler.StartOperationContext(
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
    result = await handler.start(start_context("list_tools"), None)
    # The result has only the tools. MCP SDKs reject an empty cacheScope.
    assert list(result.value) == ["tools"]
    return result.value["tools"]


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
