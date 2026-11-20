"""Tests for temporalio.nexus_mcp.workflow.NexusMCPClient."""

from typing import Any

import pytest
from mcp import types
from mcp.shared.exceptions import MCPError

import temporalio.workflow
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.nexus_mcp.workflow import NexusMCPClient

_TOOLS = {
    "tools": [
        {"name": "lookup", "inputSchema": {"type": "object"}},
        {"name": "slow", "inputSchema": {"type": "object"}},
    ]
}


class _FakeNexus:
    """Records operations and returns a result for each operation name."""

    def __init__(self, results: dict[str, Any]) -> None:
        self.results = results
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, operation: str, argument: Any) -> Any:
        self.calls.append((operation, argument))
        result = self.results[operation]
        if isinstance(result, BaseException):
            raise result
        return result


def _client(
    monkeypatch: pytest.MonkeyPatch, results: dict[str, Any]
) -> tuple[NexusMCPClient, _FakeNexus]:
    fake = _FakeNexus({"list_tools": _TOOLS, **results})
    client = NexusMCPClient(service="weather", endpoint="weather-endpoint")
    monkeypatch.setattr(client, "_execute", fake.execute)
    return client, fake


def test_requires_service_and_endpoint() -> None:
    for service, endpoint in [("", "endpoint"), ("service", " ")]:
        with pytest.raises(ValueError, match="required"):
            NexusMCPClient(service=service, endpoint=endpoint)


async def test_list_tools_returns_tool_definitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake = _client(monkeypatch, {})
    tools = await client.list_tools()
    assert [tool.name for tool in tools] == ["lookup", "slow"]
    assert fake.calls == [("list_tools", None)]


async def test_list_tools_rejects_duplicate_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake = _client(monkeypatch, {})
    fake.results["list_tools"] = {"tools": [{"name": "a", "inputSchema": {}}] * 2}
    with pytest.raises(ValueError, match="more than once"):
        await client.list_tools()


async def test_call_tool_calls_the_operation_of_the_same_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake = _client(monkeypatch, {"lookup": {"answer": 42}})
    result = await client.call_tool("lookup", {"q": "x"})
    assert result.structured_content == {"answer": 42}
    assert result.is_error is False
    assert isinstance(result.content[0], types.TextContent)
    assert '"answer": 42' in result.content[0].text

    await client.call_tool("lookup")
    assert fake.calls == [("lookup", {"q": "x"}), ("lookup", {})]


async def test_empty_and_list_tools_names_raise_invalid_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, fake = _client(monkeypatch, {})
    for name in ["", "list_tools"]:
        with pytest.raises(MCPError) as info:
            await client.call_tool(name)
        assert info.value.error.code == types.INVALID_PARAMS
    assert fake.calls == []


async def test_operation_failure_becomes_tool_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler_failure = ApplicationError("handler error")
    handler_failure.__cause__ = ApplicationError("tool failed")
    failure = RuntimeError("nexus operation failed")
    failure.__cause__ = handler_failure
    client, _ = _client(monkeypatch, {"lookup": failure})
    result = await client.call_tool("lookup")
    assert result.is_error is True
    assert isinstance(result.content[0], types.TextContent)
    assert result.content[0].text == "tool failed"


async def test_cancellation_is_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    client, _ = _client(monkeypatch, {"slow": CancelledError("cancelled")})
    with pytest.raises(CancelledError):
        await client.call_tool("slow")


@pytest.mark.parametrize(
    ("value", "text", "structured"),
    [
        ("hello", "hello", None),
        (None, "", None),
        ([1, "b"], '[1, "b"]', None),
        ({"a": 1}, '{\n  "a": 1\n}', {"a": 1}),
    ],
)
async def test_result_mapping(
    monkeypatch: pytest.MonkeyPatch,
    value: Any,
    text: str,
    structured: dict[str, Any] | None,
) -> None:
    client, _ = _client(monkeypatch, {"lookup": value})
    result = await client.call_tool("lookup")
    assert isinstance(result.content[0], types.TextContent)
    assert result.content[0].text == text
    assert result.structured_content == structured


async def test_envelope_keeps_content_meta_and_error_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = {
        "io.temporal/mcpCallToolResult": {
            "content": [{"type": "image", "data": "", "mimeType": "image/png"}],
            "isError": True,
            "_meta": {"trace": "id"},
        }
    }
    client, _ = _client(monkeypatch, {"lookup": envelope})
    result = await client.call_tool("lookup")
    assert isinstance(result.content[0], types.ImageContent)
    assert result.is_error is True
    assert result.meta == {"trace": "id"}


async def test_execute_uses_workflow_nexus_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[dict[str, str]] = []
    executed: list[tuple[str, Any]] = []

    class _NexusClient:
        async def execute_operation(self, operation: str, argument: Any) -> Any:
            executed.append((operation, argument))
            return _TOOLS

    def create_nexus_client(*, service: str, endpoint: str) -> _NexusClient:
        created.append({"service": service, "endpoint": endpoint})
        return _NexusClient()

    monkeypatch.setattr(temporalio.workflow, "create_nexus_client", create_nexus_client)
    client = NexusMCPClient(service="weather", endpoint="weather-endpoint")
    await client.list_tools()
    assert created == [{"service": "weather", "endpoint": "weather-endpoint"}]
    assert executed == [("list_tools", None)]
