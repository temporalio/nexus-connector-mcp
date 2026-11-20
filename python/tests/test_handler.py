"""Tests for temporalio.nexus_mcp.service and temporalio.nexus_mcp.handler."""

from dataclasses import dataclass
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest
from mcp.types import ToolAnnotations

import temporalio.nexus_mcp.handler
from temporalio import nexus, nexus_mcp


@dataclass
class LuckyInput:
    topic: str


@dataclass
class LuckyOutput:
    number: int


@nexusrpc.service(name="lucky")
@nexus_mcp.service
class LuckyService:
    """A service with marked and unmarked operations."""

    get_number: nexusrpc.Operation[LuckyInput, str]
    get_delayed: nexusrpc.Operation[LuckyInput, LuckyOutput]
    no_input: nexusrpc.Operation[None, str]
    not_a_tool: nexusrpc.Operation[LuckyInput, str]


@nexusrpc.handler.service_handler(service=LuckyService)
@temporalio.nexus_mcp.handler.service_handler
class LuckyHandler:
    """Handler for LuckyService."""

    @temporalio.nexus_mcp.handler.tool(title="Get a lucky number")
    @nexus.temporal_operation
    async def get_number(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: LuckyInput,
    ) -> nexus.TemporalOperationResult[str]:
        """Return a lucky number for a topic."""
        return nexus.TemporalOperationResult.sync(f"{input.topic}: 7")

    @temporalio.nexus_mcp.handler.tool(
        description="Return a lucky number later.",
        annotations=ToolAnnotations(read_only_hint=True),
        meta={"example.com/owner": "team"},
    )
    @nexusrpc.handler.sync_operation
    async def get_delayed(
        self, ctx: nexusrpc.handler.StartOperationContext, input: LuckyInput
    ) -> LuckyOutput:
        """This docstring is not used, because description is set."""
        return LuckyOutput(number=7)

    @temporalio.nexus_mcp.handler.tool()
    @nexusrpc.handler.sync_operation
    async def no_input(
        self, ctx: nexusrpc.handler.StartOperationContext, input: None
    ) -> str:
        return "7"

    @nexusrpc.handler.sync_operation
    async def not_a_tool(
        self, ctx: nexusrpc.handler.StartOperationContext, input: LuckyInput
    ) -> str:
        return "hidden"


async def _list_tools() -> dict[str, Any]:
    # service_handler adds list_tools at run time. list_tools does not read the
    # context.
    list_tools = getattr(LuckyHandler(), "list_tools")
    manifest: dict[str, Any] = await list_tools(None, None)
    return manifest


def _tools_by_name(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {tool["name"]: tool for tool in manifest["tools"]}


def test_service_adds_list_tools_operation() -> None:
    definition = nexusrpc.get_service_definition(LuckyService)
    assert definition is not None
    list_tools = definition.operation_definitions["list_tools"]
    assert list_tools.input_type is type(None)


async def test_list_tools_lists_only_marked_operations() -> None:
    tools = _tools_by_name(await _list_tools())
    assert sorted(tools) == ["get_delayed", "get_number", "no_input"]


async def test_tool_definition_fields() -> None:
    tools = _tools_by_name(await _list_tools())

    get_number = tools["get_number"]
    assert get_number["title"] == "Get a lucky number"
    assert get_number["description"] == "Return a lucky number for a topic."
    assert get_number["inputSchema"]["type"] == "object"
    assert get_number["inputSchema"]["required"] == ["topic"]
    # A string output becomes text content, so it has no output schema.
    assert "outputSchema" not in get_number

    get_delayed = tools["get_delayed"]
    assert get_delayed["description"] == "Return a lucky number later."
    assert get_delayed["annotations"] == {"readOnlyHint": True}
    assert get_delayed["_meta"] == {"example.com/owner": "team"}
    assert get_delayed["outputSchema"]["type"] == "object"
    assert get_delayed["outputSchema"]["required"] == ["number"]

    no_input = tools["no_input"]
    assert no_input["inputSchema"] == {"type": "object", "properties": {}}
    assert "description" not in no_input


def test_service_must_be_below_nexusrpc_service() -> None:
    @nexusrpc.service(name="defined")
    class Defined:
        op: nexusrpc.Operation[LuckyInput, str]

    with pytest.raises(TypeError, match="below @nexusrpc.service"):
        nexus_mcp.service(Defined)


def test_service_rejects_declared_list_tools() -> None:
    class Declared:
        list_tools = nexusrpc.Operation[None, str](name="list_tools")

    with pytest.raises(TypeError, match="must not declare list_tools"):
        nexus_mcp.service(Declared)


def test_tool_must_be_above_operation_decorator() -> None:
    async def plain(self: object, ctx: object, input: LuckyInput) -> str:
        return ""

    with pytest.raises(TypeError, match="above a Nexus operation decorator"):
        temporalio.nexus_mcp.handler.tool()(plain)


def test_service_handler_rejects_non_object_input() -> None:
    class StringInput:
        @temporalio.nexus_mcp.handler.tool()
        @nexusrpc.handler.sync_operation
        async def echo(
            self, ctx: nexusrpc.handler.StartOperationContext, input: str
        ) -> str:
            return input

    with pytest.raises(ValueError, match="object input type"):
        temporalio.nexus_mcp.handler.service_handler(StringInput)


def test_service_handler_rejects_reserved_tool_names() -> None:
    for name in ["__temporal_op", " "]:

        class Reserved:
            @temporalio.nexus_mcp.handler.tool()
            @nexusrpc.handler.sync_operation(name=name)
            async def op(
                self, ctx: nexusrpc.handler.StartOperationContext, input: LuckyInput
            ) -> str:
                return ""

        with pytest.raises(ValueError, match="not reserved"):
            temporalio.nexus_mcp.handler.service_handler(Reserved)


def test_service_handler_must_be_below_nexusrpc_service_handler() -> None:
    with pytest.raises(TypeError, match="below @nexusrpc.handler.service_handler"):
        temporalio.nexus_mcp.handler.service_handler(LuckyHandler)


def test_service_handler_accepts_expose_marked() -> None:
    class Empty:
        pass

    decorated = temporalio.nexus_mcp.handler.service_handler(expose="marked")(Empty)
    assert hasattr(decorated, "list_tools")
