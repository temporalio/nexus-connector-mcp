"""Tests for the authoring APIs of temporalio.nexus_mcp."""

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import nexusrpc
import nexusrpc.handler
import pytest
from mcp.types import ToolAnnotations

from temporalio import activity, nexus, nexus_mcp, workflow
from temporalio.client import WorkflowUpdateStage


@dataclass
class LookupInput:
    topic: str


@dataclass
class LookupOutput:
    number: int


@workflow.defn
class LookupWorkflow:
    @workflow.run
    async def run(self, input: LookupInput) -> LookupOutput:
        return LookupOutput(number=7)

    @workflow.update
    async def lookup(self, input: LookupInput) -> LookupOutput:
        return LookupOutput(number=7)


@activity.defn
async def lookup_activity(input: LookupInput) -> LookupOutput:
    return LookupOutput(number=7)


@nexusrpc.service(name="lucky")
@nexus_mcp.service
class LuckyService:
    """A service with sync, async, and unmarked operations."""

    lookup: nexusrpc.Operation[LookupInput, LookupOutput]
    ping: nexusrpc.Operation[None, str]
    hidden: nexusrpc.Operation[LookupInput, str]
    # Async operations. Each kind backs the operation with a different
    # Temporal primitive.
    run_workflow: nexusrpc.Operation[LookupInput, LookupOutput]
    start_workflow: nexusrpc.Operation[LookupInput, LookupOutput]
    start_activity: nexusrpc.Operation[LookupInput, LookupOutput]
    start_update: nexusrpc.Operation[LookupInput, LookupOutput]
    async_token: nexusrpc.Operation[LookupInput, str]
    custom_async: nexusrpc.Operation[LookupInput, LookupOutput]


class _AsyncLookup(nexusrpc.handler.OperationHandler[LookupInput, LookupOutput]):
    """A plain Nexus operation handler that starts async."""

    async def start(
        self, ctx: nexusrpc.handler.StartOperationContext, input: LookupInput
    ) -> nexusrpc.handler.StartOperationResultAsync:
        return nexusrpc.handler.StartOperationResultAsync("lookup-token")

    async def cancel(
        self, ctx: nexusrpc.handler.CancelOperationContext, token: str
    ) -> None:
        pass


@nexusrpc.handler.service_handler(service=LuckyService)
@nexus_mcp.service_handler
class LuckyHandler:
    """Handler for LuckyService."""

    @nexus_mcp.tool(
        title="Look up a number",
        description="Look up a number now.",
        annotations=ToolAnnotations(read_only_hint=True),
        meta={"example.com/owner": "team"},
    )
    @nexusrpc.handler.sync_operation
    async def lookup(
        self, ctx: nexusrpc.handler.StartOperationContext, input: LookupInput
    ) -> LookupOutput:
        """This docstring is not used, because description is set."""
        return LookupOutput(number=7)

    @nexus_mcp.tool()
    @nexusrpc.handler.sync_operation
    async def ping(
        self, ctx: nexusrpc.handler.StartOperationContext, input: None
    ) -> str:
        return "pong"

    @nexusrpc.handler.sync_operation
    async def hidden(
        self, ctx: nexusrpc.handler.StartOperationContext, input: LookupInput
    ) -> str:
        return input.topic

    @nexus_mcp.tool()
    @nexus.workflow_run_operation
    async def run_workflow(
        self, ctx: nexus.WorkflowRunOperationContext, input: LookupInput
    ) -> nexus.WorkflowHandle[LookupOutput]:
        """Run a Workflow."""
        return await ctx.start_workflow(LookupWorkflow.run, input, id=ctx.request_id)

    @nexus_mcp.tool()
    @nexus.temporal_operation
    async def start_workflow(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: LookupInput,
    ) -> nexus.TemporalOperationResult[LookupOutput]:
        """Start a Workflow."""
        return await client.start_workflow(LookupWorkflow.run, input, id=ctx.request_id)

    @nexus_mcp.tool()
    @nexus.temporal_operation
    async def start_activity(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: LookupInput,
    ) -> nexus.TemporalOperationResult[LookupOutput]:
        """Start an Activity."""
        return await client.start_activity(
            lookup_activity,
            input,
            id=ctx.request_id,
            task_queue="lookup",
            start_to_close_timeout=timedelta(minutes=1),
        )

    @nexus_mcp.tool()
    @nexus.temporal_operation
    async def start_update(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: LookupInput,
    ) -> nexus.TemporalOperationResult[LookupOutput]:
        """Start a Workflow Update."""
        return await client.start_workflow_update(
            "lookup-workflow",
            LookupWorkflow.lookup,
            input,
            wait_for_stage=WorkflowUpdateStage.ACCEPTED,
        )

    @nexus_mcp.tool()
    @nexus.temporal_operation
    async def async_token(
        self,
        ctx: nexus.TemporalStartOperationContext,
        client: nexus.TemporalNexusClient,
        input: LookupInput,
    ) -> nexus.TemporalOperationResult[str]:
        """Return an operation token."""
        return nexus.TemporalOperationResult.async_token("lookup-token")

    @nexus_mcp.tool()
    @nexusrpc.handler.operation_handler
    def custom_async(
        self,
    ) -> nexusrpc.handler.OperationHandler[LookupInput, LookupOutput]:
        """Start a custom async operation."""
        return _AsyncLookup()


# The handler class that each async operation must keep. The name check
# covers WorkflowRunOperationHandler, which temporalio.nexus does not export.
_ASYNC_HANDLER_CLASSES = {
    "run_workflow": "WorkflowRunOperationHandler",
    "start_workflow": "TemporalOperationHandler",
    "start_activity": "TemporalOperationHandler",
    "start_update": "TemporalOperationHandler",
    "async_token": "TemporalOperationHandler",
    "custom_async": "_AsyncLookup",
}


async def _list_tools() -> dict[str, Any]:
    """Run the list_tools operation of LuckyHandler and return its result.

    It calls the operation method directly, so the tests need no Worker and no
    Temporal server. A standalone Nexus operation needs both, so it belongs in
    an integration test.
    """
    # service_handler adds list_tools at run time, so the type checker does not
    # know the attribute. getattr avoids the type error. list_tools does not
    # read the context.
    list_tools = getattr(LuckyHandler(), "list_tools")  # noqa: B009
    manifest: dict[str, Any] = await list_tools(None, None)
    return manifest


def _tools_by_name(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {tool["name"]: tool for tool in manifest["tools"]}


def _operation_handler(name: str) -> nexusrpc.handler.OperationHandler[Any, Any]:
    handler = nexusrpc.handler.Handler([LuckyHandler()])
    return handler.service_handlers["lucky"].operation_handlers[name]


def test_service_adds_list_tools_operation() -> None:
    definition = nexusrpc.get_service_definition(LuckyService)
    assert definition is not None
    list_tools = definition.operation_definitions["list_tools"]
    assert list_tools.input_type is type(None)


async def test_list_tools_lists_only_marked_operations() -> None:
    tools = _tools_by_name(await _list_tools())
    assert sorted(tools) == sorted(
        [
            "lookup",
            "ping",
            "run_workflow",
            "start_workflow",
            "start_activity",
            "start_update",
            "async_token",
            "custom_async",
        ]
    )


async def test_tool_definition_fields() -> None:
    tools = _tools_by_name(await _list_tools())

    lookup = tools["lookup"]
    assert lookup["title"] == "Look up a number"
    assert lookup["description"] == "Look up a number now."
    assert lookup["annotations"] == {"readOnlyHint": True}
    assert lookup["_meta"] == {"example.com/owner": "team"}
    assert lookup["inputSchema"]["type"] == "object"
    assert lookup["inputSchema"]["required"] == ["topic"]
    assert lookup["outputSchema"]["type"] == "object"

    # An operation without input has an empty-object schema.
    ping = tools["ping"]
    assert ping["inputSchema"] == {"type": "object", "properties": {}}
    assert "description" not in ping


@pytest.mark.parametrize("name", sorted(_ASYNC_HANDLER_CLASSES))
async def test_async_tools(name: str) -> None:
    """Check each kind of async operation.

    The tool definition comes from the operation types and the docstring. The
    service keeps the Temporal SDK handler, so the async start and cancel stay
    with the SDK.
    """
    tool = _tools_by_name(await _list_tools())[name]
    assert tool["description"]
    assert tool["inputSchema"]["type"] == "object"
    assert tool["inputSchema"]["required"] == ["topic"]
    if name == "async_token":
        # A string output becomes text content, so it has no output schema.
        assert "outputSchema" not in tool
    else:
        assert tool["outputSchema"]["required"] == ["number"]
    handler_class = type(_operation_handler(name))
    assert _ASYNC_HANDLER_CLASSES[name] in [c.__name__ for c in handler_class.__mro__]


async def test_custom_async_tool_starts_async() -> None:
    op = _operation_handler("custom_async")
    assert isinstance(op, _AsyncLookup)
    # _AsyncLookup does not read the context.
    result = await op.start(None, LookupInput(topic="cats"))  # type: ignore[arg-type]
    assert result == nexusrpc.handler.StartOperationResultAsync("lookup-token")


def test_service_must_be_below_nexusrpc_service() -> None:
    @nexusrpc.service(name="defined")
    class Defined:
        op: nexusrpc.Operation[LookupInput, str]

    with pytest.raises(TypeError, match="below @nexusrpc.service"):
        nexus_mcp.service(Defined)


def test_service_rejects_declared_list_tools() -> None:
    class Declared:
        list_tools = nexusrpc.Operation[None, str](name="list_tools")

    with pytest.raises(TypeError, match="must not declare list_tools"):
        nexus_mcp.service(Declared)


def test_tool_must_be_above_operation_decorator() -> None:
    async def plain(self: object, ctx: object, input: LookupInput) -> str:
        return ""

    with pytest.raises(TypeError, match="above a Nexus operation decorator"):
        nexus_mcp.tool()(plain)


def test_service_handler_rejects_non_object_input() -> None:
    class StringInput:
        @nexus_mcp.tool()
        @nexusrpc.handler.sync_operation
        async def echo(
            self, ctx: nexusrpc.handler.StartOperationContext, input: str
        ) -> str:
            return input

    with pytest.raises(ValueError, match="object input type"):
        nexus_mcp.service_handler(StringInput)


def test_service_handler_rejects_reserved_tool_names() -> None:
    for name in ["__temporal_op", " "]:

        class Reserved:
            @nexus_mcp.tool()
            @nexusrpc.handler.sync_operation(name=name)
            async def op(
                self, ctx: nexusrpc.handler.StartOperationContext, input: LookupInput
            ) -> str:
                return ""

        with pytest.raises(ValueError, match="not reserved"):
            nexus_mcp.service_handler(Reserved)


def test_service_handler_must_be_below_nexusrpc_service_handler() -> None:
    with pytest.raises(TypeError, match="below @nexusrpc.handler.service_handler"):
        nexus_mcp.service_handler(LuckyHandler)


def test_service_handler_accepts_expose_marked() -> None:
    class Empty:
        pass

    decorated = nexus_mcp.service_handler(expose="marked")(Empty)
    assert hasattr(decorated, "list_tools")
