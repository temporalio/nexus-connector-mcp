"""Nexus handler APIs that expose operations as MCP tools.

:mod:`temporalio.nexus_mcp` exports :func:`service_handler` and :func:`tool`.
Import them from there. This module must stay safe to import in Workflow
code, because :mod:`temporalio.nexus_mcp` imports it.

The author keeps the ``nexusrpc`` decorators. The decorators of this module go
below them on the class, and above them on each operation:

- ``@nexus_mcp.service_handler`` on the service handler class implements
  ``list_tools``.
- ``@nexus_mcp.tool(...)`` on each operation marks it as an MCP tool.
  Operations without it are not tools.
"""

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypeVar, overload

import nexusrpc
import nexusrpc.handler
import pydantic
from mcp.types import Tool, ToolAnnotations

__all__ = ["service_handler", "tool"]

HandlerT = TypeVar("HandlerT", bound=type)
OperationT = TypeVar("OperationT", bound=Callable[..., Any])

_LIST_TOOLS_OPERATION = "list_tools"
# Operation names with this prefix are reserved for Temporal.
_RESERVED_PREFIX = "__temporal_"
# The attribute that holds the tool options of a marked operation.
_TOOL_ATTR = "__temporalio_nexus_mcp_tool__"
# The input schema of an operation that takes no input.
_EMPTY_OBJECT_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}


@dataclass(frozen=True)
class _ToolOptions:
    title: str | None
    description: str | None
    annotations: ToolAnnotations | None
    meta: Mapping[str, Any] | None


@overload
def service_handler(cls: HandlerT, /) -> HandlerT: ...


@overload
def service_handler(
    *, expose: Literal["marked"] = "marked"
) -> Callable[[HandlerT], HandlerT]: ...


def service_handler(
    cls: HandlerT | None = None, /, *, expose: Literal["marked"] = "marked"
) -> HandlerT | Callable[[HandlerT], HandlerT]:
    """Implement the ``list_tools`` operation on a Nexus service handler.

    Apply it below ``@nexusrpc.handler.service_handler``. Use it bare or with
    arguments. The service definition needs ``@nexus_mcp.service``.

    It checks each marked operation when the class is built. A tool name must
    be non-empty, must not be ``list_tools``, and must not start with
    ``__temporal_``. The input type must have an object JSON schema.

    Args:
        cls: The Nexus service handler class.
        expose: Which operations to list as tools. ``"marked"`` lists only
            operations marked with :func:`tool`.

    Returns:
        The service handler class, or a decorator that returns it.

    Raises:
        TypeError: If it is applied above
            ``@nexusrpc.handler.service_handler``.
        ValueError: If a tool name is not valid, or a tool input type has no
            object JSON schema.
    """
    # The Literal type allows only "marked". Untyped callers can pass other
    # values, so check at run time too.
    if expose != "marked":
        raise ValueError(f"expose must be 'marked', not {expose!r}")  # pyright: ignore[reportUnreachable]
    if cls is None:
        return _service_handler
    return _service_handler(cls)


def _service_handler(cls: HandlerT) -> HandlerT:
    if nexusrpc.get_service_definition(cls) is not None:
        raise TypeError(
            f"{cls.__name__}: apply @nexus_mcp.service_handler below "
            "@nexusrpc.handler.service_handler"
        )
    if _LIST_TOOLS_OPERATION in vars(cls):
        raise TypeError(
            f"{cls.__name__} must not implement {_LIST_TOOLS_OPERATION}; "
            "@nexus_mcp.service_handler adds it"
        )
    # Check the marked operations now, so a bad tool fails at Worker start,
    # not at the first tools/list.
    for method in vars(cls).values():
        op = nexusrpc.get_operation(method)
        if op is not None and _tool_options(method) is not None:
            _input_schema(op.name, op.input_type)

    @nexusrpc.handler.sync_operation(name=_LIST_TOOLS_OPERATION)
    async def list_tools(
        self: Any, ctx: nexusrpc.handler.StartOperationContext, input: None
    ) -> dict[str, Any]:
        """Return the MCP tool definitions of this service."""
        return _manifest(type(self))

    setattr(cls, _LIST_TOOLS_OPERATION, list_tools)
    return cls


def tool(
    *,
    title: str | None = None,
    description: str | None = None,
    annotations: ToolAnnotations | None = None,
    meta: Mapping[str, Any] | None = None,
) -> Callable[[OperationT], OperationT]:
    """Mark a Nexus operation as an MCP tool.

    The tool name is the registered Nexus operation name. Apply it above the
    Nexus operation decorator. It works for sync and async operations, such
    as ``workflow_run_operation`` and ``temporal_operation`` operations that
    start a Workflow, an Activity, or a Workflow Update.

    Args:
        title: Display label for the tool.
        description: Tool description. When unset or empty, the handler
            docstring is used if present.
        annotations: MCP tool annotations.
        meta: MCP ``_meta`` for the tool.

    Returns:
        A decorator that marks the operation.

    Raises:
        TypeError: If the decorated function is not a Nexus operation.
    """
    options = _ToolOptions(title, description, annotations, meta)

    def decorate(op: OperationT) -> OperationT:
        if nexusrpc.get_operation(op) is None:
            raise TypeError("apply @nexus_mcp.tool above a Nexus operation decorator")
        setattr(op, _TOOL_ATTR, options)
        return op

    return decorate


def _tool_options(method: Any) -> _ToolOptions | None:
    options = getattr(method, _TOOL_ATTR, None)
    return options if isinstance(options, _ToolOptions) else None


def _manifest(handler_class: type) -> dict[str, Any]:
    """Return the ``list_tools`` result of a service handler class."""
    definition = nexusrpc.get_service_definition(handler_class)
    if definition is None:
        raise TypeError(f"{handler_class.__name__} is not a Nexus service handler")
    tools: list[dict[str, Any]] = []
    for op in definition.operation_definitions.values():
        if op.name == _LIST_TOOLS_OPERATION:
            continue
        method = getattr(handler_class, op.method_name or op.name, None)
        options = _tool_options(method)
        if options is None:
            continue
        description = options.description
        if (not description or not description.strip()) and method is not None:
            doc = inspect.getdoc(method)
            description = doc or None
        fields: dict[str, Any] = {
            "name": op.name,
            "title": options.title,
            "description": description or None,
            "inputSchema": _input_schema(op.name, op.input_type),
            "outputSchema": _object_schema(op.output_type),
            "annotations": options.annotations,
            "_meta": dict(options.meta) if options.meta else None,
        }
        mcp_tool = Tool.model_validate(
            {key: value for key, value in fields.items() if value is not None}
        )
        tools.append(mcp_tool.model_dump(mode="json", by_alias=True, exclude_none=True))
    return {"tools": tools}


def _input_schema(name: str, input_type: Any) -> dict[str, Any]:
    """Return the input schema of a tool, or raise if it is not valid."""
    if (
        not name.strip()
        or name == _LIST_TOOLS_OPERATION
        or name.startswith(_RESERVED_PREFIX)
    ):
        raise ValueError(f"Tool name {name!r} must be non-empty and not reserved")
    if input_type in (None, type(None)):
        return dict(_EMPTY_OBJECT_SCHEMA)
    try:
        schema = pydantic.TypeAdapter(input_type).json_schema()
    except (pydantic.PydanticUserError, TypeError, ValueError) as err:
        raise ValueError(
            f"Tool {name!r}: cannot generate an input schema: {err}"
        ) from err
    if schema.get("type") != "object":
        raise ValueError(f"Tool {name!r} must have an object input type")
    return schema


def _object_schema(output_type: Any) -> dict[str, Any] | None:
    """Return the output schema of a tool.

    Only object outputs have one. Other outputs become text content.
    """
    if output_type in (None, type(None), Any):
        return None
    try:
        schema = pydantic.TypeAdapter(output_type).json_schema()
    except (pydantic.PydanticUserError, TypeError, ValueError):
        return None
    return schema if schema.get("type") == "object" else None
