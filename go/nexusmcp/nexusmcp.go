// Package nexusmcp exposes Nexus operations as MCP tools.
//
// [Tool] marks a Nexus operation as an MCP tool. [Register] registers the
// marked operations on a Nexus service and adds a list_tools operation. The
// connector and the Workflow client call list_tools to discover the tools.
//
// The tool name is the registered Nexus operation name. The input and output
// schemas come from the operation input and output types. Field names and
// descriptions come from the json and jsonschema struct tags.
package nexusmcp

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
)

const (
	listToolsOperation = "list_tools"
	// reservedPrefix starts operation names that Temporal reserves.
	reservedPrefix = "__temporal_"
)

// ToolOptions sets the MCP tool metadata for a marked operation.
type ToolOptions struct {
	// Title is a display label.
	Title string
	// Description is the tool description.
	Description string
	// Annotations are MCP tool hints.
	Annotations *mcp.ToolAnnotations
	// Meta is the MCP _meta field of the tool.
	Meta map[string]any
}

// MarkedTool is a Nexus operation marked as an MCP tool. [Tool] creates it.
type MarkedTool interface {
	isMarkedTool()
}

// markedTool holds the operation and its tool definition. Tool has no error
// result, so it keeps a schema error for Register to return.
type markedTool struct {
	op   nexus.RegisterableOperation
	tool *mcp.Tool
	err  error
}

func (*markedTool) isMarkedTool() {}

// Tool marks op as an MCP tool. It works for sync and async operations, such
// as the temporalnexus Workflow, Activity, and Update operations.
//
// The input type must have an object JSON schema: a struct, a pointer to a
// struct, or a map with string keys. An operation without input has an
// empty-object schema. Only an object output type gets an output schema,
// because other outputs become text content. [Register] returns the error if
// a schema is not valid.
func Tool[I, O any](op nexus.Operation[I, O], opts ToolOptions) MarkedTool {
	if op == nil {
		return &markedTool{err: errors.New("nexusmcp: Tool needs an operation")}
	}
	name := op.Name()
	input, err := inputSchema(reflect.TypeFor[I]())
	if err != nil {
		return &markedTool{op: op, err: fmt.Errorf("nexusmcp: tool %q input schema: %w", name, err)}
	}
	tool := &mcp.Tool{
		Name:        name,
		Title:       opts.Title,
		Description: opts.Description,
		Annotations: opts.Annotations,
		Meta:        opts.Meta,
		InputSchema: input,
	}
	// OutputSchema has type any. A nil *jsonschema.Schema in it would encode
	// as "outputSchema": null, so set it only when there is a schema.
	if output := outputSchema(reflect.TypeFor[O]()); output != nil {
		tool.OutputSchema = output
	}
	return &markedTool{op: op, tool: tool}
}

// Register registers each marked operation on svc and adds the list_tools
// operation. Register operations that are not tools with
// [nexus.Service.Register].
//
// It returns an error if a tool has an invalid schema, a tool name is empty,
// list_tools, or starts with __temporal_, or svc already has an operation of
// the same name.
func Register(svc *nexus.Service, tools ...MarkedTool) error {
	if svc == nil {
		return errors.New("nexusmcp: Register needs a service")
	}
	ops := make([]nexus.RegisterableOperation, 0, len(tools)+1)
	defs := make([]*mcp.Tool, 0, len(tools))
	for _, t := range tools {
		marked, ok := t.(*markedTool)
		if !ok || marked == nil {
			return errors.New("nexusmcp: Register takes the results of Tool")
		}
		if marked.err != nil {
			return marked.err
		}
		name := marked.tool.Name
		if strings.TrimSpace(name) == "" || name == listToolsOperation || strings.HasPrefix(name, reservedPrefix) {
			return fmt.Errorf("nexusmcp: tool name %q must be non-empty and not reserved", name)
		}
		ops = append(ops, marked.op)
		defs = append(defs, marked.tool)
	}
	// The tool list does not change after Register.
	manifest := &listToolsResult{Tools: defs}
	ops = append(ops, nexus.NewSyncOperation(listToolsOperation,
		func(context.Context, nexus.NoValue, nexus.StartOperationOptions) (*listToolsResult, error) {
			return manifest, nil
		}))
	if err := svc.Register(ops...); err != nil {
		return fmt.Errorf("nexusmcp: %w", err)
	}
	return nil
}

// listToolsResult is the output of list_tools: an MCP tools/list result with
// only the tools. mcp.ListToolsResult also encodes its cache fields, with an
// empty cacheScope that other MCP SDKs reject.
type listToolsResult struct {
	Tools []*mcp.Tool `json:"tools"`
}

var noValueType = reflect.TypeFor[nexus.NoValue]()

// inputSchema returns the JSON schema of an input type.
func inputSchema(t reflect.Type) (*jsonschema.Schema, error) {
	if t == noValueType {
		return &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{}}, nil
	}
	// MCP arguments are objects, also when the operation takes a pointer.
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	schema, err := jsonschema.ForType(t, nil)
	if err != nil {
		return nil, err
	}
	if schema.Type != "object" {
		return nil, fmt.Errorf("input type %s must be a struct or a map with string keys", t)
	}
	return schema, nil
}

// outputSchema returns the JSON schema of an object output type, or nil.
func outputSchema(t reflect.Type) *jsonschema.Schema {
	if t == noValueType || t.Kind() == reflect.Interface {
		return nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	schema, err := jsonschema.ForType(t, nil)
	if err != nil || schema.Type != "object" {
		return nil
	}
	return schema
}
