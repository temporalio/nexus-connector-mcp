// Package handler exposes Nexus operations as MCP tools.
//
// [Tool] marks a Nexus operation as an MCP tool. [Register] registers the
// marked operations on a Nexus service and adds a list_tools operation. The
// connector and the Workflow client call list_tools to discover the tools.
//
// The tool name is the registered Nexus operation name. The input and output
// schemas come from the operation input and output types.
package handler

import (
	"errors"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
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

// Tool marks op as an MCP tool.
func Tool[I, O any](op nexus.Operation[I, O], opts ToolOptions) MarkedTool {
	_, _ = op, opts
	return nil
}

// Register registers each marked operation on svc and adds the list_tools
// operation. Register operations that are not tools with
// [nexus.Service.Register].
func Register(svc *nexus.Service, tools ...MarkedTool) error {
	_, _ = svc, tools
	return errors.New("handler: Register is not implemented")
}
