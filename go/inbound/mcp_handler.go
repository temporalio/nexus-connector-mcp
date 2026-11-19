package inbound

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/nexus-connector-mcp/go/inbound/internal/mcpwire"
)

const (
	// defaultConnectorName is the MCP server name when Options.ConnectorName
	// is empty.
	defaultConnectorName = "nexus-connector-mcp"
	// maxConnectorNameBytes limits Options.ConnectorName.
	maxConnectorNameBytes = 64
	// modulePath finds the version of this module in the build info.
	modulePath = "github.com/temporalio/nexus-connector-mcp/go"
	// listToolsTimeout limits one list_tools call. list_tools is a sync
	// operation.
	listToolsTimeout = 30 * time.Second
)

// connector answers MCP tools/list and tools/call. It runs the Nexus
// operations through nexus.
type connector struct {
	// opts is the NewServer copy of the options, with defaults applied.
	opts  *Options
	nexus *nexusClient
}

func newServer(c *connector) *mcp.Server {
	s := mcp.NewServer(
		&mcp.Implementation{Name: c.opts.ConnectorName, Version: moduleVersion()},
		&mcp.ServerOptions{Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}},
	)
	// The tool list comes from Nexus at request time, so no tool is
	// registered in the SDK. This middleware answers tools/list and
	// tools/call. The SDK answers all other methods.
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch method {
			case "tools/list":
				ctx = context.WithValue(ctx, requestExtraKey{}, req.GetExtra())
				return c.listTools(ctx, operationIDPrefix(req))
			case "tools/call":
				callReq, ok := req.(*mcp.CallToolRequest)
				if !ok {
					return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: fmt.Sprintf("unexpected request type %T", req)}
				}
				ctx = context.WithValue(ctx, requestExtraKey{}, req.GetExtra())
				return c.callTool(ctx, operationIDPrefix(req), callReq)
			}
			return next(ctx, method, req)
		}
	})
	return s
}

// moduleVersion returns the version of this module in the binary, or
// "(devel)" when the build info does not have it.
func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	if info.Main.Path == modulePath && info.Main.Version != "" {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep.Path == modulePath {
			return dep.Version
		}
	}
	return "(devel)"
}

// owner returns the owner hash of the verified caller, or "".
func (c *connector) owner(ctx context.Context) string {
	if identity, ok := c.opts.CallerIdentity(ctx); ok && identity != "" {
		return ownerHash(identity)
	}
	return ""
}

// operationID returns a new operation ID for operation. It adds the service,
// endpoint, and operation to prefix.
func (c *connector) operationID(ctx context.Context, prefix, operation string) string {
	return newOperationID(strings.Join([]string{prefix, c.opts.Service, c.opts.Endpoint, operation}, "-"), c.owner(ctx))
}

// listTools runs list_tools on the service and returns its tools.
func (c *connector) listTools(ctx context.Context, prefix string) (*mcp.ListToolsResult, error) {
	ctx, cancel := context.WithTimeout(ctx, listToolsTimeout)
	defer cancel()
	operation := mcpwire.ListToolsOperation
	h, err := c.nexus.start(ctx, c.operationID(ctx, prefix, operation), operation, nil, listToolsTimeout)
	if err != nil {
		return nil, c.listToolsError(err)
	}
	out, err := wait(ctx, h)
	if errors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("did not complete in %s; check that the handler Worker runs", listToolsTimeout)
	}
	if err == nil {
		err = out.failure
	}
	if err != nil {
		return nil, c.listToolsError(err)
	}
	tools, err := c.checkTools(out.value)
	if err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	// TODO(long-nt-tran): cache the tool list. Refresh it on a timer and on
	// an unknown tool name, and keep a failed read for a short time.
	//
	// Each tools/list reads the tools from Nexus, and the tool list can
	// depend on the caller. Clients must not cache it or share it.
	return &mcp.ListToolsResult{Tools: tools, Cacheable: mcp.Cacheable{TTLMs: 0, CacheScope: "private"}}, nil
}

// listToolsError maps a failed list_tools call to a JSON-RPC error.
func (c *connector) listToolsError(err error) *jsonrpc.Error {
	return &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: fmt.Sprintf("list tools of service %q: %s", c.opts.Service, err)}
}

// checkTools decodes the result of the list_tools operation and checks the
// tool names.
//
// The result is one JSON object. All tools are elements of its "tools" array:
//
//	{"tools": [{"name": "lookup", "inputSchema": {...}}, ...]}
//
// Each element decodes as an [mcp.Tool]. Unknown fields are ignored. A missing
// "tools" field gives an empty list. checkTools does not check underneath
// inputSchema or outputSchema, since the MCP spec says these could be any object.
//
// checkTools accepts all tools or none. It returns an error, and tools/list
// fails, when:
//   - the result is not one JSON object. An example is data after the object.
//   - a field has the wrong JSON type. An example is "name": 5.
//   - a tool is null or has an empty name.
//   - two tools have the same name.
func (c *connector) checkTools(raw []byte) ([]*mcp.Tool, error) {
	svc := c.opts.Service
	var result mcp.ListToolsResult
	if err := mcpwire.Decode(raw, &result); err != nil {
		return nil, fmt.Errorf("decode list_tools result of service %q: %w", svc, err)
	}
	// MCP requires "tools" to be an array. An empty list must not encode as
	// null.
	tools := []*mcp.Tool{}
	seen := map[string]bool{}
	for _, tool := range result.Tools {
		if tool == nil || tool.Name == "" {
			return nil, fmt.Errorf("service %q returned a tool without a name", svc)
		}
		if seen[tool.Name] {
			return nil, fmt.Errorf("service %q returned tool %q more than once", svc, tool.Name)
		}
		seen[tool.Name] = true
		tools = append(tools, tool)
	}
	return tools, nil
}

func (c *connector) callTool(ctx context.Context, prefix string, req *mcp.CallToolRequest) (mcp.Result, error) {
	args := map[string]any{}
	if len(req.Params.Arguments) > 0 {
		err := mcpwire.Decode(req.Params.Arguments, &args)
		if err != nil {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: fmt.Sprintf("arguments must be a JSON object: %s", err)}
		}
		if args == nil {
			return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "arguments must be a JSON object, not null"}
		}
	}
	// The tool name is the operation name. An operation that the service
	// does not have fails, and the failure becomes a tool error.
	// TODO(long-nt-tran): check the name against a cached tool list, so
	// tools/call can start only the operations that list_tools returns.
	name := req.Params.Name
	if name == "" || name == mcpwire.ListToolsOperation {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: fmt.Sprintf("unknown tool %q", name)}
	}
	h, err := c.nexus.start(ctx, c.operationID(ctx, prefix, name), name, args, 0)
	if err != nil {
		return mcpwire.Error(err.Error()), nil
	}

	out, err := wait(ctx, h)
	if ctx.Err() != nil {
		// The caller cancelled the tool call, so cancel the operation.
		if cancelErr := cancelOperation(ctx, h, "cancelled by MCP client"); cancelErr != nil {
			c.opts.Logger.Warn("could not cancel Nexus operation", "operation_id", h.GetID(), "error", cancelErr)
		}
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
	}
	return toCallToolResult(out), nil
}

// toCallToolResult maps a closed operation to an MCP tool result. A failed
// operation becomes a tool result with isError set.
func toCallToolResult(out outcome) *mcp.CallToolResult {
	if out.failure != nil {
		return mcpwire.Error(out.failure.Error())
	}
	var value any
	if len(out.value) > 0 {
		if err := mcpwire.Decode(out.value, &value); err != nil {
			return mcpwire.Error("invalid operation result: " + err.Error())
		}
	}
	result, err := mcpwire.Result(value)
	if err != nil {
		return mcpwire.Error("invalid operation result: " + err.Error())
	}
	return result
}
