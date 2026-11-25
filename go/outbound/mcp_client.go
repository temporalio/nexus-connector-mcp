package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// resultEnvelopeKey marks an operation result that is already an MCP
	// tool result. The inbound connector and the Workflow client keep its
	// content, _meta, and isError.
	resultEnvelopeKey = "io.temporal/mcpCallToolResult"
)

// listTools reads all tool-list pages of the upstream. It returns an
// error if a tool name is empty, reserved, or repeated. The caller's context
// sets the deadline.
func listTools(ctx context.Context, u *Upstream) ([]*mcp.Tool, error) {
	// Get a session to the upstream MCP server through the provided ClientFactory.
	// For now, we'll open and close the connection on every call, which should be
	// ok since these are request-response type interactions, not long-lived.
	session, err := u.ClientFactory(ctx)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, errors.New("ClientFactory returned no session")
	}
	defer func() { _ = session.Close() }()

	// We use the MCP SDK's session.Tools(...) which:
	// - checks the MCP client cache for tool names, then
	// - paginates over the upstream MCP server's tools via "tools/list" call
	tools := []*mcp.Tool{}
	names := map[string]bool{}
	for tool, err := range session.Tools(ctx, nil) {
		if err != nil {
			return nil, err
		}
		if tool == nil {
			return nil, errors.New("upstream returned a null tool")
		}
		if !isToolName(tool.Name) {
			return nil, fmt.Errorf("upstream returned an empty or reserved tool name: %q", tool.Name)
		}
		if names[tool.Name] {
			return nil, fmt.Errorf("upstream returned tool %q more than once", tool.Name)
		}
		names[tool.Name] = true
		tools = append(tools, tool)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return tools, nil
}

// errProtocol reports an upstream result that the proxy cannot use.
var errProtocol = errors.New("invalid or incomplete upstream MCP result")

// callTool opens a session, calls the tool, and closes the session. The
// Activity context sets the deadline of the connection and the call.
func callTool(ctx context.Context, u *Upstream, tool string, arguments json.RawMessage) (map[string]json.RawMessage, error) {
	session, err := u.ClientFactory(ctx)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, errProtocol
	}
	defer func() { _ = session.Close() }()
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: arguments})
	if err != nil {
		return nil, err
	}
	// The proxy does not support upstream tasks or input requests yet, so it
	// rejects them. It does not treat them as a complete tool result.
	if result == nil || result.NeedsInput() {
		return nil, errProtocol
	}
	// The SDK decodes an empty content array as nil. Clients that require an
	// array reject null, so send an empty array.
	if result.Content == nil {
		complete := *result
		complete.Content = []mcp.Content{}
		result = &complete
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, errProtocol
	}
	var state struct {
		ResultType string `json:"resultType"`
	}
	if err := json.Unmarshal(encoded, &state); err != nil || (state.ResultType != "" && state.ResultType != "complete") {
		return nil, errProtocol
	}
	return map[string]json.RawMessage{resultEnvelopeKey: encoded}, nil
}
