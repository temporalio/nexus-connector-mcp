package outbound

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
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
