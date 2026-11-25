package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/temporalio/nexus-connector-mcp/go/inbound"
)

// Callers are the ways to call a Nexus-backed MCP tool:
//   - "go-mcp": a Go MCP client through the inbound connector.
//   - "python" and "typescript": a Workflow with NexusMCPClient.
var Callers = []string{"go-mcp", "python", "typescript"}

// CallResult is the outcome of a tool call, in the same shape for every
// caller.
type CallResult struct {
	// Tools are the tool names that the caller listed, sorted.
	Tools []string
	// IsError reports a tool error.
	IsError bool
	// StructuredContent is the structured content of the result.
	StructuredContent json.RawMessage
	// Text is the text of the first content block.
	Text string
}

// callOutput is the JSON that the Workflow caller programs print.
type callOutput struct {
	Tools  []string `json:"tools"`
	Result struct {
		IsError           bool            `json:"isError"`
		StructuredContent json.RawMessage `json:"structuredContent"`
		Content           []struct {
			Text string `json:"text"`
		} `json:"content"`
	} `json:"result"`
}

// CallTool lists the tools of service behind endpoint, then calls tool with
// arguments, through caller.
func (e *Env) CallTool(ctx context.Context, caller, endpoint, service, tool string, arguments any) (CallResult, error) {
	if caller == "go-mcp" {
		return e.callThroughConnector(ctx, endpoint, service, tool, arguments)
	}
	var out callOutput
	input := map[string]any{"endpoint": endpoint, "service": service, "tool": tool, "arguments": arguments}
	if err := e.RunJSON(ctx, caller, "caller", input, &out); err != nil {
		return CallResult{}, err
	}
	result := CallResult{Tools: out.Tools, IsError: out.Result.IsError, StructuredContent: out.Result.StructuredContent}
	if len(out.Result.Content) > 0 {
		result.Text = out.Result.Content[0].Text
	}
	return result, nil
}

func (e *Env) callThroughConnector(ctx context.Context, endpoint, service, tool string, arguments any) (CallResult, error) {
	url, err := e.Connector(ConnectorOptions{Endpoint: endpoint, Service: service, ReturnTaskAfter: inbound.NeverReturnTask})
	if err != nil {
		return CallResult{}, err
	}
	session, err := ConnectMCP(ctx, url)
	if err != nil {
		return CallResult{}, err
	}
	defer func() { _ = session.Close() }()
	listed, err := session.ListTools(ctx, nil)
	if err != nil {
		return CallResult{}, fmt.Errorf("tools/list: %w", err)
	}
	var result CallResult
	for _, t := range listed.Tools {
		result.Tools = append(result.Tools, t.Name)
	}
	slices.Sort(result.Tools)
	called, err := session.CallTool(ctx, &mcp.CallToolParams{Name: tool, Arguments: arguments})
	if err != nil {
		return CallResult{}, fmt.Errorf("tools/call: %w", err)
	}
	result.IsError = called.IsError
	if called.StructuredContent != nil {
		if result.StructuredContent, err = json.Marshal(called.StructuredContent); err != nil {
			return CallResult{}, err
		}
	}
	if len(called.Content) > 0 {
		if text, ok := called.Content[0].(*mcp.TextContent); ok {
			result.Text = text.Text
		}
	}
	return result, nil
}
