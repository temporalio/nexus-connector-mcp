package gofixtures

import (
	"context"
	"net/http"
	"net/http/httptest"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/nexus-connector-mcp/e2e_tests/contract"
)

const (
	// UpstreamEcho echoes the message with the kind "upstream".
	UpstreamEcho = "echo"
	// UpstreamToolError returns a tool result with isError set.
	UpstreamToolError = "tool_error"
	// UpstreamBroken is listed, but tools/call for it returns a JSON-RPC
	// error.
	UpstreamBroken = "broken"
	// KindUpstream is the output kind of the upstream tools.
	KindUpstream = "upstream"
)

// Upstream is an MCP server for the outbound proxy to call.
type Upstream struct {
	// URL is the MCP URL of the server.
	URL string

	server *mcp.Server
	http   *httptest.Server
}

func echo(_ context.Context, _ *mcp.CallToolRequest, in contract.Input) (*mcp.CallToolResult, contract.Output, error) {
	return nil, contract.Output{Message: in.Message, Kind: KindUpstream}, nil
}

// StartUpstream serves the upstream MCP server over Streamable HTTP.
func StartUpstream() *Upstream {
	server := mcp.NewServer(&mcp.Implementation{Name: "upstream", Version: "v0.0.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: UpstreamEcho, Description: "Echo the message."}, echo)
	mcp.AddTool(server, &mcp.Tool{Name: UpstreamToolError, Description: "Return a tool error."},
		func(_ context.Context, _ *mcp.CallToolRequest, in contract.Input) (*mcp.CallToolResult, any, error) {
			return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: "tool error: " + in.Message}}}, nil, nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: UpstreamBroken, Description: "Fail with a JSON-RPC error."}, echo)
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if call, ok := req.(*mcp.CallToolRequest); ok && call.Params.Name == UpstreamBroken {
				return nil, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "broken upstream"}
			}
			return next(ctx, method, req)
		}
	})
	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: true},
	)
	httpServer := httptest.NewServer(handler)
	return &Upstream{URL: httpServer.URL, server: server, http: httpServer}
}

// AddEcho adds an echo tool named name, after the proxy has listed the tools.
func (u *Upstream) AddEcho(name string) {
	mcp.AddTool(u.server, &mcp.Tool{Name: name, Description: "Echo the message."}, echo)
}

// Close stops the server.
func (u *Upstream) Close() {
	u.http.CloseClientConnections()
	u.http.Close()
}
