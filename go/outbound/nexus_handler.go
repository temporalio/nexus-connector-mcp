package outbound

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporalnexus"
)

// listToolsResult is the output of list_tools: an MCP tools/list result with
// only the tools. mcp.ListToolsResult also encodes its cache fields, with an
// empty cacheScope that other MCP SDKs reject.
type listToolsResult struct {
	Tools []*mcp.Tool `json:"tools"`
}

// newService returns the Nexus service of an upstream. list_tools calls the
// upstream tools/list.
//
// TODO(long-nt-tran): investigate smart type inference here based on in/out
// schemas + past tool calls. This would allow us to provide smartly typed
// APIs given an upstream MCP spec.
func newService(u *Upstream) *nexus.Service {
	service := nexus.NewService(u.Service)
	service.MustRegister(nexus.NewSyncOperation(listToolsOperation,
		func(ctx context.Context, _ nexus.NoValue, _ nexus.StartOperationOptions) (*listToolsResult, error) {
			// The Worker logger comes from the client options of the user.
			tools, err := handleListTools(ctx, temporalnexus.GetLogger(ctx), u)
			if err != nil {
				return nil, err
			}
			return &listToolsResult{Tools: tools}, nil
		}))
	return service
}

// handleListTools runs discovery for each call. A failure gives an unavailable
// handler error. The log and the error leave out the cause, because upstream
// errors can hold credentials and private URLs.
//
// TODO(long-nt-tran): cache the last successful tool list, and use it before
// calling upstream (similar to what MCP SDK does).
func handleListTools(ctx context.Context, logger log.Logger, u *Upstream) ([]*mcp.Tool, error) {
	tools, err := listTools(ctx, u)
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn("could not discover upstream MCP tools", "service", u.Service)
		}
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "upstream tool list is not available")
	}
	return tools, nil
}
