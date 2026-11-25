package gofixtures

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/temporalio/nexus-connector-mcp/go/outbound"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// StartProxy starts a Worker on taskQueue with the outbound proxy plugin. The
// plugin serves upstreamURL as the Nexus service service. The returned
// function stops the Worker.
func StartProxy(c client.Client, taskQueue, service, upstreamURL string) (func(), error) {
	plugin, err := outbound.NewPlugin(outbound.Options{
		Upstreams: []outbound.Upstream{{
			Service: service,
			ClientFactory: func(ctx context.Context) (*mcp.ClientSession, error) {
				client := mcp.NewClient(&mcp.Implementation{Name: "e2e-proxy", Version: "v0.0.0"}, nil)
				return client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: upstreamURL}, nil)
			},
		}},
	})
	if err != nil {
		return nil, err
	}
	w := worker.New(c, taskQueue, worker.Options{Plugins: []worker.Plugin{plugin}})
	if err := w.Start(); err != nil {
		return nil, err
	}
	return w.Stop, nil
}
