// Sample Worker that proxies an upstream MCP server that requires OAuth.
//
// This is a sample, not a library API. main.go uses the connector library.
// oauth.go adds OAuth with golang.org/x/oauth2.
//
// Run:
//
//	OAUTH_CLIENT_SECRET=... go run . -upstream https://mcp.example.com/mcp \
//	  -token-url https://login.example.com/oauth/token -client-id mcp-proxy \
//	  -service weather
package main

import (
	"context"
	"flag"
	"log"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/temporalio/nexus-connector-mcp/go/outbound"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	var (
		temporal  = flag.String("temporal", client.DefaultHostPort, "Temporal frontend address")
		ns        = flag.String("namespace", client.DefaultNamespace, "Temporal namespace of the Worker")
		taskQueue = flag.String("task-queue", "mcp-proxy", "Worker task queue")
		service   = flag.String("service", "", "Nexus service name")
		upstream  = flag.String("upstream", "", "upstream MCP URL, such as https://mcp.example.com/mcp")
	)
	flag.Parse()

	c, err := client.Dial(client.Options{HostPort: *temporal, Namespace: *ns})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	httpClient := oauthHTTPClient(*upstream)
	plugin, err := outbound.NewPlugin(outbound.Options{
		Upstreams: []outbound.Upstream{{
			Service: *service,
			ClientFactory: func(ctx context.Context) (*mcp.ClientSession, error) {
				client := mcp.NewClient(&mcp.Implementation{Name: "oauth-outbound-connector", Version: "v0.0.0"}, nil)
				return client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: *upstream, HTTPClient: httpClient}, nil)
			},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}
	w := worker.New(c, *taskQueue, worker.Options{Plugins: []worker.Plugin{plugin}})
	log.Fatal(w.Run(worker.InterruptCh()))
}
