// Sample host that serves the inbound connector over Streamable HTTP behind
// OAuth. The connector acts as an OAuth resource server.
//
// This is a sample, not a library API. main.go uses the connector library.
// oauth.go adds OAuth with go-oidc and the MCP SDK auth package.
//
// The host supplies HTTPS. The MCP client owns login and token refresh.
//
// Run:
//
//	go run . -resource https://connector.example.com/mcp \
//	  -issuer https://login.example.com -audience https://connector.example.com \
//	  -endpoint weather-endpoint -service weather
package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/temporalio/nexus-connector-mcp/go/inbound"
	"go.temporal.io/sdk/client"
)

func main() {
	var (
		addr     = flag.String("addr", ":8080", "HTTP listen address")
		temporal = flag.String("temporal", client.DefaultHostPort, "Temporal frontend address")
		ns       = flag.String("namespace", client.DefaultNamespace, "Temporal namespace of the connector")
		endpoint = flag.String("endpoint", "", "Nexus endpoint name")
		service  = flag.String("service", "", "Nexus service name")
	)
	flag.Parse()

	c, err := client.Dial(client.Options{HostPort: *temporal, Namespace: *ns})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	server, err := inbound.NewServer(inbound.Options{Client: c, Endpoint: *endpoint, Service: *service})
	if err != nil {
		log.Fatal(err)
	}
	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		// The host chooses the transport options: stateless or stateful, and
		// whether a client disconnect cancels the tool call. This sample
		// follows the MCP 2026-07-28 spec, where closing the request of a
		// stateless call cancels it. The connector then cancels the operation.
		&mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true},
	)
	protected, err := withOAuth(handler)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(http.ListenAndServe(*addr, protected))
}
