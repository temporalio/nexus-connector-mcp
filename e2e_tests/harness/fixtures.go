package harness

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/temporalio/nexus-connector-mcp/go/inbound"

	gofixtures "github.com/temporalio/nexus-connector-mcp/e2e_tests/fixtures/go"
)

// Langs are the languages that implement the contract handler.
var Langs = []string{"go", "python", "typescript"}

// ProxyLangs are the languages that implement the outbound proxy.
var ProxyLangs = []string{"go", "python"}

// UpstreamService is the Nexus service name of the proxied upstream.
const UpstreamService = "upstream"

// Handler starts the contract handler of lang once, and returns the Nexus
// endpoint that targets it.
func (e *Env) Handler(lang string) (string, error) {
	return Fixture(e, "handler-"+lang, func() (string, error) {
		taskQueue := "contract-" + lang
		endpoint := "e2e-contract-" + lang
		if lang == "go" {
			stop, err := gofixtures.StartHandler(context.Background(), e.Client, taskQueue)
			if err != nil {
				return "", err
			}
			e.OnStop(stop)
		} else if _, err := e.StartProcess(lang, "handler", "handler-"+lang, "--task-queue", taskQueue); err != nil {
			return "", err
		}
		return endpoint, e.CreateEndpoint(context.Background(), endpoint, taskQueue)
	})
}

// Upstream starts the upstream MCP server once.
func (e *Env) Upstream() (*gofixtures.Upstream, error) {
	return Fixture(e, "upstream", func() (*gofixtures.Upstream, error) {
		upstream := gofixtures.StartUpstream()
		e.OnStop(upstream.Close)
		return upstream, nil
	})
}

// Proxy starts the outbound proxy of lang once, and returns the Nexus endpoint
// that targets it. The proxy serves the upstream as UpstreamService.
func (e *Env) Proxy(lang string) (string, error) {
	return Fixture(e, "proxy-"+lang, func() (string, error) {
		upstream, err := e.Upstream()
		if err != nil {
			return "", err
		}
		taskQueue := "proxy-" + lang
		endpoint := "e2e-proxy-" + lang
		if lang == "go" {
			stop, err := gofixtures.StartProxy(e.Client, taskQueue, UpstreamService, upstream.URL)
			if err != nil {
				return "", err
			}
			e.OnStop(stop)
		} else if _, err := e.StartProcess(lang, "proxy", "proxy-"+lang,
			"--task-queue", taskQueue, "--service", UpstreamService, "--upstream", upstream.URL); err != nil {
			return "", err
		}
		return endpoint, e.CreateEndpoint(context.Background(), endpoint, taskQueue)
	})
}

// ConnectorOptions configure a connector fixture.
type ConnectorOptions struct {
	Endpoint        string
	Service         string
	ReturnTaskAfter time.Duration
}

// Connector serves an inbound connector over stateless Streamable HTTP, once
// per option set. It returns the MCP URL. A client disconnect cancels the tool
// call, as the README of the go module recommends.
func (e *Env) Connector(opts ConnectorOptions) (string, error) {
	key := fmt.Sprintf("connector-%+v", opts)
	return Fixture(e, key, func() (string, error) {
		server, err := inbound.NewServer(inbound.Options{
			Client:          e.Client,
			Endpoint:        opts.Endpoint,
			Service:         opts.Service,
			ReturnTaskAfter: opts.ReturnTaskAfter,
		})
		if err != nil {
			return "", err
		}
		handler := mcp.NewStreamableHTTPHandler(
			func(*http.Request) *mcp.Server { return server },
			&mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true},
		)
		httpServer := httptest.NewServer(handler)
		e.OnStop(func() {
			// A tool call can still wait on an operation. Close its connection
			// first, so Close does not wait for it.
			httpServer.CloseClientConnections()
			httpServer.Close()
		})
		return httpServer.URL, nil
	})
}

// ConnectMCP connects a Go MCP client to url.
func ConnectMCP(ctx context.Context, url string) (*mcp.ClientSession, error) {
	client := mcp.NewClient(&mcp.Implementation{Name: "e2e", Version: "v0.0.0"}, nil)
	return client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: url}, nil)
}
