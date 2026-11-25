# Go module

Go module for the Nexus MCP connector: `github.com/temporalio/nexus-connector-mcp/go`.

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Features

| Feature | Package | Entry point |
| --- | --- | --- |
| Inbound connector: serves Nexus operations as MCP tools. | `inbound` | `NewServer` returns an `*mcp.Server`. |
| Outbound proxy: runs upstream MCP tool calls as Activities behind a Nexus service. | `outbound` | `NewPlugin` returns a Worker plugin. |
| Authoring: marks Nexus operations as MCP tools. | `handler` | `Tool` marks an operation. `Register` registers the marked operations and `list_tools`. |

The Workflow client is in the [Python](../python) and [TypeScript](../typescript) packages.

## Proxy upstream MCP servers

`outbound.NewPlugin` returns a Worker plugin that proxies upstream MCP servers. Each `Upstream`
becomes a Nexus service on the Worker. Each tool call runs as a Standalone Activity, so Temporal
retries, times out, and records it.

The Temporal server must have Standalone Activities enabled.

```go
func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	plugin, err := outbound.NewPlugin(outbound.Options{
		Upstreams: []outbound.Upstream{{
			Service: "github",
			ClientFactory: func(ctx context.Context) (*mcp.ClientSession, error) {
				client := mcp.NewClient(&mcp.Implementation{Name: "mcp-proxy", Version: "v1"}, nil)
				return client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: "https://api.example.com/mcp"}, nil)
			},
		}},
	})
	if err != nil {
		log.Fatal(err)
	}
	w := worker.New(c, "mcp-proxy", worker.Options{Plugins: []worker.Plugin{plugin}})
	log.Fatal(w.Run(worker.InterruptCh()))
}
```

Create a Nexus endpoint that targets the Worker task queue. Callers then use the service like any
Nexus service:

| Nexus operation | Upstream |
| --- | --- |
| `list_tools` | Calls `tools/list`. Returns the last successful list if the upstream is down. |
| Tool name | Starts an Activity that calls `tools/call`. Returns the MCP tool result. |

If the upstream tool returns a result with `isError` set, the caller gets that result and the
Activity completes. A failed upstream call fails the Activity. See [Failures](#failures).

### Upstream options

| Field | Purpose |
| --- | --- |
| `Service` | Nexus service name. Required. |
| `ClientFactory` | Connects to the upstream. It sets the transport, URL, and credentials. Required. |
| `ActivityOptionsOverrides` | Activity options per tool name, such as timeouts, retry policy, and heartbeat timeout. |
| `ClassifyError` | Overrides the failure classification. |

The proxy calls `ClientFactory` once for each discovery and each Activity attempt. The credentials
stay in the Worker, because the Activity input holds only the tool name and arguments.

The [OAuth sample](../samples/go/oauth_outbound_connector) connects to an upstream that requires
OAuth.

### Activity defaults

- Start-to-close timeout: 1 minute.
- Retry policy: the Temporal default. A tool annotated as destructive, not read-only, and not
  idempotent gets one attempt.
- Heartbeats: off. Set `HeartbeatTimeout` to turn them on. Heartbeats deliver cancellation to the
  running call.

### Failures

A failed upstream call fails the Activity with one of these types. The failure message holds only
the type, because upstream messages can hold private data.

| Type | Cause | Retryable |
| --- | --- | --- |
| `UpstreamConnectionError` | Network error, closed connection, or timeout. | Yes |
| `UpstreamAuthenticationError` | Permission denied. | No |
| `UpstreamProtocolError` | Invalid MCP or JSON-RPC response. | No |
| `ProxyConfigurationError` | Missing command or file, or an invalid `ClassifyError` result. | No |
| `UpstreamError` | Any other error. | No |

## Development

See [CONTRIBUTING.md](../.github/CONTRIBUTING.md).
