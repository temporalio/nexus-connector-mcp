# Nexus MCP Connector for Go

Go module `github.com/temporalio/nexus-connector-mcp/go`. It serves Temporal Nexus operations as MCP
tools.

| Package | Purpose |
| --- | --- |
| `nexusmcp` | Mark Nexus operations as MCP tools. |
| `inbound` | Serve the tools of a Nexus service to MCP clients. |

> [!NOTE]
> This project is currently experimental. APIs are subject to change.

## Requirements

- Go 1.26 or later
- A Temporal server with Standalone Nexus Operations enabled

## Installation

```bash
go get github.com/temporalio/nexus-connector-mcp/go
```

## Mark operations as tools

`nexusmcp.Tool` marks a Nexus operation as an MCP tool. `nexusmcp.Register` registers the marked
operations on a Nexus service and adds a `list_tools` operation. Register the service on a Worker
as usual.

```go
type WeatherInput struct {
	City string `json:"city" jsonschema:"the city name"`
}

type WeatherOutput struct {
	Forecast string `json:"forecast"`
}

var getWeather = temporalnexus.NewWorkflowRunOperation("get_weather", WeatherWorkflow,
	func(_ context.Context, _ WeatherInput, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
		return client.StartWorkflowOptions{ID: opts.RequestID}, nil
	})

func main() {
	svc := nexus.NewService("weather")
	err := nexusmcp.Register(svc,
		nexusmcp.Tool(getWeather, nexusmcp.ToolOptions{Description: "Get the weather forecast for a city."}),
	)
	if err != nil {
		log.Fatal(err)
	}
	w.RegisterNexusService(svc)
}
```

- The tool name is the Nexus operation name.
- The input and output schemas come from the operation types. The `json` and `jsonschema` struct
  tags set the field names and descriptions.
- The input type must be a struct, a pointer to a struct, or a map with string keys.
- Sync and async operations work, such as Workflow, Activity, and Update operations.
- `list_tools` lists only the operations passed to `nexusmcp.Tool`. Register other operations with
  `svc.Register`.

## Serve tools to MCP clients

`inbound.NewServer` returns a standard `*mcp.Server`. Serve it with an MCP SDK transport.

```go
func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	server, err := inbound.NewServer(inbound.Options{
		Client:   c,
		Endpoint: "weather-endpoint",
		Service:  "weather",
	})
	if err != nil {
		log.Fatal(err)
	}
	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{Stateless: true, PropagateRequestCancellation: true},
	)
	// Add authentication middleware around handler here.
	log.Fatal(http.ListenAndServe(":8080", handler))
}
```

| MCP method | Nexus |
| --- | --- |
| `tools/list` | Runs the `list_tools` operation of the service. |
| `tools/call` | Starts the operation with the tool name and waits for its result. |
| `tasks/get` | Describes the operation. Returns the result when it is complete. |
| `tasks/cancel` | Cancels the operation. |

Each tool call starts a standalone Nexus operation in the namespace of `Client`. The Nexus endpoint
must allow that namespace as a caller. A failed operation returns a tool result with `isError` set.
If the MCP client cancels the call, the connector cancels the operation. Over stateless Streamable
HTTP, the client cancels by closing the request, so set `PropagateRequestCancellation`.

### Options

| Field | Purpose |
| --- | --- |
| `Client` | Temporal client. Required. Its data converter must match the Nexus handlers. |
| `Endpoint` | Nexus endpoint name. Required. |
| `Service` | Nexus service name. Required. |
| `ConnectorName` | MCP server name. Use the same name across replicas. |
| `Logger` | Connector logs. Defaults to `slog.Default()`. |
| `ReturnTaskAfter` | When to return an MCP task. See [Tasks](#tasks). |
| `CallerIdentity` | Returns the verified caller identity. See [Authentication](#authentication). |

### Tasks

Clients that declare the MCP tasks extension can get a task while the operation runs. The task ID
is the Nexus operation ID. `ReturnTaskAfter` sets when the connector returns the task:

| Value | Behavior |
| --- | --- |
| `0` (default) | Return a task right after the operation starts. |
| Positive | Wait up to this duration. Return the result if it is ready, otherwise a task. |
| `inbound.NeverReturnTask` | Always wait for the result. |

A client without the tasks extension waits for the result.

### Authentication

Add authentication, TLS, and rate limiting with your own HTTP middleware around the handler.

The connector binds each task to the caller identity. Only the same caller can read or cancel the
task. Without an identity, any caller with the task ID can read or cancel it. By default, the
identity is `TokenInfo.UserID` from the MCP SDK
[`auth`](https://pkg.go.dev/github.com/modelcontextprotocol/go-sdk/auth) middleware. Set
`CallerIdentity` to read the identity from other middleware.

## Call tools from a Workflow

`NexusMCPClient` calls the tools from Workflow code. See the
[Python](../python/README.md#call-tools-from-a-workflow) and
[TypeScript](../typescript/README.md#call-tools-from-a-workflow) READMEs.

## Development

See [CONTRIBUTING.md](../.github/CONTRIBUTING.md).

## License

[MIT](../LICENSE)
