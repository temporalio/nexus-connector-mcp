// Package inbound serves Nexus operations as MCP tools.
//
// [NewServer] returns a standard [mcp.Server]. The user supplies the Temporal
// client, the Nexus endpoint and service, authentication middleware, and the
// server lifecycle. The user serves the returned server over Streamable HTTP
// or stdio with the MCP SDK transports.
//
// Each tool call starts a standalone Nexus operation in the connector
// namespace and waits for its result. Clients that support the MCP tasks
// extension can receive a task while the operation continues.
package inbound

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
)

// NeverReturnTask is an [Options.ReturnTaskAfter] value. The connector waits
// for the operation to complete or for the caller to cancel. It never returns
// a task.
const NeverReturnTask time.Duration = -1

// Options configures [NewServer].
type Options struct {
	// Client is the Temporal client. The connector starts the Nexus
	// operations in the namespace of Client. Required. Its data converter
	// must match the payload encoding of the Nexus handlers.
	Client client.Client

	// Endpoint is the Nexus endpoint name used to reach Service. Required.
	Endpoint string

	// Service is the Nexus service name. tools/list returns the tools of
	// this service, and tools/call starts the operation with the tool name.
	// Required.
	Service string

	// ConnectorName is an optional logical deployment name. Use the same
	// name across replicas.
	ConnectorName string

	// Logger receives connector logs. Defaults to slog.Default(). With
	// stdio, stdout is reserved for MCP.
	Logger *slog.Logger

	// ReturnTaskAfter sets how long the connector waits before it returns
	// a task to a client that supports the MCP tasks extension.
	//   - 0 (default): return a task right after the operation starts.
	//   - Positive: wait up to this duration. Return the tool result if it
	//     is ready. Otherwise return a task.
	//   - NeverReturnTask: never return a task.
	//   - Other negative values are invalid.
	//
	// It is not an operation execution timeout.
	ReturnTaskAfter time.Duration

	// CallerIdentity returns the verified caller identity as
	// issuer#subject. The connector uses it for task ownership checks.
	// The default reads the MCP SDK token info from the request context.
	// Set it when the user uses other authentication middleware.
	CallerIdentity func(context.Context) (string, bool)
}

// NewServer validates opts and returns an MCP server that maps MCP requests
// to standalone Nexus operations.
func NewServer(opts Options) (*mcp.Server, error) {
	if opts.Client == nil {
		return nil, errors.New("inbound: Options.Client is required")
	}
	if strings.TrimSpace(opts.Endpoint) == "" || strings.TrimSpace(opts.Service) == "" {
		return nil, errors.New("inbound: Options.Endpoint and Options.Service are required")
	}
	if opts.ReturnTaskAfter < 0 && opts.ReturnTaskAfter != NeverReturnTask {
		return nil, errors.New("inbound: Options.ReturnTaskAfter must be zero, positive, or NeverReturnTask")
	}
	if len(opts.ConnectorName) > maxConnectorNameBytes {
		return nil, fmt.Errorf("inbound: Options.ConnectorName must be at most %d bytes", maxConnectorNameBytes)
	}

	if opts.ConnectorName == "" {
		opts.ConnectorName = defaultConnectorName
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.CallerIdentity == nil {
		opts.CallerIdentity = tokenInfoIdentity
	}
	nexus, err := newNexusClient(opts.Client, opts.Endpoint, opts.Service)
	if err != nil {
		return nil, fmt.Errorf("inbound: %w", err)
	}
	return newServer(&connector{opts: &opts, nexus: nexus})
}
