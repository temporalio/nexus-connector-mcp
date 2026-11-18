// Package outbound runs calls to upstream MCP servers as Temporal Activities.
//
// [NewPlugin] returns a Worker plugin. The plugin registers one Nexus service
// per [Upstream]. Each service lists the upstream tools and runs each tool call
// as a standalone Activity.
package outbound

import (
	"context"
	"errors"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// ClientFactory connects to an upstream MCP server over HTTP or stdio. The
// factory owns the transport, destination, and credentials.
type ClientFactory func(context.Context) (*mcp.ClientSession, error)

// FailureClassification sets how a failed upstream call is reported to
// Temporal.
type FailureClassification struct {
	// Type is a stable failure type.
	Type string
	// Retryable reports whether the Activity retry policy applies.
	Retryable bool
}

// Upstream configures one upstream MCP server.
type Upstream struct {
	// Service is the Nexus service name for this upstream. Required.
	Service string

	// ClientFactory connects to the upstream. Required.
	ClientFactory ClientFactory

	// ActivityOptionsOverrides maps an upstream tool name to Activity
	// options for that tool. Zero-valued fields use the proxy defaults.
	// Connector-owned fields must be zero: Activity ID, task queue,
	// conflict and reuse policies, and Nexus callbacks.
	ActivityOptionsOverrides map[string]client.StartActivityOptions

	// ClassifyError optionally overrides the built-in classification of a
	// failed upstream call. Cancellation skips this hook.
	ClassifyError func(err error, def FailureClassification) FailureClassification
}

// Options configures [NewPlugin].
type Options struct {
	// Upstreams lists the upstream MCP servers. Each one maps to its own
	// Nexus service.
	Upstreams []Upstream
}

// NewPlugin validates opts and returns a Worker plugin that registers the
// proxy Nexus services and Activities.
func NewPlugin(opts Options) (worker.Plugin, error) {
	_ = opts
	return nil, errors.New("outbound: NewPlugin is not implemented")
}
