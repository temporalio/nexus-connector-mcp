// Package outbound runs calls to upstream MCP servers as Temporal Activities.
//
// [NewPlugin] returns a Worker plugin. The plugin registers one Nexus service
// per [Upstream]. Each service has a list_tools operation that calls the
// upstream tools/list directly.
package outbound

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

const (
	listToolsOperation = "list_tools"
	// reservedPrefix starts names that Temporal reserves.
	reservedPrefix = "__temporal_"
)

// ClientFactory connects to an upstream MCP server over HTTP or stdio. The
// factory owns the transport, destination, and credentials.
//
// The proxy calls it once for each list_tools call, and closes the session
// after that call. A stdio factory starts one process for each call. Honor
// ctx during setup. The session must stay open after the setup context ends.
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
	//
	// Not implemented yet. NewPlugin rejects a non-empty map.
	ActivityOptionsOverrides map[string]client.StartActivityOptions

	// ClassifyError optionally overrides the built-in classification of a
	// failed upstream call. Cancellation skips this hook.
	//
	// Not implemented yet. NewPlugin rejects a non-nil hook.
	ClassifyError func(err error, def FailureClassification) FailureClassification
}

// Options configures [NewPlugin].
type Options struct {
	// Upstreams lists the upstream MCP servers. Each one maps to its own
	// Nexus service.
	Upstreams []Upstream
}

// NewPlugin validates opts and returns a Worker plugin that registers the
// proxy Nexus services.
//
// Factories must support concurrent calls. Do not change opts while a Worker
// runs.
func NewPlugin(opts Options) (worker.Plugin, error) {
	if len(opts.Upstreams) == 0 {
		return nil, errors.New("outbound: Options.Upstreams needs at least one Upstream")
	}
	p := &plugin{}
	services := map[string]bool{}
	for _, u := range opts.Upstreams {
		prepared, err := prepareUpstream(u)
		if err != nil {
			return nil, err
		}
		if services[u.Service] {
			return nil, fmt.Errorf("outbound: duplicate Upstream service %q", u.Service)
		}
		services[u.Service] = true
		p.upstreams = append(p.upstreams, &prepared)
	}
	return p, nil
}

// isToolName reports whether name can be an upstream tool name. list_tools
// and names that start with __temporal_ are reserved.
func isToolName(name string) bool {
	return strings.TrimSpace(name) != "" && name != listToolsOperation && !strings.HasPrefix(name, reservedPrefix)
}

// prepareUpstream validates u and returns a copy that later changes by the
// caller do not affect.
func prepareUpstream(u Upstream) (Upstream, error) {
	if strings.TrimSpace(u.Service) == "" || strings.HasPrefix(u.Service, reservedPrefix) {
		return u, fmt.Errorf("outbound: Upstream.Service %q must be non-empty and not reserved", u.Service)
	}
	if u.ClientFactory == nil {
		return u, fmt.Errorf("outbound: Upstream %q needs a ClientFactory", u.Service)
	}
	if len(u.ActivityOptionsOverrides) > 0 {
		return u, fmt.Errorf("outbound: Upstream %q: ActivityOptionsOverrides is not implemented yet", u.Service)
	}
	if u.ClassifyError != nil {
		return u, fmt.Errorf("outbound: Upstream %q: ClassifyError is not implemented yet", u.Service)
	}
	return u, nil
}
