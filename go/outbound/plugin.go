package outbound

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
)

const (
	pluginName = "nexus-connector-mcp.outbound"
)

// plugin registers one Nexus service and one call Activity per upstream.
type plugin struct {
	worker.PluginBase

	// upstreams points to the copies that NewPlugin validated. The handlers
	// share them and do not change them.
	upstreams []*Upstream
	caches    map[string]*toolCache
}

var _ worker.Plugin = (*plugin)(nil)

func (*plugin) Name() string { return pluginName }

// StartWorker registers the call Activity and the Nexus service of each
// upstream.
func (p *plugin) StartWorker(ctx context.Context, opts worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
	for _, u := range p.upstreams {
		activityName := activityName(u.Service)
		opts.WorkerRegistry.RegisterActivityWithOptions(callToolActivity(u), activity.RegisterOptions{Name: activityName})
		opts.WorkerRegistry.RegisterNexusService(newService(u, p.caches[u.Service], activityName))
	}
	return next(ctx, opts)
}
