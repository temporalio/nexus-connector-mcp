package outbound

import (
	"context"

	"go.temporal.io/sdk/worker"
)

const pluginName = "nexus-connector-mcp.outbound"

// plugin registers one Nexus service per upstream.
type plugin struct {
	worker.PluginBase

	// upstreams points to the copies that NewPlugin validated. The handlers
	// share them and do not change them.
	upstreams []*Upstream
}

var _ worker.Plugin = (*plugin)(nil)

func (*plugin) Name() string { return pluginName }

// StartWorker registers the Nexus service of each upstream.
func (p *plugin) StartWorker(ctx context.Context, opts worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
	for _, u := range p.upstreams {
		opts.WorkerRegistry.RegisterNexusService(newService(u))
	}
	return next(ctx, opts)
}
