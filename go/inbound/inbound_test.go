package inbound_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/temporalio/nexus-connector-mcp/go/inbound"
)

// stubClient creates Nexus clients and has no other methods.
type stubClient struct{ client.Client }

func (stubClient) NewNexusClient(client.NexusClientOptions) (client.NexusClient, error) {
	return nil, nil
}

func TestNewServerValidatesOptions(t *testing.T) {
	t.Parallel()

	valid := inbound.Options{
		Client:   stubClient{},
		Endpoint: "endpoint",
		Service:  "tools",
	}
	for _, test := range []struct {
		name, message string
		change        func(*inbound.Options)
	}{
		{"client", "Options.Client is required", func(o *inbound.Options) { o.Client = nil }},
		{"endpoint", "Options.Endpoint and Options.Service", func(o *inbound.Options) { o.Endpoint = " " }},
		{"service", "Options.Endpoint and Options.Service", func(o *inbound.Options) { o.Service = "" }},
		{"return task after", "ReturnTaskAfter", func(o *inbound.Options) { o.ReturnTaskAfter = -time.Second }},
		{"connector name", "ConnectorName", func(o *inbound.Options) { o.ConnectorName = string(make([]byte, 65)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			opts := valid
			test.change(&opts)
			server, err := inbound.NewServer(opts)
			require.ErrorContains(t, err, test.message)
			require.Nil(t, server)
		})
	}

	for _, wait := range []time.Duration{0, time.Second, inbound.NeverReturnTask} {
		opts := valid
		opts.ReturnTaskAfter = wait
		server, err := inbound.NewServer(opts)
		require.NoError(t, err)
		require.NotNil(t, server)
	}
}
