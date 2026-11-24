package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// fakeUpstream answers tools/list for a real MCP client session.
type fakeUpstream struct {
	list   func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error)
	closes *atomic.Int32
}

// session returns a real MCP client session whose tools/list goes to f. The
// client middleware lets f return failures and malformed data that a real
// server would not send.
func (f *fakeUpstream) session(t *testing.T) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "upstream", Version: "1"}, nil)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "proxy", Version: "1"}, nil)
	mcpClient.AddSendingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			if method == "tools/list" {
				return f.list(ctx, request.GetParams().(*mcp.ListToolsParams))
			}
			return next(ctx, method, request)
		}
	})
	session, err := mcpClient.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close(); _ = serverSession.Close() })
	if f.closes != nil {
		go func() { _ = serverSession.Wait(); f.closes.Add(1) }()
	}
	return session
}

func (f *fakeUpstream) factory(t *testing.T) ClientFactory {
	return func(context.Context) (*mcp.ClientSession, error) { return f.session(t), nil }
}

// discardLogger drops the logs of functions that tests call outside a Nexus
// operation.
var discardLogger = log.NewStructuredLogger(slog.New(slog.DiscardHandler))

func prepared(t *testing.T, u Upstream) *Upstream {
	t.Helper()
	if u.Service == "" {
		u.Service = "tools"
	}
	if u.ClientFactory == nil {
		u.ClientFactory = func(context.Context) (*mcp.ClientSession, error) { return nil, errors.New("not connected") }
	}
	result, err := prepareUpstream(u)
	require.NoError(t, err)
	return &result
}

func TestNewPluginValidatesOptions(t *testing.T) {
	t.Parallel()

	factory := func(context.Context) (*mcp.ClientSession, error) { return nil, errors.New("not connected") }
	valid := Upstream{Service: "tools", ClientFactory: factory}
	for name, test := range map[string]struct {
		opts    Options
		message string
	}{
		"no upstreams":     {Options{}, "at least one Upstream"},
		"duplicate":        {Options{Upstreams: []Upstream{valid, valid}}, "duplicate"},
		"empty service":    {Options{Upstreams: []Upstream{{ClientFactory: factory}}}, "non-empty and not reserved"},
		"reserved service": {Options{Upstreams: []Upstream{{Service: "__temporal_x", ClientFactory: factory}}}, "non-empty and not reserved"},
		"no factory":       {Options{Upstreams: []Upstream{{Service: "tools"}}}, "needs a ClientFactory"},
		"overrides": {Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: factory,
			ActivityOptionsOverrides: map[string]client.StartActivityOptions{"tool": {}}}}}, "not implemented"},
		"classify hook": {Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: factory,
			ClassifyError: func(_ error, def FailureClassification) FailureClassification { return def }}}}, "not implemented"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p, err := NewPlugin(test.opts)
			require.ErrorContains(t, err, test.message)
			require.Nil(t, p)
		})
	}

	p, err := NewPlugin(Options{Upstreams: []Upstream{valid, {Service: "other", ClientFactory: factory}}})
	require.NoError(t, err)
	require.Equal(t, pluginName, p.Name())
}

func TestListToolsReadsAllPagesAndKeepsMetadata(t *testing.T) {
	t.Parallel()

	var closes atomic.Int32
	pages := map[string]*mcp.ListToolsResult{
		"": {Tools: []*mcp.Tool{{
			Name:         "a",
			InputSchema:  map[string]any{"type": "object"},
			OutputSchema: map[string]any{"type": "object", "properties": map[string]any{"message": map[string]any{"type": "string"}}},
			Annotations:  &mcp.ToolAnnotations{ReadOnlyHint: true, IdempotentHint: true, OpenWorldHint: new(false)},
			Meta:         mcp.Meta{"source": "upstream"},
		}}, NextCursor: "second"},
		"second": {Tools: []*mcp.Tool{{Name: "b"}}},
	}
	upstream := &fakeUpstream{closes: &closes, list: func(_ context.Context, p *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
		return pages[p.Cursor], nil
	}}
	tools, err := handleListTools(t.Context(), discardLogger, prepared(t, Upstream{ClientFactory: upstream.factory(t)}))
	require.NoError(t, err)
	require.Len(t, tools, 2)
	require.Equal(t, "object", tools[0].InputSchema.(map[string]any)["type"])
	require.Equal(t, "object", tools[0].OutputSchema.(map[string]any)["type"])
	require.True(t, tools[0].Annotations.ReadOnlyHint)
	require.True(t, tools[0].Annotations.IdempotentHint)
	require.False(t, *tools[0].Annotations.OpenWorldHint)
	require.Equal(t, "upstream", tools[0].Meta["source"])
	require.Equal(t, "b", tools[1].Name)
	// The proxy closes each discovery session.
	require.Eventually(t, func() bool { return closes.Load() == 1 }, time.Second, time.Millisecond)
}

// The list_tools result has only the tools. MCP SDKs reject the empty
// cacheScope that mcp.ListToolsResult encodes.
func TestListToolsResultHasOnlyTools(t *testing.T) {
	t.Parallel()

	upstream := &fakeUpstream{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
		return &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "a"}}}, nil
	}}
	u := prepared(t, Upstream{ClientFactory: upstream.factory(t)})
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterNexusService(newService(u))
	env.ExecuteWorkflow(func(ctx workflow.Context) (map[string]json.RawMessage, error) {
		var result map[string]json.RawMessage
		err := workflow.NewNexusClient("endpoint", u.Service).
			ExecuteOperation(ctx, listToolsOperation, nil, workflow.NexusOperationOptions{}).
			Get(ctx, &result)
		return result, err
	})
	require.NoError(t, env.GetWorkflowError())
	var result map[string]json.RawMessage
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, []string{"tools"}, slices.Collect(maps.Keys(result)))
}

func TestListToolsRejectsBadLists(t *testing.T) {
	t.Parallel()

	var page *mcp.ListToolsResult
	upstream := &fakeUpstream{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) { return page, nil }}
	u := prepared(t, Upstream{ClientFactory: upstream.factory(t)})
	page = &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "weather.lookup"}, {Name: strings.Repeat("x", 100)}}}
	_, err := listTools(t.Context(), u)
	require.NoError(t, err)

	for _, bad := range [][]*mcp.Tool{
		{{Name: "list_tools"}},
		{{Name: "__temporal_internal"}},
		{{Name: "dup"}, {Name: "dup"}},
	} {
		page = &mcp.ListToolsResult{Tools: bad}
		_, err := listTools(t.Context(), u)
		require.Error(t, err)
	}
}

func TestHandleListToolsFailureIsUnavailable(t *testing.T) {
	t.Parallel()

	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return nil, errors.New("dial https://private.example.com?token=secret")
	}})
	_, err := handleListTools(t.Context(), discardLogger, u)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerErr.Type)
	require.NotContains(t, err.Error(), "secret")
}

// testRegistry records the registrations of the plugin.
type testRegistry struct {
	worker.Worker

	services []*nexus.Service
}

func (r *testRegistry) RegisterNexusService(service *nexus.Service) {
	r.services = append(r.services, service)
}

func TestPluginRegistersListTools(t *testing.T) {
	t.Parallel()

	p, err := NewPlugin(Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return nil, errors.New("not connected")
	}}}})
	require.NoError(t, err)
	registry := &testRegistry{}
	started := false
	err = p.(*plugin).StartWorker(t.Context(), worker.PluginStartWorkerOptions{WorkerRegistry: registry},
		func(context.Context, worker.PluginStartWorkerOptions) error { started = true; return nil })
	require.NoError(t, err)
	require.True(t, started)
	require.Len(t, registry.services, 1)
	require.Equal(t, "tools", registry.services[0].Name)
	require.NotNil(t, registry.services[0].Operation("list_tools"))
}
