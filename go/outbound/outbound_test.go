package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

// fakeUpstream answers tools/list and tools/call for a real MCP client
// session.
type fakeUpstream struct {
	list   func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error)
	call   func(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error)
	closes *atomic.Int32
}

// session returns a real MCP client session whose tools/list and tools/call
// go to f. The client middleware lets f return failures and malformed data
// that a real server would not send.
func (f *fakeUpstream) session(t *testing.T) *mcp.ClientSession {
	t.Helper()
	server := mcp.NewServer(&mcp.Implementation{Name: "upstream", Version: "1"}, nil)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "proxy", Version: "1"}, nil)
	mcpClient.AddSendingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, request mcp.Request) (mcp.Result, error) {
			switch method {
			case "tools/list":
				return f.list(ctx, request.GetParams().(*mcp.ListToolsParams))
			case "tools/call":
				return f.call(ctx, request.GetParams().(*mcp.CallToolParams))
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

func listing(names ...string) func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
	return func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
		tools := make([]*mcp.Tool, 0, len(names))
		for _, name := range names {
			tools = append(tools, &mcp.Tool{Name: name})
		}
		return &mcp.ListToolsResult{Tools: tools}, nil
	}
}

func prepared(t *testing.T, u Upstream) Upstream {
	t.Helper()
	if u.Service == "" {
		u.Service = "tools"
	}
	if u.ClientFactory == nil {
		u.ClientFactory = func(context.Context) (*mcp.ClientSession, error) { return nil, errors.New("not connected") }
	}
	result, err := prepareUpstream(u)
	require.NoError(t, err)
	return result
}

func TestActivityOptionsDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	u := prepared(t, Upstream{})
	defaults, err := u.optionsFor("tool", nil)
	require.NoError(t, err)
	require.Equal(t, time.Minute, defaults.StartToCloseTimeout)
	require.Zero(t, defaults.ScheduleToCloseTimeout)
	require.Zero(t, defaults.HeartbeatTimeout)
	require.Nil(t, defaults.RetryPolicy)
	require.Zero(t, defaults.Priority)
	require.Zero(t, defaults.TypedSearchAttributes.Size())

	// A destructive tool that is not idempotent gets one attempt.
	annotations := &mcp.ToolAnnotations{DestructiveHint: new(true)}
	opts, err := u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.EqualValues(t, 1, opts.RetryPolicy.MaximumAttempts)
	annotations.IdempotentHint = true
	opts, err = u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.Nil(t, opts.RetryPolicy)
	annotations.IdempotentHint, annotations.ReadOnlyHint = false, true
	opts, err = u.optionsFor("tool", annotations)
	require.NoError(t, err)
	require.Nil(t, opts.RetryPolicy)

	policy := &temporal.RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: []string{"CustomFailure"}}
	priority := temporal.Priority{PriorityKey: 2, FairnessKey: "tenant", FairnessWeight: 2}
	attributes := temporal.NewSearchAttributes(temporal.NewSearchAttributeKeyKeyword("project").ValueSet("weather"))
	u = prepared(t, Upstream{ActivityOptionsOverrides: map[string]client.StartActivityOptions{
		"tool": {
			StartToCloseTimeout:    time.Hour,
			ScheduleToCloseTimeout: 2 * time.Hour,
			ScheduleToStartTimeout: 5 * time.Second,
			HeartbeatTimeout:       30 * time.Second,
			StartDelay:             time.Second,
			RetryPolicy:            policy,
			Priority:               priority,
			TypedSearchAttributes:  attributes,
			Summary:                "weather tool",
			StaticDetails:          "Runs the upstream tool",
		},
	}})
	opts, err = u.optionsFor("tool", &mcp.ToolAnnotations{DestructiveHint: new(true)})
	require.NoError(t, err)
	require.Equal(t, time.Hour, opts.StartToCloseTimeout)
	require.Equal(t, 2*time.Hour, opts.ScheduleToCloseTimeout)
	require.Equal(t, 5*time.Second, opts.ScheduleToStartTimeout)
	require.Equal(t, 30*time.Second, opts.HeartbeatTimeout)
	require.Equal(t, time.Second, opts.StartDelay)
	require.Same(t, policy, opts.RetryPolicy)
	require.Equal(t, priority, opts.Priority)
	require.Equal(t, 1, opts.TypedSearchAttributes.Size())
	require.Equal(t, "weather tool", opts.Summary)
	require.Equal(t, "Runs the upstream tool", opts.StaticDetails)

	// Other tools keep the defaults.
	other, err := u.optionsFor("other", nil)
	require.NoError(t, err)
	require.Equal(t, defaults, other)
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
		"reserved tool": {Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: factory,
			ActivityOptionsOverrides: map[string]client.StartActivityOptions{"list_tools": {}}}}}, "non-empty and not reserved"},
		"negative timeout": {Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: factory,
			ActivityOptionsOverrides: map[string]client.StartActivityOptions{"tool": {HeartbeatTimeout: -time.Second}}}}}, "must not be negative"},
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

func TestConnectorOwnedActivityOptionsAreRejected(t *testing.T) {
	t.Parallel()

	for _, options := range []client.StartActivityOptions{
		{ID: "custom-id"},
		{TaskQueue: "other-queue"},
		{ActivityIDConflictPolicy: enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING},
		{ActivityIDReusePolicy: enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE},
	} {
		_, err := NewPlugin(Options{Upstreams: []Upstream{{
			Service:                  "tools",
			ClientFactory:            func(context.Context) (*mcp.ClientSession, error) { return nil, errors.New("unused") },
			ActivityOptionsOverrides: map[string]client.StartActivityOptions{"tool": options},
		}}})
		require.ErrorContains(t, err, "the proxy sets")
	}
}

func TestActivityID(t *testing.T) {
	t.Parallel()

	require.Equal(t, "mcp-outbound-endpoint-service-tool-request", activityID("endpoint", "service", "tool", "request"))
	long := activityID("endpoint", "service", "tool", strings.Repeat("x", 2000))
	require.Len(t, long, maxActivityIDBytes)
	require.Regexp(t, "^mcp-outbound-endpoint-service-tool-x+-[0-9a-f]{64}$", long)
	require.NotEqual(t, long, activityID("endpoint", "service", "tool", strings.Repeat("x", 2001)))
	multibyte := activityID("endpoint", "service", "tool", strings.Repeat("é", 600))
	require.True(t, utf8.ValidString(multibyte))
	require.LessOrEqual(t, len(multibyte), maxActivityIDBytes)
}

func TestCatalogReadsAllPagesAndKeepsMetadata(t *testing.T) {
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
	c := newCatalog(prepared(t, Upstream{ClientFactory: upstream.factory(t)}))
	tools, err := c.listTools(t.Context())
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

func TestCatalogFollowsUpstreamChangesAndKeepsListOnFailure(t *testing.T) {
	t.Parallel()

	names := []string{"a"}
	var fail atomic.Bool
	attempts := 0
	upstream := &fakeUpstream{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
		return listing(names...)(context.Background(), nil)
	}}
	c := newCatalog(prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		attempts++
		if fail.Load() {
			return nil, errors.New("offline")
		}
		return upstream.session(t), nil
	}}))

	first, err := c.listTools(t.Context())
	require.NoError(t, err)
	require.Equal(t, "a", first[0].Name)

	names = []string{"b", "c"}
	next, err := c.listTools(t.Context())
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, []string{next[0].Name, next[1].Name})

	// A failed discovery keeps the last list. The backoff skips the next
	// discovery.
	fail.Store(true)
	for range 2 {
		next, err = c.listTools(t.Context())
		require.NoError(t, err)
		require.Equal(t, "b", next[0].Name)
	}
	require.Equal(t, 3, attempts)
	tool, discovered := c.lookup("c")
	require.True(t, discovered)
	require.Equal(t, "c", tool.Name)
	tool, discovered = c.lookup("a")
	require.True(t, discovered)
	require.Nil(t, tool)
}

func TestCatalogRejectsBadListsAndKeepsPreviousList(t *testing.T) {
	t.Parallel()

	page := &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "weather.lookup"}, {Name: strings.Repeat("x", 100)}}}
	upstream := &fakeUpstream{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) { return page, nil }}
	c := newCatalog(prepared(t, Upstream{ClientFactory: upstream.factory(t)}))
	previous, err := c.listTools(t.Context())
	require.NoError(t, err)
	require.Equal(t, "weather.lookup", previous[0].Name)

	for _, bad := range [][]*mcp.Tool{
		{{Name: "list_tools"}},
		{{Name: "__temporal_internal"}},
		{{Name: "dup"}, {Name: "dup"}},
	} {
		page = &mcp.ListToolsResult{Tools: bad}
		c.mu.Lock()
		c.retryAt = time.Time{}
		c.mu.Unlock()
		require.Error(t, c.refresh(t.Context()))
		current, err := c.listTools(t.Context())
		require.NoError(t, err)
		require.Equal(t, previous, current)
	}
}

func TestCatalogWithoutDiscoveryIsUnavailable(t *testing.T) {
	t.Parallel()

	c := newCatalog(prepared(t, Upstream{}))
	_, err := c.listTools(t.Context())
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerErr.Type)
	tool, discovered := c.lookup("a")
	require.False(t, discovered)
	require.Nil(t, tool)
}

func TestCatalogRefreshWaitHonorsContext(t *testing.T) {
	t.Parallel()

	c := newCatalog(prepared(t, Upstream{}))
	c.refreshLock <- struct{}{}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, c.refresh(ctx), context.Canceled)
}

func TestCatalogBackoffAndRecovery(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		fail := true
		upstream := &fakeUpstream{list: listing("recovered")}
		c := newCatalog(prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
			attempts++
			if fail {
				return nil, errors.New("private-token")
			}
			return upstream.session(t), nil
		}}))
		for i := range 9 {
			require.Error(t, c.refresh(t.Context()))
			expected := min(time.Second*time.Duration(1<<i), time.Minute)
			require.Equal(t, expected, c.retryDelay)
			wait := time.Until(c.retryAt)
			require.GreaterOrEqual(t, wait, expected/2)
			require.LessOrEqual(t, wait, expected)
			// Calls during the backoff do not reach the upstream.
			for range 5 {
				require.NoError(t, c.refresh(t.Context()))
			}
			require.Equal(t, i+1, attempts)
			time.Sleep(wait)
		}
		fail = false
		require.NoError(t, c.refresh(t.Context()))
		require.Zero(t, c.retryDelay)
		require.True(t, c.retryAt.IsZero())
		tools, err := c.listTools(t.Context())
		require.NoError(t, err)
		require.Equal(t, "recovered", tools[0].Name)
	})
}

func runActivity(t *testing.T, u Upstream, call callInput) (map[string]json.RawMessage, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	fn := callActivity(u)
	env.RegisterActivity(fn)
	value, err := env.ExecuteActivity(fn, call)
	if err != nil {
		return nil, err
	}
	var envelope map[string]json.RawMessage
	require.NoError(t, value.Get(&envelope))
	return envelope, nil
}

func TestCallActivityReturnsToolResultInEnvelope(t *testing.T) {
	t.Parallel()

	var closes atomic.Int32
	upstream := &fakeUpstream{closes: &closes, call: func(_ context.Context, p *mcp.CallToolParams) (*mcp.CallToolResult, error) {
		if p.Name != "echo" {
			return nil, errors.New("unexpected tool")
		}
		return &mcp.CallToolResult{
			Content:           []mcp.Content{&mcp.TextContent{Text: "detail"}, &mcp.ImageContent{Data: []byte{1, 2}, MIMEType: "image/png"}},
			StructuredContent: map[string]any{"x": 1},
			IsError:           true,
		}, nil
	}}
	envelope, err := runActivity(t, prepared(t, Upstream{ClientFactory: upstream.factory(t)}),
		callInput{Tool: "echo", Arguments: json.RawMessage(`{"x":1}`)})
	require.NoError(t, err)
	var result mcp.CallToolResult
	require.NoError(t, json.Unmarshal(envelope[resultEnvelopeKey], &result))
	// A tool error is a result, not an Activity failure.
	require.True(t, result.IsError)
	require.Len(t, result.Content, 2)
	require.Eventually(t, func() bool { return closes.Load() == 1 }, time.Second, time.Millisecond)
}

func TestCallActivitySendsEmptyContentArray(t *testing.T) {
	t.Parallel()

	upstream := &fakeUpstream{call: func(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{}, nil
	}}
	result, err := callUpstream(t.Context(), prepared(t, Upstream{ClientFactory: upstream.factory(t)}), callInput{Tool: "empty"})
	require.NoError(t, err)
	var decoded struct {
		Content json.RawMessage `json:"content"`
	}
	require.NoError(t, json.Unmarshal(result[resultEnvelopeKey], &decoded))
	require.JSONEq(t, `[]`, string(decoded.Content))
}

func TestCallActivityRejectsIncompleteResults(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"input_required", "task"} {
		var incomplete mcp.CallToolResult
		require.NoError(t, json.Unmarshal(fmt.Appendf(nil, `{"content":[],"resultType":%q}`, kind), &incomplete))
		upstream := &fakeUpstream{call: func(context.Context, *mcp.CallToolParams) (*mcp.CallToolResult, error) { return &incomplete, nil }}
		_, err := callUpstream(t.Context(), prepared(t, Upstream{ClientFactory: upstream.factory(t)}), callInput{Tool: "incomplete"})
		require.ErrorIs(t, err, errProtocol, kind)
		require.Equal(t, failureProtocol, defaultClassification(err).Type)
	}
}

func TestCallActivityFailuresHoldOnlyTheType(t *testing.T) {
	t.Parallel()

	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return nil, fmt.Errorf("dial https://private.example.com?token=secret: %w", mcp.ErrConnectionClosed)
	}})
	_, err := runActivity(t, u, callInput{Tool: "echo"})
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, failureConnection, appErr.Type())
	require.False(t, appErr.NonRetryable())
	require.NotContains(t, err.Error(), "secret")

	// The hook can change the type and retryability.
	u.ClassifyError = func(_ error, def FailureClassification) FailureClassification {
		require.Equal(t, failureConnection, def.Type)
		return FailureClassification{Type: "CustomFailure"}
	}
	_, err = runActivity(t, u, callInput{Tool: "echo"})
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "CustomFailure", appErr.Type())
	require.True(t, appErr.NonRetryable())

	// A hook that panics gives a configuration failure without its value.
	u.ClassifyError = func(error, FailureClassification) FailureClassification { panic("private-hook-value") }
	_, err = runActivity(t, u, callInput{Tool: "echo"})
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, failureConfiguration, appErr.Type())
	require.True(t, appErr.NonRetryable())
	require.NotContains(t, err.Error(), "private")
}

func TestDefaultClassification(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		err       error
		failure   string
		retryable bool
	}{
		{os.ErrPermission, failureAuthentication, false},
		{&exec.Error{Name: "missing", Err: exec.ErrNotFound}, failureConfiguration, false},
		{&jsonrpc.Error{Code: jsonrpc.CodeInvalidParams}, failureProtocol, false},
		{&url.Error{Op: "Post", URL: "bad", Err: errors.New("unsupported protocol scheme")}, failureProtocol, false},
		{fmt.Errorf("call: %w", mcp.ErrConnectionClosed), failureConnection, true},
		{context.DeadlineExceeded, failureConnection, true},
		{errors.New("other"), failureOther, false},
	} {
		got := defaultClassification(test.err)
		require.Equal(t, FailureClassification{Type: test.failure, Retryable: test.retryable}, got, test.err.Error())
	}
}

// testRegistry records the registrations of the plugin.
type testRegistry struct {
	worker.Worker

	activities []string
	services   []*nexus.Service
}

func (r *testRegistry) RegisterActivityWithOptions(_ any, options activity.RegisterOptions) {
	r.activities = append(r.activities, options.Name)
}

func (r *testRegistry) RegisterNexusService(service *nexus.Service) {
	r.services = append(r.services, service)
}

func TestPluginRegistersHandlersAndStopsDiscovery(t *testing.T) {
	t.Parallel()

	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint("start fails=", fail), func(t *testing.T) {
			t.Parallel()

			entered, closed := make(chan struct{}), make(chan struct{})
			p, err := NewPlugin(Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: func(ctx context.Context) (*mcp.ClientSession, error) {
				close(entered)
				<-ctx.Done()
				close(closed)
				return nil, ctx.Err()
			}}}})
			require.NoError(t, err)
			proxy := p.(*plugin)
			registry := &testRegistry{}
			err = proxy.StartWorker(t.Context(), worker.PluginStartWorkerOptions{WorkerInstanceKey: "worker", WorkerRegistry: registry},
				func(context.Context, worker.PluginStartWorkerOptions) error {
					<-entered
					if fail {
						return errors.New("start failed")
					}
					return nil
				})
			if fail {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				stopped := false
				proxy.StopWorker(t.Context(), worker.PluginStopWorkerOptions{WorkerInstanceKey: "worker"},
					func(context.Context, worker.PluginStopWorkerOptions) { stopped = true })
				require.True(t, stopped)
			}
			select {
			case <-closed:
			default:
				t.Fatal("discovery did not stop")
			}
			require.Empty(t, proxy.stops)
			require.Equal(t, []string{"mcp-outbound.tools.call_upstream_tool"}, registry.activities)
			require.Len(t, registry.services, 1)
			require.NotNil(t, registry.services[0].Operation("list_tools"))
			// Every other name goes to the dynamic handler.
			require.NotNil(t, registry.services[0].Operation("any_tool"))
		})
	}
}

func TestDiscoveryChangesToolsWithoutReplacingHandlers(t *testing.T) {
	t.Parallel()

	names := []string{"a"}
	var gate bool
	entered, release := make(chan struct{}), make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	upstream := &fakeUpstream{list: func(ctx context.Context, _ *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
		if gate {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return listing(names...)(ctx, nil)
	}}
	u := prepared(t, Upstream{ClientFactory: upstream.factory(t)})
	c := newCatalog(u)
	service := newService(u, c, "call")
	require.NoError(t, c.refresh(t.Context()))

	names, gate = []string{"b"}, true
	done := make(chan error, 1)
	go func() { done <- c.refresh(t.Context()) }()
	<-entered
	// A running discovery does not change the published list.
	tool, _ := c.lookup("a")
	require.NotNil(t, tool)
	releaseOnce()
	require.NoError(t, <-done)
	tool, _ = c.lookup("b")
	require.NotNil(t, tool)
	tool, _ = c.lookup("a")
	require.Nil(t, tool)
	// The handlers stay the same: list_tools plus the dynamic handler.
	require.NotNil(t, service.Operation("list_tools"))
	require.Equal(t, "b", service.Operation("b").Name())
}
