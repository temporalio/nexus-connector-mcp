package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"os/exec"
	"slices"
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
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
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

func TestActivityOptionsDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	u := prepared(t, Upstream{})
	defaults := u.activityOptionsFor("tool", nil, "id")
	require.Equal(t, "id", defaults.ID)
	require.Equal(t, enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING, defaults.ActivityIDConflictPolicy)
	require.Equal(t, enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE, defaults.ActivityIDReusePolicy)
	require.Equal(t, time.Minute, defaults.StartToCloseTimeout)
	require.Zero(t, defaults.ScheduleToCloseTimeout)
	require.Zero(t, defaults.HeartbeatTimeout)
	require.Nil(t, defaults.RetryPolicy)
	require.Zero(t, defaults.Priority)
	require.Zero(t, defaults.TypedSearchAttributes.Size())

	// A destructive tool that is not idempotent gets one attempt.
	annotations := &mcp.ToolAnnotations{DestructiveHint: new(true)}
	opts := u.activityOptionsFor("tool", annotations, "id")
	require.EqualValues(t, 1, opts.RetryPolicy.MaximumAttempts)
	annotations.IdempotentHint = true
	opts = u.activityOptionsFor("tool", annotations, "id")
	require.Nil(t, opts.RetryPolicy)
	annotations.IdempotentHint, annotations.ReadOnlyHint = false, true
	opts = u.activityOptionsFor("tool", annotations, "id")
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
	opts = u.activityOptionsFor("tool", &mcp.ToolAnnotations{DestructiveHint: new(true)}, "id")
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
	other := u.activityOptionsFor("other", nil, "id")
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

func TestToolCacheReadsAllPagesAndKeepsMetadata(t *testing.T) {
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
	c := newToolCache(prepared(t, Upstream{ClientFactory: upstream.factory(t)}))
	tools, err := c.listTools(t.Context(), discardLogger)
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
	env.RegisterNexusService(newService(u, newToolCache(u), activityName(u.Service)))
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

func TestToolCacheFollowsUpstreamChangesAndKeepsListOnFailure(t *testing.T) {
	t.Parallel()

	names := []string{"a"}
	var fail atomic.Bool
	attempts := 0
	upstream := &fakeUpstream{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
		return listing(names...)(context.Background(), nil)
	}}
	c := newToolCache(prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		attempts++
		if fail.Load() {
			return nil, errors.New("offline")
		}
		return upstream.session(t), nil
	}}))

	first, err := c.listTools(t.Context(), discardLogger)
	require.NoError(t, err)
	require.Equal(t, "a", first[0].Name)

	names = []string{"b", "c"}
	next, err := c.listTools(t.Context(), discardLogger)
	require.NoError(t, err)
	require.Equal(t, []string{"b", "c"}, []string{next[0].Name, next[1].Name})

	// A failed discovery keeps the last list. The backoff skips the next
	// discovery.
	fail.Store(true)
	for range 2 {
		next, err = c.listTools(t.Context(), discardLogger)
		require.NoError(t, err)
		require.Equal(t, "b", next[0].Name)
	}
	require.Equal(t, 3, attempts)
	tool, err := c.findTool(t.Context(), discardLogger, "c")
	require.NoError(t, err)
	require.Equal(t, "c", tool.Name)
	_, err = c.findTool(t.Context(), discardLogger, "a")
	require.ErrorIs(t, err, errToolNotFound)
}

func TestToolCacheRejectsBadListsAndKeepsPreviousList(t *testing.T) {
	t.Parallel()

	page := &mcp.ListToolsResult{Tools: []*mcp.Tool{{Name: "weather.lookup"}, {Name: strings.Repeat("x", 100)}}}
	upstream := &fakeUpstream{list: func(context.Context, *mcp.ListToolsParams) (*mcp.ListToolsResult, error) { return page, nil }}
	c := newToolCache(prepared(t, Upstream{ClientFactory: upstream.factory(t)}))
	previous, err := c.listTools(t.Context(), discardLogger)
	require.NoError(t, err)
	require.Equal(t, "weather.lookup", previous[0].Name)

	for _, bad := range [][]*mcp.Tool{
		{{Name: "list_tools"}},
		{{Name: "__temporal_internal"}},
		{{Name: "dup"}, {Name: "dup"}},
	} {
		page = &mcp.ListToolsResult{Tools: bad}
		c.mu.Lock()
		c.nextDiscoveryAt = time.Time{}
		c.mu.Unlock()
		require.Error(t, c.refresh(t.Context()))
		current, err := c.listTools(t.Context(), discardLogger)
		require.NoError(t, err)
		require.Equal(t, previous, current)
	}
}

func TestToolCacheWithoutDiscoveryIsUnavailable(t *testing.T) {
	t.Parallel()

	c := newToolCache(prepared(t, Upstream{}))
	_, err := c.listTools(t.Context(), discardLogger)
	require.ErrorIs(t, err, errToolListUnavailable)
	_, err = c.findTool(t.Context(), discardLogger, "a")
	require.ErrorIs(t, err, errToolListUnavailable)
}

func TestToolCacheWithNoUpstreamToolsIsDiscovered(t *testing.T) {
	t.Parallel()

	upstream := &fakeUpstream{list: listing()}
	c := newToolCache(prepared(t, Upstream{ClientFactory: upstream.factory(t)}))
	tools, err := c.listTools(t.Context(), discardLogger)
	require.NoError(t, err)
	require.Empty(t, tools)
	_, err = c.findTool(t.Context(), discardLogger, "a")
	require.ErrorIs(t, err, errToolNotFound)
}

func TestToolCacheBackoffAndRecovery(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		fail := true
		upstream := &fakeUpstream{list: listing("recovered")}
		c := newToolCache(prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
			attempts++
			if fail {
				return nil, errors.New("private-token")
			}
			return upstream.session(t), nil
		}}))
		for i := range 9 {
			require.Error(t, c.refresh(t.Context()))
			expected := min(time.Second*time.Duration(1<<i), time.Minute)
			require.Equal(t, expected, c.backoff)
			wait := time.Until(c.nextDiscoveryAt)
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
		require.Zero(t, c.backoff)
		require.True(t, c.nextDiscoveryAt.IsZero())
		tools, err := c.listTools(t.Context(), discardLogger)
		require.NoError(t, err)
		require.Equal(t, "recovered", tools[0].Name)
	})
}

// toolCall is one run of the call Activity.
type toolCall struct {
	input callToolInput
	info  activity.Info
}

// runToolCall starts operation on the proxy service from a test Workflow. A
// recording Activity replaces the call Activity, so no upstream call runs.
func runToolCall(t *testing.T, u *Upstream, c *toolCache, operation string, input any) ([]toolCall, json.RawMessage, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	name := activityName(u.Service)
	var calls []toolCall
	env.RegisterActivityWithOptions(func(ctx context.Context, call callToolInput) (json.RawMessage, error) {
		calls = append(calls, toolCall{input: call, info: activity.GetInfo(ctx)})
		return json.RawMessage(`{"done":true}`), nil
	}, activity.RegisterOptions{Name: name})
	env.RegisterNexusService(newService(u, c, name))
	caller := func(ctx workflow.Context) (json.RawMessage, error) {
		var out json.RawMessage
		err := workflow.NewNexusClient("weather-endpoint", u.Service).
			ExecuteOperation(ctx, operation, input, workflow.NexusOperationOptions{ScheduleToCloseTimeout: time.Minute}).
			Get(ctx, &out)
		return out, err
	}
	env.RegisterWorkflow(caller)
	env.ExecuteWorkflow(caller)
	require.True(t, env.IsWorkflowCompleted())
	if err := env.GetWorkflowError(); err != nil {
		return calls, nil, err
	}
	var out json.RawMessage
	require.NoError(t, env.GetWorkflowResult(&out))
	return calls, out, nil
}

// weatherUpstream lists tools a and b, and counts the upstream connections.
type weatherUpstream struct {
	mu       sync.Mutex
	tools    []string
	connects atomic.Int32
}

func newWeatherUpstream(t *testing.T) (*weatherUpstream, *Upstream) {
	t.Helper()
	w := &weatherUpstream{tools: []string{"a", "b"}}
	fake := &fakeUpstream{list: func(ctx context.Context, p *mcp.ListToolsParams) (*mcp.ListToolsResult, error) {
		w.mu.Lock()
		names := slices.Clone(w.tools)
		w.mu.Unlock()
		return listing(names...)(ctx, p)
	}}
	u := prepared(t, Upstream{Service: "weather", ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		w.connects.Add(1)
		return fake.session(t), nil
	}})
	return w, u
}

func (w *weatherUpstream) add(name string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tools = append(w.tools, name)
}

// requireHandlerError requires that err holds a Nexus handler error of type
// want.
func requireHandlerError(t *testing.T, err error, want nexus.HandlerErrorType) {
	t.Helper()
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, want, handlerErr.Type)
}

func TestNewStartUsesOptionsAndStableIdentity(t *testing.T) {
	t.Parallel()

	_, weather := newWeatherUpstream(t)
	u := prepared(t, Upstream{Service: "weather", ClientFactory: weather.ClientFactory, ActivityOptionsOverrides: map[string]client.StartActivityOptions{
		"a": {ScheduleToCloseTimeout: time.Hour},
	}})
	c := newToolCache(u)
	require.NoError(t, c.refresh(t.Context()))
	calls, out, err := runToolCall(t, u, c, "a", map[string]any{"x": 1})
	require.NoError(t, err)
	require.JSONEq(t, `{"done":true}`, string(out))
	require.Len(t, calls, 1)
	require.Equal(t, "a", calls[0].input.Tool)
	require.JSONEq(t, `{"x":1}`, string(calls[0].input.Arguments))
	require.Regexp(t, "^mcp-outbound-weather-endpoint-weather-a-[0-9a-f-]+$", calls[0].info.ActivityID)
	require.Equal(t, defaultStartToCloseTimeout, calls[0].info.StartToCloseTimeout)
	// The test environment does not report the retry policy or priority of
	// the Activity. TestActivityOptionsDefaultsAndOverrides covers them.
	require.Equal(t, time.Hour, calls[0].info.ScheduleToCloseTimeout)
}

func TestUnknownToolAndMissingDiscovery(t *testing.T) {
	t.Parallel()

	u := prepared(t, Upstream{Service: "weather", ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return nil, errors.New("offline")
	}})
	calls, _, err := runToolCall(t, u, newToolCache(u), "a", map[string]any{})
	requireHandlerError(t, err, nexus.HandlerErrorTypeUnavailable)
	require.Empty(t, calls)

	// A tool call fills an empty cache. A tool that the upstream does not list
	// is not found.
	_, u = newWeatherUpstream(t)
	calls, _, err = runToolCall(t, u, newToolCache(u), "missing", map[string]any{})
	requireHandlerError(t, err, nexus.HandlerErrorTypeNotFound)
	require.Empty(t, calls)
}

func TestCacheHitMakesNoUpstreamCallAndMissRefreshesOnce(t *testing.T) {
	t.Parallel()

	w, u := newWeatherUpstream(t)
	c := newToolCache(u)
	require.NoError(t, c.refresh(t.Context()))
	_, _, err := runToolCall(t, u, c, "a", map[string]any{})
	require.NoError(t, err)
	require.Equal(t, int32(1), w.connects.Load())

	// A tool that is new upstream is a cache miss, so the call refreshes once.
	w.add("c")
	_, _, err = runToolCall(t, u, c, "c", map[string]any{})
	require.NoError(t, err)
	require.Equal(t, int32(2), w.connects.Load())
}

func TestToolCallFillsAnEmptyCache(t *testing.T) {
	t.Parallel()

	w, u := newWeatherUpstream(t)
	calls, _, err := runToolCall(t, u, newToolCache(u), "a", map[string]any{})
	require.NoError(t, err)
	require.Equal(t, int32(1), w.connects.Load())
	require.Len(t, calls, 1)
}

func TestNullArgumentsBecomeEmptyObject(t *testing.T) {
	t.Parallel()

	_, u := newWeatherUpstream(t)
	calls, _, err := runToolCall(t, u, newToolCache(u), "b", nil)
	require.NoError(t, err)
	require.Len(t, calls, 1)
	require.JSONEq(t, `{}`, string(calls[0].input.Arguments))
}

func TestNonObjectArgumentsAreBadRequests(t *testing.T) {
	t.Parallel()

	for _, value := range []any{[]any{}, []int{1}, "text", 7, false} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			t.Parallel()

			_, u := newWeatherUpstream(t)
			calls, _, err := runToolCall(t, u, newToolCache(u), "a", value)
			requireHandlerError(t, err, nexus.HandlerErrorTypeBadRequest)
			require.Empty(t, calls)
		})
	}
}

func TestInvalidToolNamesAreNotFound(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"", "__temporal_internal"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, u := newWeatherUpstream(t)
			calls, _, err := runToolCall(t, u, newToolCache(u), name, map[string]any{})
			requireHandlerError(t, err, nexus.HandlerErrorTypeNotFound)
			require.Empty(t, calls)
		})
	}
}

// closedHandle is an Activity handle whose Activity already closed.
type closedHandle struct {
	client.ActivityHandle

	result string
}

func (h closedHandle) Get(_ context.Context, valuePtr any) error {
	return json.Unmarshal([]byte(h.result), valuePtr)
}

// A retried start of an Activity that already closed fails with
// ActivityExecutionAlreadyStarted. The test environment does not return that
// error, so this test calls closedResult directly.
func TestRetriedStartOfClosedActivityReturnsItsResult(t *testing.T) {
	t.Parallel()

	result, err := closedResult(t.Context(), closedHandle{result: `"earlier result"`})
	require.NoError(t, err)
	require.Equal(t, temporalnexus.NewSyncResult[any](json.RawMessage(`"earlier result"`)), result)
}

func runActivity(t *testing.T, u *Upstream, call callToolInput) (map[string]json.RawMessage, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	fn := callToolActivity(u)
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
		callToolInput{Tool: "echo", Arguments: json.RawMessage(`{"x":1}`)})
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
	result, err := callTool(t.Context(), prepared(t, Upstream{ClientFactory: upstream.factory(t)}), "empty", nil)
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
		_, err := callTool(t.Context(), prepared(t, Upstream{ClientFactory: upstream.factory(t)}), "incomplete", nil)
		require.ErrorIs(t, err, errProtocol, kind)
		require.Equal(t, failureProtocol, defaultClassification(err).Type)
	}
}

func TestCallActivityFailuresHoldOnlyTheType(t *testing.T) {
	t.Parallel()

	u := prepared(t, Upstream{ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		return nil, fmt.Errorf("dial https://private.example.com?token=secret: %w", mcp.ErrConnectionClosed)
	}})
	_, err := runActivity(t, u, callToolInput{Tool: "echo"})
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
	_, err = runActivity(t, u, callToolInput{Tool: "echo"})
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "CustomFailure", appErr.Type())
	require.True(t, appErr.NonRetryable())

	// A hook that panics gives a configuration failure without its value.
	u.ClassifyError = func(error, FailureClassification) FailureClassification { panic("private-hook-value") }
	_, err = runActivity(t, u, callToolInput{Tool: "echo"})
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

func TestPluginRegistersHandlers(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	p, err := NewPlugin(Options{Upstreams: []Upstream{{Service: "tools", ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		calls.Add(1)
		return nil, errors.New("not connected")
	}}}})
	require.NoError(t, err)
	registry := &testRegistry{}
	started := false
	err = p.(*plugin).StartWorker(t.Context(), worker.PluginStartWorkerOptions{WorkerRegistry: registry},
		func(context.Context, worker.PluginStartWorkerOptions) error { started = true; return nil })
	require.NoError(t, err)
	require.True(t, started)
	// The Worker starts without discovery, so it starts while an upstream is
	// down.
	require.Zero(t, calls.Load())
	require.Equal(t, []string{"mcp-outbound.tools.call_upstream_tool"}, registry.activities)
	require.Len(t, registry.services, 1)
	require.NotNil(t, registry.services[0].Operation("list_tools"))
	// Every other name goes to the dynamic handler.
	require.NotNil(t, registry.services[0].Operation("any_tool"))
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
	c := newToolCache(u)
	service := newService(u, c, "call")
	require.NoError(t, c.refresh(t.Context()))

	names, gate = []string{"b"}, true
	done := make(chan error, 1)
	go func() { done <- c.refresh(t.Context()) }()
	<-entered
	// A running discovery does not change the published list.
	require.Contains(t, c.nameToTools, "a")
	releaseOnce()
	require.NoError(t, <-done)
	require.Contains(t, c.nameToTools, "b")
	require.NotContains(t, c.nameToTools, "a")
	// The handlers stay the same: list_tools plus the dynamic handler.
	require.NotNil(t, service.Operation("list_tools"))
	require.Equal(t, "b", service.Operation("b").Name())
}
