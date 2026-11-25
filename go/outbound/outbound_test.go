package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
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
// go to f. The
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
	env.RegisterNexusService(newService(u, activityName(u.Service)))
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
	require.Equal(t, []string{"mcp-outbound.tools.call_upstream_tool"}, registry.activities)
	require.Len(t, registry.services, 1)
	require.Equal(t, "tools", registry.services[0].Name)
	require.NotNil(t, registry.services[0].Operation("list_tools"))
	// Every other name goes to the dynamic handler.
	require.NotNil(t, registry.services[0].Operation("any_tool"))
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

// toolCall is one run of the call Activity.
type toolCall struct {
	input callToolInput
	info  activity.Info
}

// runToolCall starts operation on the proxy service from a test Workflow. A
// recording Activity replaces the call Activity, so no upstream call runs.
func runToolCall(t *testing.T, u *Upstream, operation string, input any) ([]toolCall, json.RawMessage, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	name := activityName(u.Service)
	var calls []toolCall
	env.RegisterActivityWithOptions(func(ctx context.Context, call callToolInput) (json.RawMessage, error) {
		calls = append(calls, toolCall{input: call, info: activity.GetInfo(ctx)})
		return json.RawMessage(`{"done":true}`), nil
	}, activity.RegisterOptions{Name: name})
	env.RegisterNexusService(newService(u, name))
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

// requireHandlerError requires that err holds a Nexus handler error of type
// want.
func requireHandlerError(t *testing.T, err error, want nexus.HandlerErrorType) {
	t.Helper()
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, want, handlerErr.Type)
}

func TestNewStartUsesDefaultOptionsAndStableIdentity(t *testing.T) {
	t.Parallel()

	u := prepared(t, Upstream{Service: "weather", ClientFactory: (&fakeUpstream{}).factory(t)})
	calls, out, err := runToolCall(t, u, "a", map[string]any{"x": 1})
	require.NoError(t, err)
	require.JSONEq(t, `{"done":true}`, string(out))
	require.Len(t, calls, 1)
	require.Equal(t, "a", calls[0].input.Tool)
	require.JSONEq(t, `{"x":1}`, string(calls[0].input.Arguments))
	require.Regexp(t, "^mcp-outbound-weather-endpoint-weather-a-[0-9a-f-]+$", calls[0].info.ActivityID)
	require.Equal(t, defaultStartToCloseTimeout, calls[0].info.StartToCloseTimeout)
}

func TestToolCallDoesNotReadTheToolList(t *testing.T) {
	t.Parallel()

	var connects atomic.Int32
	u := prepared(t, Upstream{Service: "weather", ClientFactory: func(context.Context) (*mcp.ClientSession, error) {
		connects.Add(1)
		return nil, errors.New("not connected")
	}})
	// The upstream does not list this tool. The upstream call fails for it.
	calls, _, err := runToolCall(t, u, "missing", map[string]any{})
	require.NoError(t, err)
	require.Zero(t, connects.Load())
	require.Len(t, calls, 1)
	require.Equal(t, "missing", calls[0].input.Tool)
}

func TestNullArgumentsBecomeEmptyObject(t *testing.T) {
	t.Parallel()

	u := prepared(t, Upstream{Service: "weather", ClientFactory: (&fakeUpstream{}).factory(t)})
	calls, _, err := runToolCall(t, u, "b", nil)
	require.NoError(t, err)
	require.Len(t, calls, 1)
	require.JSONEq(t, `{}`, string(calls[0].input.Arguments))
}

func TestNonObjectArgumentsAreBadRequests(t *testing.T) {
	t.Parallel()

	for _, value := range []any{[]any{}, []int{1}, "text", 7, false} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			t.Parallel()

			u := prepared(t, Upstream{Service: "weather", ClientFactory: (&fakeUpstream{}).factory(t)})
			calls, _, err := runToolCall(t, u, "a", value)
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

			u := prepared(t, Upstream{Service: "weather", ClientFactory: (&fakeUpstream{}).factory(t)})
			calls, _, err := runToolCall(t, u, name, map[string]any{})
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
	require.Equal(t, failureUpstream, appErr.Type())
	require.True(t, appErr.NonRetryable())
	require.NotContains(t, err.Error(), "secret")
}
