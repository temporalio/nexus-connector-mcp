package inbound

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"github.com/temporalio/nexus-connector-mcp/go/inbound/internal/mcpwire"
)

// tasksMeta declares the tasks extension, as a client on MCP 2026-07-28 does
// in the _meta of each request.
func tasksMeta() mcp.Meta {
	return mcp.Meta{
		mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{mcpwire.TasksExtension: map[string]any{}}},
	}
}

func callWithMeta(ctx context.Context, c *connector, name string, meta mcp.Meta) (mcp.Result, error) {
	return c.callTool(ctx, testPrefix, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{Name: name, Meta: meta}})
}

func TestTaskHandoff(t *testing.T) {
	t.Parallel()

	t.Run("zero returns a task right after the start", func(t *testing.T) {
		t.Parallel()

		fake := newFakeClient()
		result, err := callWithMeta(t.Context(), newConnector(fake, Options{}), "lookup", tasksMeta())
		require.NoError(t, err)
		task := result.(*mcpwire.CreateTaskResult)
		require.Equal(t, "task", task.ResultType)
		require.Equal(t, "working", task.Status)
		require.Equal(t, fake.starts[0].ID, task.TaskID)
	})

	t.Run("threshold returns the result when it is ready", func(t *testing.T) {
		t.Parallel()

		result, err := callWithMeta(t.Context(), newConnector(newFakeClient(), Options{ReturnTaskAfter: time.Second}), "lookup", tasksMeta())
		require.NoError(t, err)
		require.False(t, result.(*mcp.CallToolResult).IsError)
	})

	t.Run("threshold returns a task and keeps the operation", func(t *testing.T) {
		t.Parallel()

		fake := newFakeClient()
		result, err := callWithMeta(t.Context(), newConnector(fake, Options{ReturnTaskAfter: 5 * time.Millisecond}), "slow", tasksMeta())
		require.NoError(t, err)
		task := result.(*mcpwire.CreateTaskResult)
		require.Regexp(t, `^mcp-inbound-stdio-svc-a-ep-a-slow-[0-9a-f]{32}$`, task.TaskID)
		require.EqualValues(t, 2000, task.PollIntervalMs)
		require.Empty(t, fake.cancelled)
	})

	for name, test := range map[string]struct {
		opts Options
		meta mcp.Meta
	}{
		"NeverReturnTask waits":      {Options{ReturnTaskAfter: NeverReturnTask}, tasksMeta()},
		"client without tasks waits": {Options{ReturnTaskAfter: time.Millisecond}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fake := newFakeClient()
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			defer cancel()
			_, err := callWithMeta(ctx, newConnector(fake, test.opts), "slow", test.meta)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Len(t, fake.cancelled, 1)
		})
	}
}

func TestGetTaskMapsOperationStates(t *testing.T) {
	t.Parallel()

	fake := newFakeClient()
	c := newConnector(fake, Options{})
	ids := map[string]string{}
	for _, name := range []string{"slow", "lookup", "fails"} {
		result, err := callWithMeta(t.Context(), c, name, tasksMeta())
		require.NoError(t, err)
		ids[name] = result.(*mcpwire.CreateTaskResult).TaskID
	}

	running, err := c.getTask(t.Context(), ids["slow"])
	require.NoError(t, err)
	require.Equal(t, "working", running.Status)
	require.Nil(t, running.Result)

	done, err := c.getTask(t.Context(), ids["lookup"])
	require.NoError(t, err)
	require.Equal(t, "completed", done.Status)
	require.False(t, done.Result.IsError)
	require.Equal(t, time.Unix(200, 0).UTC().Format(time.RFC3339Nano), done.LastUpdatedAt)

	// A handler failure is a tool error, so the task completes.
	failed, err := c.getTask(t.Context(), ids["fails"])
	require.NoError(t, err)
	require.Equal(t, "completed", failed.Status)
	require.True(t, failed.Result.IsError)

	fake.operations[ids["slow"]].status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED
	cancelled, err := c.getTask(t.Context(), ids["slow"])
	require.NoError(t, err)
	require.Equal(t, "cancelled", cancelled.Status)

	fake.operations[ids["slow"]].status = enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TIMED_OUT
	timedOut, err := c.getTask(t.Context(), ids["slow"])
	require.NoError(t, err)
	require.Equal(t, "failed", timedOut.Status)
	require.NotNil(t, timedOut.Error)

	// A read failure is an error, not a tool result.
	fake.readError = serviceerror.NewUnavailable("read unavailable")
	_, err = c.getTask(t.Context(), ids["lookup"])
	require.Error(t, err)
}

func TestTaskMethodsOverMCPClient(t *testing.T) {
	t.Parallel()

	fake := newFakeClient()
	alice := "https://idp.example.com#alice"
	identity := alice
	server, err := NewServer(Options{
		Client:   fake,
		Endpoint: "ep-a",
		Service:  "svc-a",
		Logger:   slog.New(slog.DiscardHandler),
		CallerIdentity: func(ctx context.Context) (string, bool) {
			return identity, true
		},
	})
	require.NoError(t, err)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	defer func() { _ = serverSession.Close() }()
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	// TODO(long-nt-tran): use the SDK client task API when the SDK supports
	// the tasks extension. SDK v1.8.0 has none, so the client sends custom
	// methods.
	for _, method := range []string{"tasks/get", "tasks/cancel"} {
		require.NoError(t, mcp.AddSendingCustomMethod[*mcpwire.TaskIDParams, *mcpwire.GetTaskResult](mcpClient, method))
	}
	require.NoError(t, mcp.AddSendingCustomMethod[*mcpwire.TaskIDParams, *mcpwire.AckResult](mcpClient, "tasks/update"))
	session, err := mcpClient.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	defer func() { _ = session.Close() }()

	require.Contains(t, session.InitializeResult().Capabilities.Extensions, mcpwire.TasksExtension)

	// Start two operations on the connector, as tools/call does.
	c := newConnector(fake, Options{CallerIdentity: func(context.Context) (string, bool) { return alice, true }})
	ids := map[string]string{}
	for _, name := range []string{"lookup", "slow"} {
		result, err := callWithMeta(t.Context(), c, name, tasksMeta())
		require.NoError(t, err)
		ids[name] = result.(*mcpwire.CreateTaskResult).TaskID
	}
	params := func(id string) *mcpwire.TaskIDParams {
		p := &mcpwire.TaskIDParams{TaskID: id}
		p.Meta = tasksMeta()
		return p
	}
	rpcCode := func(err error) int64 {
		var rpcErr *jsonrpc.Error
		require.ErrorAs(t, err, &rpcErr)
		return rpcErr.Code
	}

	got, err := mcp.CallCustomMethod[*mcpwire.TaskIDParams, *mcpwire.GetTaskResult](t.Context(), session, "tasks/get", params(ids["lookup"]))
	require.NoError(t, err)
	require.Equal(t, "completed", got.Status)
	require.Equal(t, map[string]any{"answer": float64(42)}, got.Result.StructuredContent)

	_, err = mcp.CallCustomMethod[*mcpwire.TaskIDParams, *mcpwire.GetTaskResult](t.Context(), session, "tasks/cancel", params(ids["slow"]))
	require.NoError(t, err)
	require.Equal(t, []string{ids["slow"]}, fake.cancelled)

	_, err = mcp.CallCustomMethod[*mcpwire.TaskIDParams, *mcpwire.AckResult](t.Context(), session, "tasks/update", params(ids["slow"]))
	require.EqualValues(t, jsonrpc.CodeInvalidParams, rpcCode(err))

	// A request without the tasks extension gets -32021. The client fills
	// _meta with its own capabilities, which do not declare the extension.
	_, err = mcp.CallCustomMethod[*mcpwire.TaskIDParams, *mcpwire.GetTaskResult](t.Context(), session, "tasks/get", &mcpwire.TaskIDParams{TaskID: ids["lookup"]})
	require.EqualValues(t, mcp.CodeMissingRequiredClientCapabilities, rpcCode(err))

	// Another caller cannot read or cancel the task.
	identity = "https://idp.example.com#bob"
	for _, method := range []string{"tasks/get", "tasks/cancel"} {
		_, err = mcp.CallCustomMethod[*mcpwire.TaskIDParams, *mcpwire.GetTaskResult](t.Context(), session, method, params(ids["slow"]))
		require.EqualValues(t, jsonrpc.CodeInvalidParams, rpcCode(err), method)
	}
	require.Len(t, fake.cancelled, 1)

	// An operation that does not exist has the same error as a foreign task.
	identity = alice
	_, err = mcp.CallCustomMethod[*mcpwire.TaskIDParams, *mcpwire.GetTaskResult](t.Context(), session, "tasks/get",
		params(newOperationID("mcp-inbound-stdio-svc-a-ep-a-lookup", ownerHash(alice))))
	require.EqualValues(t, jsonrpc.CodeInvalidParams, rpcCode(err))
}

func TestCallerIdentityFromTokenInfoReachesTaskMethods(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(t.Context(), requestExtraKey{}, &mcp.RequestExtra{TokenInfo: &auth.TokenInfo{UserID: "issuer#alice"}})
	c := newConnector(newFakeClient(), Options{})
	require.Equal(t, ownerHash("issuer#alice"), c.owner(ctx))
}
