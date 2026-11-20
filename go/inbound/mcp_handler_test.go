package inbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// fakeClient runs Nexus operations in memory.
//   - list_tools returns the tool list of the service. With blockToolList,
//     it runs until the read context ends.
//   - "slow" runs until the read context ends.
//   - "fails" fails with an application error.
//   - "bad_start" cannot start.
//   - Operations in results return their entry.
//   - Other operations fail, as an unknown Nexus operation does.
type fakeClient struct {
	client.Client

	mu            sync.Mutex
	toolList      string
	blockToolList bool
	results       map[string]string
	readError     error
	starts        []client.StartNexusOperationOptions
	inputs        []any
	cancelled     []string

	nexusClients []client.NexusClientOptions
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		toolList: tools("lookup", "slow", "fails"),
		results:  map[string]string{"lookup": `{"answer":42}`},
	}
}

func tools(names ...string) string {
	defs := make([]string, 0, len(names))
	for _, name := range names {
		defs = append(defs, fmt.Sprintf(`{"name":%q,"inputSchema":{"type":"object"},"outputSchema":{"type":"object"}}`, name))
	}
	return `{"tools":[` + strings.Join(defs, ",") + `]}`
}

func (c *fakeClient) NewNexusClient(options client.NexusClientOptions) (client.NexusClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nexusClients = append(c.nexusClients, options)
	return &fakeNexusClient{parent: c}, nil
}

// startedOperations returns the operation names in start order.
func (c *fakeClient) startedOperations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.starts))
	for _, start := range c.starts {
		names = append(names, start.Summary)
	}
	return names
}

type fakeNexusClient struct {
	client.NexusClient

	parent *fakeClient
}

func (n *fakeNexusClient) ExecuteOperation(_ context.Context, operation, input any, options client.StartNexusOperationOptions) (client.NexusOperationHandle, error) {
	c := n.parent
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starts = append(c.starts, options)
	c.inputs = append(c.inputs, input)
	if operation == "bad_start" {
		return nil, errors.New("start rejected")
	}
	name, _ := operation.(string)
	return &fakeHandle{parent: c, id: options.ID, operation: name}, nil
}

type fakeHandle struct {
	client.NexusOperationHandle

	parent    *fakeClient
	id        string
	operation string
}

func (h *fakeHandle) GetID() string { return h.id }

func (h *fakeHandle) Get(ctx context.Context, out any) error {
	c := h.parent
	c.mu.Lock()
	readError, toolList, blockToolList := c.readError, c.toolList, c.blockToolList
	result, known := c.results[h.operation]
	c.mu.Unlock()
	switch {
	case readError != nil:
		return readError
	case h.operation == "list_tools" && blockToolList:
		<-ctx.Done()
		return ctx.Err()
	case h.operation == "list_tools":
		return json.Unmarshal([]byte(toolList), out)
	case h.operation == "slow":
		<-ctx.Done()
		return ctx.Err()
	case h.operation == "fails":
		return temporal.NewApplicationError("operation failed", "ToolError")
	case !known:
		return temporal.NewApplicationError("operation not found", "NotFound")
	}
	return json.Unmarshal([]byte(result), out)
}

func (h *fakeHandle) Cancel(ctx context.Context, _ client.CancelNexusOperationOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h.parent.mu.Lock()
	defer h.parent.mu.Unlock()
	h.parent.cancelled = append(h.parent.cancelled, h.id)
	return nil
}

func newConnector(c *fakeClient, opts Options) *connector {
	if opts.Service == "" {
		opts.Endpoint, opts.Service = "ep-a", "svc-a"
	}
	opts.Logger = slog.New(slog.DiscardHandler)
	if opts.CallerIdentity == nil {
		opts.CallerIdentity = tokenInfoIdentity
	}
	opts.Client = c
	nexus, err := newNexusClient(c, opts.Endpoint, opts.Service)
	if err != nil {
		panic(err)
	}
	return &connector{opts: &opts, nexus: nexus}
}

const testPrefix = "mcp-inbound-stdio"

func call(ctx context.Context, c *connector, name, args string) (mcp.Result, error) {
	params := &mcp.CallToolParamsRaw{Name: name}
	if args != "" {
		params.Arguments = json.RawMessage(args)
	}
	return c.callTool(ctx, testPrefix, &mcp.CallToolRequest{Params: params})
}

func listedNames(t *testing.T, c *connector) []string {
	t.Helper()
	result, err := c.listTools(t.Context(), testPrefix)
	require.NoError(t, err)
	names := make([]string, 0, len(result.Tools))
	for _, tool := range result.Tools {
		names = append(names, tool.Name)
	}
	return names
}

func TestCallToolStartsOperationAndKeepsLargeIntegers(t *testing.T) {
	t.Parallel()

	fake := newFakeClient()
	fake.results["lookup"] = `{"id":9007199254740993}`
	c := newConnector(fake, Options{})
	result, err := call(t.Context(), c, "lookup", `{"id":9007199254740993}`)
	require.NoError(t, err)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"id":9007199254740993`)

	// tools/call starts the operation with the tool name. It does not call
	// list_tools first.
	require.Equal(t, []string{"lookup"}, fake.startedOperations())
	require.Equal(t, []client.NexusClientOptions{{Endpoint: "ep-a", Service: "svc-a"}}, fake.nexusClients)
	start := fake.starts[0]
	require.Regexp(t, `^mcp-inbound-stdio-svc-a-ep-a-lookup-[0-9a-f]{32}$`, start.ID)
	require.Equal(t, enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE, start.IDReusePolicy)
	require.Zero(t, start.ScheduleToCloseTimeout)
	require.Equal(t, map[string]any{"id": json.Number("9007199254740993")}, fake.inputs[0])
}

func TestCallToolWithoutArgumentsSendsEmptyObject(t *testing.T) {
	t.Parallel()

	fake := newFakeClient()
	c := newConnector(fake, Options{})
	_, err := call(t.Context(), c, "lookup", "")
	require.NoError(t, err)
	require.Equal(t, map[string]any{}, fake.inputs[0])
}

func TestCallToolRejectsNonObjectArguments(t *testing.T) {
	t.Parallel()

	for _, args := range []string{`[1]`, `"text"`, `null`, `{} {}`} {
		fake := newFakeClient()
		_, err := call(t.Context(), newConnector(fake, Options{}), "lookup", args)
		var rpcErr *jsonrpc.Error
		require.ErrorAs(t, err, &rpcErr, args)
		require.EqualValues(t, jsonrpc.CodeInvalidParams, rpcErr.Code)
		require.Empty(t, fake.startedOperations())
	}
}

func TestCallToolRejectsListToolsAndEmptyName(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"", "list_tools"} {
		fake := newFakeClient()
		_, err := call(t.Context(), newConnector(fake, Options{}), name, "")
		var rpcErr *jsonrpc.Error
		require.ErrorAs(t, err, &rpcErr, name)
		require.EqualValues(t, jsonrpc.CodeInvalidParams, rpcErr.Code)
		require.Empty(t, fake.startedOperations())
	}
}

func TestFailuresBecomeToolErrors(t *testing.T) {
	t.Parallel()

	c := newConnector(newFakeClient(), Options{})
	// "missing" is not an operation of the service, so the operation fails.
	for _, name := range []string{"fails", "bad_start", "missing"} {
		result, err := call(t.Context(), c, name, "")
		require.NoError(t, err)
		require.True(t, result.(*mcp.CallToolResult).IsError, name)
	}
}

func TestReadErrorIsJSONRPCError(t *testing.T) {
	t.Parallel()

	fake := newFakeClient()
	fake.readError = serviceerror.NewUnavailable("read unavailable")
	_, err := call(t.Context(), newConnector(fake, Options{}), "lookup", "")
	var rpcErr *jsonrpc.Error
	require.ErrorAs(t, err, &rpcErr)
	require.EqualValues(t, jsonrpc.CodeInternalError, rpcErr.Code)
}

func TestCancelledCallCancelsOperation(t *testing.T) {
	t.Parallel()

	fake := newFakeClient()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := call(ctx, newConnector(fake, Options{}), "slow", "")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, fake.cancelled, 1)
	require.Equal(t, fake.starts[0].ID, fake.cancelled[0])
}

func TestListToolsKeepsOutputSchemaAndDisablesCaching(t *testing.T) {
	t.Parallel()

	fake := newFakeClient()
	c := newConnector(fake, Options{})
	require.Equal(t, []string{"lookup", "slow", "fails"}, listedNames(t, c))
	result, err := c.listTools(t.Context(), testPrefix)
	require.NoError(t, err)
	require.NotNil(t, result.Tools[0].OutputSchema)
	require.Zero(t, result.TTLMs)
	require.Equal(t, "private", result.CacheScope)
	// The connector creates one Nexus client and uses it for every call.
	require.Equal(t, []client.NexusClientOptions{{Endpoint: "ep-a", Service: "svc-a"}}, fake.nexusClients)
	require.Equal(t, listToolsTimeout, fake.starts[0].ScheduleToCloseTimeout)
}

func TestListToolsRejectsInvalidToolNames(t *testing.T) {
	t.Parallel()

	for _, test := range []struct{ toolList, message string }{
		{`{"tools":[{"name":""}]}`, "without a name"},
		{`{"tools":[null]}`, "without a name"},
		{tools("dup", "dup"), "more than once"},
	} {
		fake := newFakeClient()
		fake.toolList = test.toolList
		_, err := newConnector(fake, Options{}).listTools(t.Context(), testPrefix)
		var rpcErr *jsonrpc.Error
		require.ErrorAs(t, err, &rpcErr, test.toolList)
		require.EqualValues(t, jsonrpc.CodeInternalError, rpcErr.Code)
		require.Contains(t, rpcErr.Message, test.message)
	}
}

func TestListToolsErrors(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name, message string
		change        func(*fakeClient)
	}{
		{"timeout", "check that the handler Worker runs", func(f *fakeClient) { f.blockToolList = true }},
		{"failure", "list tools failed", func(f *fakeClient) { f.readError = temporal.NewApplicationError("list tools failed", "Error") }},
		{"read error", "read unavailable", func(f *fakeClient) { f.readError = serviceerror.NewUnavailable("read unavailable") }},
	} {
		fake := newFakeClient()
		test.change(fake)
		// The caller deadline ends the blocked read before listToolsTimeout.
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		_, err := newConnector(fake, Options{}).listTools(ctx, testPrefix)
		cancel()
		var rpcErr *jsonrpc.Error
		require.ErrorAs(t, err, &rpcErr, test.name)
		require.EqualValues(t, jsonrpc.CodeInternalError, rpcErr.Code, test.name)
		require.Contains(t, rpcErr.Message, `list tools of service "svc-a"`, test.name)
		require.Contains(t, rpcErr.Message, test.message, test.name)
	}
}

func TestEmptyToolListEncodesEmptyArray(t *testing.T) {
	t.Parallel()

	for _, toolList := range []string{`{"tools":[]}`, `{}`} {
		fake := newFakeClient()
		fake.toolList = toolList
		result, err := newConnector(fake, Options{}).listTools(t.Context(), testPrefix)
		require.NoError(t, err)
		encoded, err := json.Marshal(result)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"tools":[]`)
	}
}

func TestCallerIdentityAddsOwnerHash(t *testing.T) {
	t.Parallel()

	require.Equal(t, "430ea27824a993fe1754730c8434fbd26a9cde578fdcb0e68da3cdb85bd272d4", ownerHash("issuer#alice"))
	_, ok := tokenInfoIdentity(t.Context())
	require.False(t, ok)

	ctx := context.WithValue(t.Context(), requestExtraKey{}, &mcp.RequestExtra{TokenInfo: &auth.TokenInfo{UserID: "issuer#alice"}})
	identity, ok := tokenInfoIdentity(ctx)
	require.True(t, ok)
	require.Equal(t, "issuer#alice", identity)

	fake := newFakeClient()
	_, err := call(ctx, newConnector(fake, Options{}), "lookup", "")
	require.NoError(t, err)
	require.Regexp(t, `-430ea27824a993fe1754730c8434fbd26a9cde578fdcb0e68da3cdb85bd272d4-[0-9a-f]{32}$`, fake.starts[0].ID)

	custom := newConnector(newFakeClient(), Options{CallerIdentity: func(context.Context) (string, bool) { return "issuer#bob", true }})
	require.Equal(t, ownerHash("issuer#bob"), custom.owner(t.Context()))
}

// TestServerOverMCPClient runs tools/list and tools/call through the MCP SDK
// client and server over an in-memory transport.
func TestServerOverMCPClient(t *testing.T) {
	t.Parallel()

	fake := newFakeClient()
	server, err := NewServer(Options{
		Client:        fake,
		Namespace:     "default",
		Endpoint:      "ep-a",
		Service:       "svc-a",
		ConnectorName: "test-connector",
		Logger:        slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)

	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(t.Context(), serverTransport, nil)
	require.NoError(t, err)
	defer func() { _ = serverSession.Close() }()
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.0"}, nil)
	session, err := mcpClient.Connect(t.Context(), clientTransport, nil)
	require.NoError(t, err)
	defer func() { _ = session.Close() }()

	require.Equal(t, "test-connector", session.InitializeResult().ServerInfo.Name)
	require.NotNil(t, session.InitializeResult().Capabilities.Tools)

	listed, err := session.ListTools(t.Context(), nil)
	require.NoError(t, err)
	names := make([]string, 0, len(listed.Tools))
	for _, tool := range listed.Tools {
		names = append(names, tool.Name)
	}
	require.Equal(t, []string{"lookup", "slow", "fails"}, names)

	result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "lookup", Arguments: map[string]any{"q": "x"}})
	require.NoError(t, err)
	require.False(t, result.IsError)
	require.Equal(t, map[string]any{"answer": float64(42)}, result.StructuredContent)

	failed, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "fails"})
	require.NoError(t, err)
	require.True(t, failed.IsError)

	missing, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "missing"})
	require.NoError(t, err)
	require.True(t, missing.IsError)

	// The in-memory transport has no HTTP header, so the transport is stdio.
	require.True(t, strings.HasPrefix(fake.starts[0].ID, "mcp-inbound-stdio-"))
}
