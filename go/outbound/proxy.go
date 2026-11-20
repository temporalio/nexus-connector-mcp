package outbound

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
)

const (
	pluginName = "nexus-connector-mcp.outbound"
	// resultEnvelopeKey marks an operation result that is already an MCP
	// tool result. The inbound connector and the Workflow client keep its
	// content, _meta, and isError.
	resultEnvelopeKey = "io.temporal/mcpCallToolResult"
	// maxHeartbeatInterval limits the heartbeat interval of a call Activity
	// that has a heartbeat timeout.
	maxHeartbeatInterval = 3 * time.Second
)

// Failure types of the built-in classification.
const (
	failureAuthentication = "UpstreamAuthenticationError"
	failureConfiguration  = "ProxyConfigurationError"
	failureProtocol       = "UpstreamProtocolError"
	failureConnection     = "UpstreamConnectionError"
	failureOther          = "UpstreamError"
)

// plugin registers one Nexus service and one call Activity per upstream.
type plugin struct {
	worker.PluginBase

	upstreams []Upstream
	catalogs  map[string]*catalog

	mu sync.Mutex
	// stops holds the function that stops the initial discovery of each
	// started Worker.
	stops map[string]func()
}

var _ worker.Plugin = (*plugin)(nil)

func (*plugin) Name() string { return pluginName }

// activityName returns the name of the call Activity of an upstream.
func activityName(service string) string {
	return "mcp-outbound." + service + ".call_upstream_tool"
}

// StartWorker registers the handlers and starts discovery in the background,
// so the Worker can start while an upstream is down.
func (p *plugin) StartWorker(ctx context.Context, opts worker.PluginStartWorkerOptions, next func(context.Context, worker.PluginStartWorkerOptions) error) error {
	for _, u := range p.upstreams {
		name := activityName(u.Service)
		opts.WorkerRegistry.RegisterActivityWithOptions(callActivity(u), activity.RegisterOptions{Name: name})
		opts.WorkerRegistry.RegisterNexusService(newService(u, p.catalogs[u.Service], name))
	}
	background, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, c := range p.catalogs {
		wg.Go(func() { c.refreshAndLog(background) })
	}
	stop := func() { cancel(); wg.Wait() }
	// The Worker does not call StopWorker after a failed start, so stop here.
	if err := next(ctx, opts); err != nil {
		stop()
		return err
	}
	p.mu.Lock()
	p.stops[opts.WorkerInstanceKey] = stop
	p.mu.Unlock()
	return nil
}

func (p *plugin) StopWorker(ctx context.Context, opts worker.PluginStopWorkerOptions, next func(context.Context, worker.PluginStopWorkerOptions)) {
	p.mu.Lock()
	stop := p.stops[opts.WorkerInstanceKey]
	delete(p.stops, opts.WorkerInstanceKey)
	p.mu.Unlock()
	if stop != nil {
		stop()
	}
	next(ctx, opts)
}

// newService returns the Nexus service of an upstream. Discovery changes the
// catalog, not the handlers.
func newService(u Upstream, c *catalog, activityName string) *nexus.Service {
	service := nexus.NewService(u.Service)
	service.MustRegister(nexus.NewSyncOperation(listToolsOperation,
		func(ctx context.Context, _ nexus.NoValue, _ nexus.StartOperationOptions) (*mcp.ListToolsResult, error) {
			tools, err := c.listTools(ctx)
			if err != nil {
				return nil, err
			}
			return &mcp.ListToolsResult{Tools: tools}, nil
		}))
	// Cancellation of a tool call uses the default handler, which cancels the
	// Activity by its operation token without a tool lookup.
	dynamic := temporalnexus.MustNewDynamicTemporalOperation(temporalnexus.DynamicTemporalOperationOptions{
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input *nexus.LazyValue, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
			var arguments json.RawMessage
			if err := input.Consume(&arguments); err != nil {
				return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "cannot read tool arguments")
			}
			return startTool(ctx, u, c, activityName, nc, arguments, opts)
		},
	})
	// A new service has no dynamic handler, so this cannot fail.
	if err := service.RegisterDynamic(dynamic); err != nil {
		panic(err)
	}
	return service
}

// callInput is the input of the call Activity. Temporal records it, so it
// holds no destination or credentials.
type callInput struct {
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}

// startTool starts the call Activity for the tool named by the Nexus
// operation.
//
// A retried start has the same Activity ID. It reattaches to the Activity
// before it reads the tool list, so it works after the tool is removed or
// while discovery fails. A running Activity keeps its recorded options. A
// closed Activity returns its outcome: Temporal answers a start with the same
// request ID with the first response, so a new start would return a token
// whose callback already ran.
func startTool(ctx context.Context, u Upstream, c *catalog, activityName string, nc temporalnexus.NexusClient, arguments json.RawMessage, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
	var none temporalnexus.TemporalOperationResult[any]
	name := nexus.ExtractHandlerInfo(ctx).Operation
	if !isToolName(name) {
		return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "invalid or reserved tool name")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &object); err != nil {
		return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "tool arguments must be a JSON object")
	}
	if object == nil {
		arguments = json.RawMessage(`{}`)
	}

	id := newActivityID(ctx, opts)
	handle := nc.GetWorkflowClient().GetActivityHandle(client.GetActivityHandleOptions{ActivityID: id})
	existing, err := handle.Describe(ctx, client.DescribeActivityOptions{})
	var notFound *serviceerror.NotFound
	var options client.StartActivityOptions
	switch {
	case err == nil && existing.Status != enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING:
		return closedResult(ctx, handle)
	case err == nil:
		options.StartToCloseTimeout = existing.StartToCloseTimeout
		options.ScheduleToCloseTimeout = existing.ScheduleToCloseTimeout
	case !errors.As(err, &notFound):
		return none, err
	default:
		tool, discovered := c.lookup(name)
		if !discovered {
			return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "upstream tool list is not available")
		}
		if tool == nil {
			return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "unknown upstream tool")
		}
		if options, err = u.optionsFor(name, tool.Annotations); err != nil {
			return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid Activity options: %s", err)
		}
	}
	options.ID = id
	options.ActivityIDConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
	options.ActivityIDReusePolicy = enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE
	result, err := temporalnexus.StartUntypedActivity[any](ctx, nc, options, activityName, callInput{Tool: name, Arguments: arguments})
	if _, ok := errors.AsType[*serviceerror.ActivityExecutionAlreadyStarted](err); ok {
		// The Activity closed between Describe and the start.
		return closedResult(ctx, handle)
	}
	return result, err
}

// closedResult returns the outcome of a closed Activity as a sync result.
func closedResult(ctx context.Context, handle client.ActivityHandle) (temporalnexus.TemporalOperationResult[any], error) {
	var none temporalnexus.TemporalOperationResult[any]
	var result json.RawMessage
	if err := handle.Get(ctx, &result); err != nil {
		if ctx.Err() != nil {
			return none, ctx.Err()
		}
		if temporal.IsCanceledError(err) {
			return none, nexus.NewOperationCanceledErrorf("upstream call cancelled")
		}
		// Keep the failure type. The message can hold upstream data, so it
		// stays out.
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.Type() != "" {
			return none, nexus.NewOperationFailedErrorf("upstream call failed: %s", appErr.Type())
		}
		return none, nexus.NewOperationFailedErrorf("upstream call failed")
	}
	return temporalnexus.NewSyncResult[any](result), nil
}

// callActivity returns the call Activity of an upstream. It calls the
// upstream tools/call and returns the tool result in the result envelope. A
// tool result with isError is a result, not a failure.
func callActivity(u Upstream) func(context.Context, callInput) (map[string]json.RawMessage, error) {
	return func(ctx context.Context, call callInput) (map[string]json.RawMessage, error) {
		stop := startHeartbeats(ctx)
		defer stop()
		result, err := callUpstream(ctx, u, call)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil {
			return result, nil
		}
		// Cancellation skips classification.
		if errors.Is(err, context.Canceled) || temporal.IsCanceledError(err) {
			return nil, err
		}
		classification := classify(u, err)
		// The failure holds only the type. Upstream messages, response
		// bodies, and URLs can hold private data, and Temporal records the
		// failure.
		return nil, temporal.NewApplicationErrorWithOptions(classification.Type, classification.Type,
			temporal.ApplicationErrorOptions{NonRetryable: !classification.Retryable})
	}
}

// startHeartbeats heartbeats while the call runs, if the Activity has a
// heartbeat timeout. Heartbeats let the Worker receive cancellation and
// detect a lost Worker before the attempt timeout. The returned function
// stops the heartbeats.
func startHeartbeats(ctx context.Context) func() {
	timeout := activity.GetInfo(ctx).HeartbeatTimeout
	if timeout <= 0 {
		return func() {}
	}
	interval := max(min(maxHeartbeatInterval, timeout/3), time.Millisecond)
	heartbeatCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	})
	return func() { cancel(); wg.Wait() }
}

// errProtocol reports an upstream result that the proxy cannot use.
var errProtocol = errors.New("invalid or incomplete upstream MCP result")

// callUpstream opens a session, calls the tool, and closes the session. The
// Activity context sets the call deadline. Connection setup has its own
// shorter deadline.
func callUpstream(ctx context.Context, u Upstream, call callInput) (map[string]json.RawMessage, error) {
	setupCtx, cancel := context.WithTimeout(ctx, setupTimeout)
	session, err := u.ClientFactory(setupCtx)
	cancel()
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, errProtocol
	}
	defer func() { _ = session.Close() }()
	arguments := call.Arguments
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: call.Tool, Arguments: arguments})
	if err != nil {
		return nil, err
	}
	// The proxy does not support upstream tasks or input requests yet, so it
	// rejects them. It does not treat them as a complete tool result.
	if result == nil || result.NeedsInput() {
		return nil, errProtocol
	}
	// The SDK decodes an empty content array as nil. Clients that require an
	// array reject null, so send an empty array.
	if result.Content == nil {
		complete := *result
		complete.Content = []mcp.Content{}
		result = &complete
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, errProtocol
	}
	var state struct {
		ResultType string `json:"resultType"`
	}
	if err := json.Unmarshal(encoded, &state); err != nil || (state.ResultType != "" && state.ResultType != "complete") {
		return nil, errProtocol
	}
	return map[string]json.RawMessage{resultEnvelopeKey: encoded}, nil
}

// defaultClassification returns the built-in classification of a failed
// upstream call. It uses only error types, not messages.
func defaultClassification(err error) FailureClassification {
	var rpcErr *jsonrpc.Error
	var netErr net.Error
	var urlErr *url.Error
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	var execErr *exec.Error
	switch {
	case errors.Is(err, os.ErrPermission):
		return FailureClassification{Type: failureAuthentication}
	case errors.Is(err, os.ErrNotExist), errors.As(err, &execErr):
		return FailureClassification{Type: failureConfiguration}
	case errors.As(err, &rpcErr), errors.As(err, &syntaxErr), errors.As(err, &typeErr), errors.Is(err, errProtocol):
		return FailureClassification{Type: failureProtocol}
	case errors.As(err, &urlErr) && !errors.As(urlErr.Err, &netErr):
		return FailureClassification{Type: failureProtocol}
	case errors.As(err, &netErr), errors.Is(err, mcp.ErrConnectionClosed), errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, context.DeadlineExceeded):
		return FailureClassification{Type: failureConnection, Retryable: true}
	default:
		return FailureClassification{Type: failureOther}
	}
}

// classify applies Upstream.ClassifyError to the built-in classification. A
// hook that panics or returns an empty type gives a non-retryable
// configuration failure.
func classify(u Upstream, err error) (result FailureClassification) {
	result = defaultClassification(err)
	if u.ClassifyError == nil {
		return result
	}
	defer func() {
		if recover() != nil {
			result = FailureClassification{Type: failureConfiguration}
		}
	}()
	result = u.ClassifyError(err, result)
	if result.Type == "" {
		result = FailureClassification{Type: failureConfiguration}
	}
	return result
}
