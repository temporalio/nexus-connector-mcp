package outbound

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
)

const (
	// defaultStartToCloseTimeout is the proxy default for each Activity
	// attempt.
	defaultStartToCloseTimeout = time.Minute
	// activityIDPrefix starts every Activity ID that the proxy generates.
	activityIDPrefix = "mcp-outbound-"
	// maxActivityIDBytes is the default Temporal limit for IDs.
	maxActivityIDBytes = 1000
	// maxHeartbeatInterval limits the heartbeat interval of a call Activity
	// that has a heartbeat timeout.
	maxHeartbeatInterval = 3 * time.Second
	// Failure types of the built-in classification.
	failureAuthentication = "UpstreamAuthenticationError"
	failureConfiguration  = "ProxyConfigurationError"
	failureProtocol       = "UpstreamProtocolError"
	failureConnection     = "UpstreamConnectionError"
	failureOther          = "UpstreamError"
)

// listToolsResult is the output of list_tools: an MCP tools/list result with
// only the tools. mcp.ListToolsResult also encodes its cache fields, with an
// empty cacheScope that other MCP SDKs reject.
type listToolsResult struct {
	Tools []*mcp.Tool `json:"tools"`
}

// newService returns the Nexus service of an upstream. Discovery changes the
// tool cache, not the handlers.
//
// TODO(long-nt-tran): investigate smart type inference here based on in/out
// schemas + past tool calls. This would allow us to provide smartly typed
// APIs given an upstream MCP spec.
func newService(u *Upstream, c *toolCache, activityName string) *nexus.Service {
	service := nexus.NewService(u.Service)
	service.MustRegister(nexus.NewSyncOperation(listToolsOperation,
		func(ctx context.Context, _ nexus.NoValue, _ nexus.StartOperationOptions) (*listToolsResult, error) {
			// The Worker logger comes from the client options of the user.
			tools, err := c.listTools(ctx, temporalnexus.GetLogger(ctx))
			if err != nil {
				return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "%v", err)
			}
			return &listToolsResult{Tools: tools}, nil
		}))

	// Calling an upstream tool is handled by a dynamic handler, since we don't know
	// the names of the tools provided by an upstream server. This dynamic handler allows
	// the client to call a tool as a named Nexus operation.
	//
	// The dynamic handler will then handle all of these tool calls and delegate to startTool(...)
	// which is an SAA-backed call.
	//
	// The reason we want client to be able to use a first-class Nexus operation for tool call is
	// for future Nexus operation features to apply to those tool calls (vs. having a single 'callTool'
	// Nexus operation that client invokes, in which case they lose operation-level granularity provided
	// by Nexus).
	dynamic := temporalnexus.MustNewDynamicTemporalOperation(temporalnexus.DynamicTemporalOperationOptions{
		Start: func(ctx context.Context, nc temporalnexus.NexusClient, input converter.RawValue, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
			// A dynamic handler gets the raw payload after the payload codecs.
			// The client does not expose its data converter, so the default
			// data converter decodes it.
			var arguments json.RawMessage
			if err := converter.GetDefaultDataConverter().FromPayload(input.Payload(), &arguments); err != nil {
				return temporalnexus.TemporalOperationResult[any]{}, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "cannot read tool arguments")
			}
			// This is an async SAA-backed call, where the activity is the HTTP call upstream.
			return startTool(ctx, u, c, activityName, nc, arguments, opts)
		},
	})

	// This should never fail, since we compose the service in this function.
	// There should not be a dynamic handler yet anywhere.
	if err := service.RegisterDynamic(dynamic); err != nil {
		panic(err)
	}
	return service
}

// startTool uses temporalnexus to async-start an SAA for the tool named by the Nexus
// operation.
//
// A retried start uses the same Activity ID. USE_EXISTING attaches it to a
// running Activity. If the Activity already closed, the start fails with
// ActivityExecutionAlreadyStarted, and startTool returns the Activity outcome.
//
// The tool must be in the tool cache. A cache miss refreshes the cache once,
// because the tool can be new upstream, or this Worker has not listed the
// tools yet. A cache hit makes no upstream call.
func startTool(ctx context.Context, u *Upstream, c *toolCache, activityName string, nc temporalnexus.NexusClient, arguments json.RawMessage, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[any], error) {
	var none temporalnexus.TemporalOperationResult[any]
	name := nexus.ExtractHandlerInfo(ctx).Operation
	if !isToolName(name) {
		return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "invalid or reserved tool name")
	}

	// A call without input has no arguments. It becomes {}, as JSON null does.
	var object map[string]json.RawMessage
	if len(arguments) > 0 {
		if err := json.Unmarshal(arguments, &object); err != nil {
			return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "tool arguments must be a JSON object")
		}
	}
	if object == nil {
		arguments = json.RawMessage(`{}`)
	}

	tool, err := c.findTool(ctx, temporalnexus.GetLogger(ctx), name)
	switch {
	case errors.Is(err, errToolNotFound):
		return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "%v", err)
	case err != nil:
		return none, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "%v", err)
	}

	info := nexus.ExtractHandlerInfo(ctx)
	id := activityID(temporalnexus.GetOperationInfo(ctx).Endpoint, info.Service, info.Operation, opts.RequestID)
	options := u.activityOptionsFor(name, tool.Annotations, id)
	result, err := temporalnexus.StartUntypedActivity[any](ctx, nc, options, activityName, callToolInput{Tool: name, Arguments: arguments})
	if _, ok := errors.AsType[*serviceerror.ActivityExecutionAlreadyStarted](err); ok {
		// A retried start of an Activity that already closed. Return its
		// outcome.
		handle := nc.GetWorkflowClient().GetActivityHandle(client.GetActivityHandleOptions{ActivityID: id})
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

// activityOptionsFor returns the Activity options of the call with Activity
// ID id to tool name. It starts from the tool override, fills the proxy
// defaults, and sets the fields that the proxy owns. NewPlugin rejects an
// override that sets those fields.
//
// The default retry policy depends on the tool annotations. A tool with
// destructiveHint=true, readOnlyHint=false, and idempotentHint=false gets one
// attempt. Other tools use the Temporal default retry policy. A retry policy
// in the override replaces the default.
func (u *Upstream) activityOptionsFor(name string, annotations *mcp.ToolAnnotations, id string) client.StartActivityOptions {
	options := u.ActivityOptionsOverrides[name]
	if options.StartToCloseTimeout == 0 {
		options.StartToCloseTimeout = defaultStartToCloseTimeout
	}
	if options.RetryPolicy == nil && annotations != nil && !annotations.ReadOnlyHint &&
		annotations.DestructiveHint != nil && *annotations.DestructiveHint && !annotations.IdempotentHint {
		options.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	options.ID = id
	options.ActivityIDConflictPolicy = enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
	options.ActivityIDReusePolicy = enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE
	return options
}

// activityID returns "mcp-outbound-{endpoint}-{service}-{operation}-{request_id}".
// The Nexus request ID is unique, so the ID needs no escaping. An ID over the
// Temporal limit keeps its first bytes and gets the SHA-256 of the full ID,
// so it stays unique and within the limit.
func activityID(endpoint, service, operation, requestID string) string {
	id := activityIDPrefix + endpoint + "-" + service + "-" + operation + "-" + requestID
	if len(id) <= maxActivityIDBytes {
		return id
	}
	// 65 bytes hold "-" and the hex hash.
	prefix := strings.ToValidUTF8(id[:maxActivityIDBytes-65], "")
	return fmt.Sprintf("%s-%x", prefix, sha256.Sum256([]byte(id)))
}

// activityName returns the name of the call Activity of an upstream.
func activityName(service string) string {
	return "mcp-outbound." + service + ".call_upstream_tool"
}

// callToolInput is the input of the call Activity. Temporal records it, so it
// holds no destination or credentials.
type callToolInput struct {
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}

// callToolActivity returns the call Activity of an upstream. It calls the
// upstream tools/call and returns the tool result in the result envelope. A
// tool result with isError is a result, not a failure.
func callToolActivity(u *Upstream) func(context.Context, callToolInput) (map[string]json.RawMessage, error) {
	return func(ctx context.Context, call callToolInput) (map[string]json.RawMessage, error) {
		stop := startHeartbeats(ctx)
		defer stop()
		result, err := callTool(ctx, u, call.Tool, call.Arguments)
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
//
// NOTE: by default we do not heartbeat, unless the user overrides this
// setting intentionally.
func startHeartbeats(ctx context.Context) (stopFn func()) {
	// Heartbeating is off by default
	timeout := activity.GetInfo(ctx).HeartbeatTimeout
	if timeout <= 0 {
		stopFn = func() {}
		return stopFn
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
	stopFn = func() { cancel(); wg.Wait() }
	return stopFn
}

// defaultClassification returns the built-in classification of a failed
// upstream call.
//
// For now, only connection-related failures are retryable.
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
		// For now, this is the only retryable class of errors (connection-based failures).
		return FailureClassification{Type: failureConnection, Retryable: true}
	default:
		return FailureClassification{Type: failureOther}
	}
}

// classify uses a user-provided u.ClassifyError(...) to classify if we can
// retry this error. If none is provided, we fall back to defaultClassification(...).
func classify(u *Upstream, err error) (result FailureClassification) {
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
