package outbound

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
)

const (
	listToolsOperation = "list_tools"
	// reservedPrefix starts names that Temporal reserves.
	reservedPrefix = "__temporal_"
	// defaultStartToCloseTimeout is the proxy default for each Activity
	// attempt.
	defaultStartToCloseTimeout = time.Minute
	// setupTimeout limits one discovery, and the connection setup of one tool
	// call.
	setupTimeout = 10 * time.Second
	// activityIDPrefix starts every Activity ID that the proxy generates.
	activityIDPrefix = "mcp-outbound-"
	// maxActivityIDBytes is the default Temporal limit for IDs.
	maxActivityIDBytes = 1000
)

// prepareUpstream validates u and returns a copy that later changes by the
// caller do not affect.
func prepareUpstream(u Upstream) (Upstream, error) {
	if strings.TrimSpace(u.Service) == "" || strings.HasPrefix(u.Service, reservedPrefix) {
		return u, fmt.Errorf("outbound: Upstream.Service %q must be non-empty and not reserved", u.Service)
	}
	if u.ClientFactory == nil {
		return u, fmt.Errorf("outbound: Upstream %q needs a ClientFactory", u.Service)
	}
	u.ActivityOptionsOverrides = maps.Clone(u.ActivityOptionsOverrides)
	for name := range u.ActivityOptionsOverrides {
		if !isToolName(name) {
			return u, fmt.Errorf("outbound: Upstream %q: tool name %q in ActivityOptionsOverrides must be non-empty and not reserved", u.Service, name)
		}
		if _, err := u.optionsFor(name, nil); err != nil {
			return u, fmt.Errorf("outbound: Upstream %q: tool %q: %w", u.Service, name, err)
		}
	}
	return u, nil
}

// optionsFor returns the Activity options of a new call to tool name. It
// applies the proxy defaults, then the nonzero fields of the tool override.
//
// The default retry policy depends on the tool annotations. A tool with
// destructiveHint=true, readOnlyHint=false, and idempotentHint=false gets one
// attempt. Other tools use the Temporal default retry policy.
func (u Upstream) optionsFor(name string, annotations *mcp.ToolAnnotations) (client.StartActivityOptions, error) {
	options := client.StartActivityOptions{StartToCloseTimeout: defaultStartToCloseTimeout}
	if annotations != nil && !annotations.ReadOnlyHint && annotations.DestructiveHint != nil && *annotations.DestructiveHint && !annotations.IdempotentHint {
		options.RetryPolicy = &temporal.RetryPolicy{MaximumAttempts: 1}
	}
	override := u.ActivityOptionsOverrides[name]
	if override.ID != "" || override.TaskQueue != "" || override.ActivityIDConflictPolicy != 0 || override.ActivityIDReusePolicy != 0 {
		return options, errors.New("the proxy sets the Activity ID, task queue, and conflict and reuse policies")
	}
	if override.StartToCloseTimeout != 0 {
		options.StartToCloseTimeout = override.StartToCloseTimeout
	}
	if override.ScheduleToCloseTimeout != 0 {
		options.ScheduleToCloseTimeout = override.ScheduleToCloseTimeout
	}
	if override.ScheduleToStartTimeout != 0 {
		options.ScheduleToStartTimeout = override.ScheduleToStartTimeout
	}
	if override.HeartbeatTimeout != 0 {
		options.HeartbeatTimeout = override.HeartbeatTimeout
	}
	if override.StartDelay != 0 {
		options.StartDelay = override.StartDelay
	}
	// A non-nil retry policy replaces the whole default policy.
	if override.RetryPolicy != nil {
		options.RetryPolicy = override.RetryPolicy
	}
	if override.Priority != (temporal.Priority{}) {
		options.Priority = override.Priority
	}
	if override.TypedSearchAttributes.Size() != 0 {
		options.TypedSearchAttributes = override.TypedSearchAttributes
	}
	if override.Summary != "" {
		options.Summary = override.Summary
	}
	if override.StaticDetails != "" {
		options.StaticDetails = override.StaticDetails
	}
	for _, d := range []time.Duration{options.StartToCloseTimeout, options.ScheduleToCloseTimeout, options.ScheduleToStartTimeout, options.HeartbeatTimeout, options.StartDelay} {
		if d < 0 {
			return options, errors.New("timeouts and start delay must not be negative")
		}
	}
	return options, nil
}

// newActivityID returns the Activity ID of the current Nexus start. A retried
// start gets the same ID.
func newActivityID(ctx context.Context, opts temporalnexus.StartTemporalOperationOptions) string {
	info := nexus.ExtractHandlerInfo(ctx)
	return activityID(temporalnexus.GetOperationInfo(ctx).Endpoint, info.Service, info.Operation, opts.RequestID)
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
