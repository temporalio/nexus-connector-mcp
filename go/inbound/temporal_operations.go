package inbound

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// cancelTimeout limits the cancel request that follows a cancelled tool
	// call.
	cancelTimeout = 10 * time.Second
)

// nexusClient runs standalone Nexus operations on one Nexus service. It does
// not use MCP types.
type nexusClient struct {
	nexus client.NexusClient
}

func newNexusClient(c client.Client, endpoint, service string) (*nexusClient, error) {
	nc, err := c.NewNexusClient(client.NexusClientOptions{Endpoint: endpoint, Service: service})
	if err != nil {
		return nil, err
	}
	return &nexusClient{nexus: nc}, nil
}

// start starts operation with the operation ID id. A timeout of zero means no
// schedule-to-close timeout.
func (n *nexusClient) start(ctx context.Context, id, operation string, input any, timeout time.Duration) (client.NexusOperationHandle, error) {
	return n.nexus.ExecuteOperation(ctx, operation, input, client.StartNexusOperationOptions{
		ID:                     id,
		IDReusePolicy:          enumspb.NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE,
		Summary:                operation,
		ScheduleToCloseTimeout: timeout,
	})
}

// cancelOperation asks Nexus to cancel the operation of h. It uses its own
// context, because the caller context has usually ended.
func cancelOperation(ctx context.Context, h client.NexusOperationHandle, reason string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelTimeout)
	defer cancel()
	return h.Cancel(ctx, client.CancelNexusOperationOptions{Reason: reason})
}

// outcome is the result of a closed operation.
type outcome struct {
	// value is the raw operation result.
	value json.RawMessage
	// failure is set when the operation failed.
	failure error
}

// wait reads the result until the operation closes or ctx ends. It returns a
// failed read as err, and a failed operation as outcome.failure.
func wait(ctx context.Context, h client.NexusOperationHandle) (outcome, error) {
	for {
		var raw json.RawMessage
		err := h.Get(ctx, &raw)
		switch {
		case err == nil:
			return outcome{value: raw}, nil
		case ctx.Err() != nil:
			return outcome{}, ctx.Err()
		case isRPCDeadline(err):
			// One poll RPC ended on its own deadline. The caller still waits,
			// so read again.
		case isReadError(err):
			return outcome{}, err
		default:
			return outcome{failure: err}, nil
		}
	}
}

// isRPCDeadline reports whether err is the end of one poll RPC. The SDK can
// report it as a context error, a gRPC status, or a Temporal service error.
func isRPCDeadline(err error) bool {
	var deadlineErr *serviceerror.DeadlineExceeded
	var canceledErr *serviceerror.Canceled
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.As(err, &deadlineErr) || errors.As(err, &canceledErr) {
		return true
	}
	code := status.Code(err)
	return code == codes.DeadlineExceeded || code == codes.Canceled
}

// isReadError reports whether err is a failed read. Service and gRPC errors
// describe the read. Operation failures are Temporal failure errors.
func isReadError(err error) bool {
	var apiError interface{ Status() *status.Status }
	if errors.As(err, &apiError) {
		return true
	}
	_, ok := status.FromError(err)
	return ok
}
