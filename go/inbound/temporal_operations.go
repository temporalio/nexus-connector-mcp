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
	// cancelTimeout limits a cancel request.
	cancelTimeout = 10 * time.Second
)

// errOperationNotFound reports an operation ID that Temporal does not know.
var errOperationNotFound = errors.New("operation not found")

// opStatus is the status of an operation, without Temporal types.
type opStatus int

const (
	// opRunning: the operation runs.
	opRunning opStatus = iota
	// opClosed: the operation completed or failed. read returns its result.
	opClosed
	// opCancelled: the operation was cancelled.
	opCancelled
	// opEnded: the operation timed out or was terminated. It has no result.
	opEnded
)

// opState describes an operation.
type opState struct {
	status    opStatus
	scheduled time.Time
	// closed is zero while the operation runs.
	closed time.Time
}

// nexusClient runs standalone Nexus operations on one Nexus service. It does
// not use MCP types.
type nexusClient struct {
	// client reads operations by ID.
	client client.Client
	nexus  client.NexusClient
}

func newNexusClient(c client.Client, endpoint, service string) (*nexusClient, error) {
	nc, err := c.NewNexusClient(client.NexusClientOptions{Endpoint: endpoint, Service: service})
	if err != nil {
		return nil, err
	}
	return &nexusClient{client: c, nexus: nc}, nil
}

// handle returns the handle of the operation with the operation ID id.
func (n *nexusClient) handle(id string) client.NexusOperationHandle {
	return n.client.GetNexusOperationHandle(client.GetNexusOperationHandleOptions{OperationID: id})
}

// describe returns the state of the operation with the operation ID id.
func (n *nexusClient) describe(ctx context.Context, id string) (opState, error) {
	d, err := n.handle(id).Describe(ctx, client.DescribeNexusOperationOptions{})
	if err != nil {
		return opState{}, notFound(err)
	}
	state := opState{scheduled: d.ScheduledTime, closed: d.CloseTime}
	switch d.Status {
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING:
		state.status = opRunning
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED:
		state.status = opClosed
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED:
		state.status = opCancelled
	default:
		// TERMINATED and TIMED_OUT.
		state.status = opEnded
	}
	return state, nil
}

// read reads the result of the operation with the operation ID id. See wait.
func (n *nexusClient) read(ctx context.Context, id string, budget time.Duration) (outcome, error) {
	return wait(ctx, n.handle(id), budget)
}

// cancel asks Nexus to cancel the operation with the operation ID id.
func (n *nexusClient) cancel(ctx context.Context, id, reason string) error {
	return notFound(cancelOperation(ctx, n.handle(id), reason))
}

// notFound returns errOperationNotFound for a Temporal not-found error, and
// err otherwise.
func notFound(err error) error {
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return errOperationNotFound
	}
	return err
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

// outcome is the state of one result read.
type outcome struct {
	// running is set when the read budget ended before the operation closed.
	running bool
	// value is the raw operation result.
	value json.RawMessage
	// failure is set when the operation failed.
	failure error
}

// wait reads the result until the operation closes or ctx ends. If budget is
// positive, it also stops after budget and returns outcome.running. It
// returns a failed read as err, and a failed operation as outcome.failure.
func wait(ctx context.Context, h client.NexusOperationHandle, budget time.Duration) (outcome, error) {
	readCtx := ctx
	if budget > 0 {
		var cancel context.CancelFunc
		readCtx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	for {
		var raw json.RawMessage
		err := h.Get(readCtx, &raw)
		switch {
		case err == nil:
			return outcome{value: raw}, nil
		case ctx.Err() != nil:
			return outcome{}, ctx.Err()
		case readCtx.Err() != nil:
			// The budget ended. The operation keeps running.
			return outcome{running: true}, nil
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
