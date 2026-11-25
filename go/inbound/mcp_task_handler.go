package inbound

import (
	"context"
	"errors"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/temporalio/nexus-connector-mcp/go/inbound/internal/mcpwire"
)

const (
	// minResultWait is the shortest useful result read for tasks/get. The
	// SDK gives each poll RPC at least one second, so a shorter read always
	// ends on a deadline.
	minResultWait = 2 * time.Second
)

// errUnknownTask answers a task ID that is not valid for the caller, or whose
// operation does not exist. Both cases have the same error, so a caller
// cannot learn which IDs exist.
var errUnknownTask = &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "unknown task"}

// addTaskMethods adds tasks/get, tasks/cancel, and tasks/update to s. The task
// ID is the Nexus operation ID.
//
// TODO(long-nt-tran): register these with the SDK task API when the SDK
// supports the tasks extension. SDK v1.8.0 has none, so they are custom
// methods.
func addTaskMethods(s *mcp.Server, c *connector) error {
	if err := mcp.AddReceivingCustomMethod(s, "tasks/get", c.taskMethod(func(ctx context.Context, id string) (mcp.Result, error) {
		return c.getTask(ctx, id)
	})); err != nil {
		return err
	}
	if err := mcp.AddReceivingCustomMethod(s, "tasks/cancel", c.taskMethod(c.cancelTask)); err != nil {
		return err
	}
	// A Nexus operation does not ask the client for input, so no task is
	// input_required. The first release does not support tasks/update.
	return mcp.AddReceivingCustomMethod(s, "tasks/update", c.taskMethod(func(context.Context, string) (mcp.Result, error) {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "task does not accept input"}
	}))
}

// taskMethod returns a tasks/* method that runs fn after the capability check
// and the task ID check.
func (c *connector) taskMethod(fn func(ctx context.Context, id string) (mcp.Result, error)) func(context.Context, *mcp.ServerSession, *mcpwire.TaskIDParams) (mcp.Result, error) {
	return func(ctx context.Context, _ *mcp.ServerSession, p *mcpwire.TaskIDParams) (mcp.Result, error) {
		if !mcpwire.ClientSupportsTasks(p.Meta) {
			return nil, mcpwire.MissingTasksCapability
		}
		if err := checkOperationID(p.TaskID, c.owner(ctx)); err != nil {
			return nil, taskError(err)
		}
		result, err := fn(ctx, p.TaskID)
		if err != nil {
			return nil, taskError(err)
		}
		return result, nil
	}
}

// getTask maps the operation state to a tasks/get result. It reads the result
// only after the operation closes, so it does not block.
func (c *connector) getTask(ctx context.Context, id string) (*mcpwire.GetTaskResult, error) {
	state, err := c.nexus.describe(ctx, id)
	if err != nil {
		return nil, err
	}
	created, updated := state.scheduled, state.scheduled
	if !state.closed.IsZero() {
		updated = state.closed
	}
	out := &mcpwire.GetTaskResult{ResultType: "complete"}
	switch state.status {
	case opRunning:
		out.Task = mcpwire.NewTask(id, mcpwire.TaskWorking, created, updated)
	case opClosed:
		result, err := c.nexus.read(ctx, id, minResultWait)
		if err != nil {
			return nil, err
		}
		if result.running {
			// The read did not finish in time. Report working, so the client
			// polls again.
			out.Task = mcpwire.NewTask(id, mcpwire.TaskWorking, created, updated)
			break
		}
		// A handler failure is a tool error with isError set, so the task
		// completes. tools/call without a task returns the same result.
		out.Task = mcpwire.NewTask(id, mcpwire.TaskCompleted, created, updated)
		out.Result = toCallToolResult(result)
	case opCancelled:
		out.Task = mcpwire.NewTask(id, mcpwire.TaskCancelled, created, updated)
	case opEnded:
		out.Task = mcpwire.NewTask(id, mcpwire.TaskFailed, created, updated)
		out.Error = &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "operation timed out or was terminated"}
	}
	return out, nil
}

// cancelTask cancels the operation of the task.
func (c *connector) cancelTask(ctx context.Context, id string) (mcp.Result, error) {
	if err := c.nexus.cancel(ctx, id, "cancelled by MCP client"); err != nil {
		return nil, err
	}
	return &mcpwire.AckResult{ResultType: "complete"}, nil
}

// taskError maps an error of a task method to a JSON-RPC error.
func taskError(err error) error {
	var rpcErr *jsonrpc.Error
	switch {
	case errors.As(err, &rpcErr):
		return rpcErr
	case errors.Is(err, errInvalidOperationID), errors.Is(err, errOperationNotFound):
		return errUnknownTask
	}
	return &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
}
