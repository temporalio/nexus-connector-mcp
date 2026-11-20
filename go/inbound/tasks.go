package inbound

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

const (
	// tasksExtension is the ID of the MCP tasks extension (SEP-2663).
	tasksExtension = "io.modelcontextprotocol/tasks"
	// pollInterval is the poll interval that the connector suggests to
	// clients.
	pollInterval = 2 * time.Second
	// maxTaskIDBytes limits a task ID before any Temporal call.
	maxTaskIDBytes = 1024
	// requestIDHexLen is the length of the random suffix of an operation ID.
	requestIDHexLen = 32
	// minResultWait is the shortest useful result read for tasks/get. The
	// SDK gives each poll RPC at least one second, so a shorter read always
	// ends on a deadline.
	minResultWait = 2 * time.Second
)

var errUnknownOperation = errors.New("unknown task")

// task is the Task shape of SEP-2663. The task ID is the Nexus operation ID.
type task struct {
	TaskID string `json:"taskId"`
	// Status is working, completed, failed, or cancelled.
	Status         string `json:"status"`
	CreatedAt      string `json:"createdAt"`
	LastUpdatedAt  string `json:"lastUpdatedAt"`
	PollIntervalMs int64  `json:"pollIntervalMs"`
	// TTLMs is always null. Temporal keeps the operation for the retention
	// period of the namespace.
	TTLMs *int64 `json:"ttlMs"`
}

// createTaskResult answers tools/call with a task. The SDK sets resultType
// only on its own result types, so this type sets it.
type createTaskResult struct {
	mcp.ResultBase

	ResultType string `json:"resultType"`
	task
}

// getTaskResult answers tasks/get. Result is set when the task completed.
// Error is set when the task failed.
type getTaskResult struct {
	mcp.ResultBase

	ResultType string `json:"resultType"`
	task
	Result *mcp.CallToolResult `json:"result,omitempty"`
	Error  *jsonrpc.Error      `json:"error,omitempty"`
}

// ackResult answers tasks/cancel.
type ackResult struct {
	mcp.ResultBase

	ResultType string `json:"resultType"`
}

// taskIDParams are the params of the tasks/* methods.
type taskIDParams struct {
	mcp.ParamsBase

	TaskID string `json:"taskId"`
}

func newTask(id, status string, created, updated time.Time) task {
	return task{
		TaskID:         id,
		Status:         status,
		CreatedAt:      created.UTC().Format(time.RFC3339Nano),
		LastUpdatedAt:  updated.UTC().Format(time.RFC3339Nano),
		PollIntervalMs: pollInterval.Milliseconds(),
	}
}

func newCreateTaskResult(id string) *createTaskResult {
	now := time.Now()
	return &createTaskResult{ResultType: "task", task: newTask(id, "working", now, now)}
}

// clientSupportsTasks reports whether the request declares the tasks
// extension. Clients on MCP 2026-07-28 send their capabilities in the _meta
// of each request.
func clientSupportsTasks(meta mcp.Meta) bool {
	caps, _ := meta[mcp.MetaKeyClientCapabilities].(map[string]any)
	extensions, _ := caps["extensions"].(map[string]any)
	_, ok := extensions[tasksExtension]
	return ok
}

// checkTaskID checks a task ID before any Temporal call. The ID must have the
// connector prefix and a 128-bit random suffix in hex. With a verified
// caller, the owner hash before the suffix must match that caller. Without a
// verified caller, anyone with the ID can read or cancel the task.
func checkTaskID(id, owner string) error {
	separator := strings.LastIndexByte(id, '-')
	if !strings.HasPrefix(id, idPrefix+"-") || separator <= 0 || len(id) > maxTaskIDBytes || len(id)-separator-1 != requestIDHexLen {
		return errUnknownOperation
	}
	if _, err := hex.DecodeString(id[separator+1:]); err != nil {
		return errUnknownOperation
	}
	if owner != "" {
		rest := id[:separator]
		separator = strings.LastIndexByte(rest, '-')
		if separator <= 0 || rest[separator+1:] != owner {
			return errUnknownOperation
		}
	}
	return nil
}

// getTask maps the operation state to a tasks/get result. It reads the result
// only after the operation closes, so it does not block.
func (c *connector) getTask(ctx context.Context, id string) (*getTaskResult, error) {
	h := c.nexus.handle(id)
	d, err := h.Describe(ctx, client.DescribeNexusOperationOptions{})
	if err != nil {
		return nil, err
	}
	created, updated := d.ScheduledTime, d.ScheduledTime
	if !d.CloseTime.IsZero() {
		updated = d.CloseTime
	}
	out := &getTaskResult{ResultType: "complete"}
	switch d.Status {
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING:
		out.task = newTask(id, "working", created, updated)
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, enumspb.NEXUS_OPERATION_EXECUTION_STATUS_FAILED:
		result, err := wait(ctx, h, minResultWait)
		if err != nil {
			return nil, err
		}
		if result.running {
			// The read did not finish in time. Report working, so the client
			// polls again.
			out.task = newTask(id, "working", created, updated)
			break
		}
		// A handler failure is a tool error with isError set, so the task
		// completes. tools/call without a task returns the same result.
		out.task = newTask(id, "completed", created, updated)
		out.Result = toCallToolResult(result)
	case enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED:
		out.task = newTask(id, "cancelled", created, updated)
	default:
		// TERMINATED and TIMED_OUT.
		out.task = newTask(id, "failed", created, updated)
		out.Error = &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "operation timed out or was terminated"}
	}
	return out, nil
}

// addTaskMethods adds tasks/get, tasks/cancel, and tasks/update to s. The MCP
// Go SDK has no task support, so these are custom methods.
func addTaskMethods(s *mcp.Server, c *connector) error {
	// checked runs fn after the capability check and the task ID check.
	checked := func(fn func(context.Context, string) (mcp.Result, error)) func(context.Context, *mcp.ServerSession, *taskIDParams) (mcp.Result, error) {
		return func(ctx context.Context, _ *mcp.ServerSession, p *taskIDParams) (mcp.Result, error) {
			if !clientSupportsTasks(p.Meta) {
				return nil, errMissingTasksCapability
			}
			if err := checkTaskID(p.TaskID, c.owner(ctx)); err != nil {
				return nil, taskError(err)
			}
			result, err := fn(ctx, p.TaskID)
			if err != nil {
				return nil, taskError(err)
			}
			return result, nil
		}
	}
	if err := mcp.AddReceivingCustomMethod(s, "tasks/get", checked(func(ctx context.Context, id string) (mcp.Result, error) {
		return c.getTask(ctx, id)
	})); err != nil {
		return err
	}
	if err := mcp.AddReceivingCustomMethod(s, "tasks/cancel", checked(func(ctx context.Context, id string) (mcp.Result, error) {
		h := c.nexus.handle(id)
		if err := h.Cancel(ctx, client.CancelNexusOperationOptions{Reason: "cancelled by MCP client"}); err != nil {
			return nil, err
		}
		return &ackResult{ResultType: "complete"}, nil
	})); err != nil {
		return err
	}
	// A Nexus operation does not ask the client for input, so no task is
	// input_required. The first release does not support tasks/update.
	return mcp.AddReceivingCustomMethod(s, "tasks/update", checked(func(context.Context, string) (mcp.Result, error) {
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "task does not accept input"}
	}))
}

// errMissingTasksCapability answers a task method from a client that did not
// declare the tasks extension. SEP-2575 requires data.requiredCapabilities on
// this error. The data is a plain map, because mcp.ClientCapabilities always
// encodes "roots".
var errMissingTasksCapability = func() *jsonrpc.Error {
	data, _ := json.Marshal(map[string]any{
		"requiredCapabilities": map[string]any{"extensions": map[string]any{tasksExtension: map[string]any{}}},
	})
	return &jsonrpc.Error{
		Code:    mcp.CodeMissingRequiredClientCapabilities,
		Message: "missing required client capability: " + tasksExtension,
		Data:    data,
	}
}()

// taskError maps an error of a task method to a JSON-RPC error. An unknown or
// foreign task ID and a missing operation have the same error, so a caller
// cannot learn which IDs exist.
func taskError(err error) error {
	var rpcErr *jsonrpc.Error
	var notFound *serviceerror.NotFound
	switch {
	case errors.As(err, &rpcErr):
		return rpcErr
	case errors.Is(err, errUnknownOperation), errors.As(err, &notFound):
		return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: errUnknownOperation.Error()}
	}
	return &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()}
}
