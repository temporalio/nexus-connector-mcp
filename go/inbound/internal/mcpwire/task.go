package mcpwire

import (
	"encoding/json"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// TasksExtension is the ID of the MCP tasks extension (SEP-2663).
	TasksExtension = "io.modelcontextprotocol/tasks"

	// Task statuses of SEP-2663.
	TaskWorking   = "working"
	TaskCompleted = "completed"
	TaskFailed    = "failed"
	TaskCancelled = "cancelled"

	// pollInterval is the poll interval that the connector suggests to
	// clients.
	pollInterval = 2 * time.Second
)

// TODO(long-nt-tran): replace Task, CreateTaskResult, GetTaskResult,
// AckResult, and TaskIDParams with the MCP Go SDK types when the SDK supports
// the tasks extension. SDK v1.8.0 has no task types, so these types follow
// SEP-2663.

// Task is the Task shape of SEP-2663.
type Task struct {
	TaskID string `json:"taskId"`
	// Status is TaskWorking, TaskCompleted, TaskFailed, or TaskCancelled.
	Status         string `json:"status"`
	CreatedAt      string `json:"createdAt"`
	LastUpdatedAt  string `json:"lastUpdatedAt"`
	PollIntervalMs int64  `json:"pollIntervalMs"`
	// TTLMs is always null. Temporal keeps the operation for the retention
	// period of the namespace.
	TTLMs *int64 `json:"ttlMs"`
}

// NewTask returns a Task with the suggested poll interval.
func NewTask(id, status string, created, updated time.Time) Task {
	return Task{
		TaskID:         id,
		Status:         status,
		CreatedAt:      created.UTC().Format(time.RFC3339Nano),
		LastUpdatedAt:  updated.UTC().Format(time.RFC3339Nano),
		PollIntervalMs: pollInterval.Milliseconds(),
	}
}

// CreateTaskResult answers tools/call with a task. The SDK sets resultType
// only on its own result types, so this type sets it.
type CreateTaskResult struct {
	mcp.ResultBase

	ResultType string `json:"resultType"`
	Task
}

// NewCreateTaskResult returns a working task with the task ID id.
func NewCreateTaskResult(id string) *CreateTaskResult {
	now := time.Now()
	return &CreateTaskResult{ResultType: "task", Task: NewTask(id, TaskWorking, now, now)}
}

// GetTaskResult answers tasks/get. Result is set when the task completed.
// Error is set when the task failed.
type GetTaskResult struct {
	mcp.ResultBase

	ResultType string `json:"resultType"`
	Task
	Result *mcp.CallToolResult `json:"result,omitempty"`
	Error  *jsonrpc.Error      `json:"error,omitempty"`
}

// AckResult answers tasks/cancel.
type AckResult struct {
	mcp.ResultBase

	ResultType string `json:"resultType"`
}

// TaskIDParams are the params of the tasks/* methods.
type TaskIDParams struct {
	mcp.ParamsBase

	TaskID string `json:"taskId"`
}

// ClientSupportsTasks reports whether the request declares the tasks
// extension. Clients on MCP 2026-07-28 send their capabilities in the _meta
// of each request.
//
// TODO(long-nt-tran): use the SDK to read the request capabilities when it
// exports that. SDK v1.8.0 decodes them only internally, so this reads the
// _meta map.
func ClientSupportsTasks(meta mcp.Meta) bool {
	caps, _ := meta[mcp.MetaKeyClientCapabilities].(map[string]any)
	extensions, _ := caps["extensions"].(map[string]any)
	_, ok := extensions[TasksExtension]
	return ok
}

// MissingTasksCapability answers a task method from a client that did not
// declare the tasks extension. SEP-2575 requires data.requiredCapabilities on
// this error. The data is a plain map, because mcp.ClientCapabilities always
// encodes "roots".
//
// TODO(long-nt-tran): use the SDK error for a missing capability when the SDK
// supports the tasks extension.
var MissingTasksCapability = func() *jsonrpc.Error {
	data, _ := json.Marshal(map[string]any{
		"requiredCapabilities": map[string]any{"extensions": map[string]any{TasksExtension: map[string]any{}}},
	})
	return &jsonrpc.Error{
		Code:    mcp.CodeMissingRequiredClientCapabilities,
		Message: "missing required client capability: " + TasksExtension,
		Data:    data,
	}
}()
