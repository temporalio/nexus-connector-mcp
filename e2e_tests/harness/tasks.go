package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// tasksExtension is the MCP tasks extension.
	tasksExtension = "io.modelcontextprotocol/tasks"
	// protocolVersion is the MCP version that sends client capabilities in
	// the _meta of each request.
	protocolVersion = "2026-07-28"
)

// TaskClient sends JSON-RPC requests that declare the MCP tasks extension.
//
// TODO(long-nt-tran): use the MCP Go SDK client when it supports the tasks
// extension. SDK v1.8.0 decodes a task result as a CallToolResult and drops
// the task fields, so this client sends the requests itself, with the same
// headers and _meta as the SDK client.
type TaskClient struct {
	// URL is the MCP URL of the connector.
	URL string
}

// Task is the task part of a tools/call or tasks/get result.
type Task struct {
	ResultType string              `json:"resultType"`
	TaskID     string              `json:"taskId"`
	Status     string              `json:"status"`
	Result     *mcp.CallToolResult `json:"result"`
}

// CallTool sends tools/call. A task result has ResultType "task". A tool
// result has its fields in tool.
func (c TaskClient) CallTool(ctx context.Context, name string, arguments any) (task Task, tool *mcp.CallToolResult, err error) {
	raw, err := c.call(ctx, "tools/call", name, map[string]any{"name": name, "arguments": arguments})
	if err != nil {
		return Task{}, nil, err
	}
	if err := json.Unmarshal(raw, &task); err != nil {
		return Task{}, nil, err
	}
	if task.ResultType == "task" {
		return task, nil, nil
	}
	tool = &mcp.CallToolResult{}
	return task, tool, json.Unmarshal(raw, tool)
}

// GetTask sends tasks/get.
func (c TaskClient) GetTask(ctx context.Context, taskID string) (Task, error) {
	return c.taskMethod(ctx, "tasks/get", taskID)
}

// CancelTask sends tasks/cancel.
func (c TaskClient) CancelTask(ctx context.Context, taskID string) error {
	_, err := c.taskMethod(ctx, "tasks/cancel", taskID)
	return err
}

func (c TaskClient) taskMethod(ctx context.Context, method, taskID string) (Task, error) {
	raw, err := c.call(ctx, method, "", map[string]any{"taskId": taskID})
	if err != nil {
		return Task{}, err
	}
	var task Task
	return task, json.Unmarshal(raw, &task)
}

// call sends one JSON-RPC request and returns its result.
func (c TaskClient) call(ctx context.Context, method, name string, params map[string]any) (json.RawMessage, error) {
	params["_meta"] = map[string]any{
		mcp.MetaKeyProtocolVersion:    protocolVersion,
		mcp.MetaKeyClientInfo:         map[string]any{"name": "e2e", "version": "v0.0.0"},
		mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{tasksExtension: map[string]any{}}},
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("Mcp-Protocol-Version", protocolVersion)
	req.Header.Set("Mcp-Method", method)
	if name != "" {
		req.Header.Set("Mcp-Name", name)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d", method, resp.StatusCode)
	}

	// The response is JSON, or SSE with the JSON in a data line.
	var data []byte
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(nil, 1<<20)
		for scanner.Scan() {
			if rest, ok := strings.CutPrefix(scanner.Text(), "data:"); ok {
				data = []byte(strings.TrimSpace(rest))
				break
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	} else if err := json.NewDecoder(resp.Body).Decode((*json.RawMessage)(&data)); err != nil {
		return nil, err
	}
	var response struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("%s: decode %q: %w", method, data, err)
	}
	if response.Error != nil {
		return nil, fmt.Errorf("%s: JSON-RPC error %d: %s", method, response.Error.Code, response.Error.Message)
	}
	if response.Result == nil {
		return nil, errors.New(method + ": no result")
	}
	return response.Result, nil
}
