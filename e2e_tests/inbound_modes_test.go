//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-mcp/go/inbound"
	enumspb "go.temporal.io/api/enums/v1"

	"github.com/temporalio/nexus-connector-mcp/e2e_tests/contract"
	"github.com/temporalio/nexus-connector-mcp/e2e_tests/harness"
)

const (
	// pollInterval is how often the tests poll tasks/get and Workflow status.
	pollInterval = 200 * time.Millisecond
	// returnTaskBudget is the ReturnTaskAfter of the connector that waits
	// before it returns a task.
	returnTaskBudget = 2 * time.Second
)

// taskClient returns a tasks client for the Go handler behind a connector with
// returnTaskAfter.
func taskClient(t *testing.T, returnTaskAfter time.Duration) harness.TaskClient {
	t.Helper()
	endpoint, err := env.Handler("go")
	require.NoError(t, err)
	url, err := env.Connector(harness.ConnectorOptions{Endpoint: endpoint, Service: contract.Service, ReturnTaskAfter: returnTaskAfter})
	require.NoError(t, err)
	return harness.TaskClient{URL: url}
}

// waitForTask polls tasks/get until the task leaves the working status.
func waitForTask(ctx context.Context, t *testing.T, client harness.TaskClient, taskID string) harness.Task {
	t.Helper()
	var task harness.Task
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var err error
		task, err = client.GetTask(ctx, taskID)
		require.NoError(c, err)
		require.NotEqual(c, "working", task.Status)
	}, callTimeout, pollInterval)
	return task
}

// requireWorkflowStatus waits until the Workflow id has status want.
func requireWorkflowStatus(ctx context.Context, t *testing.T, id string, want enumspb.WorkflowExecutionStatus) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		described, err := env.Client.DescribeWorkflow(ctx, id, "")
		require.NoError(c, err)
		require.Equal(c, want, described.Status)
	}, callTimeout, pollInterval)
}

func TestToolCallReturnsTaskThenResult(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()

	client := taskClient(t, 0)
	created, _, err := client.CallTool(ctx, contract.WorkflowRun, contract.Input{Message: "task"})
	require.NoError(t, err)
	require.Equal(t, "task", created.ResultType)
	require.NotEmpty(t, created.TaskID)

	task := waitForTask(ctx, t, client, created.TaskID)
	require.Equal(t, "completed", task.Status)
	require.NotNil(t, task.Result)
	require.Equal(t, contract.Expected(contract.WorkflowRun, "task"), structured(t, task.Result))
}

func TestReturnTaskAfterWaitsForTheResult(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()

	// A result within the budget comes back as a tool result.
	client := taskClient(t, returnTaskBudget)
	_, tool, err := client.CallTool(ctx, contract.SyncEcho, contract.Input{Message: "fast"})
	require.NoError(t, err)
	require.NotNil(t, tool)
	require.Equal(t, contract.Expected(contract.SyncEcho, "fast"), structured(t, tool))

	// An operation that runs past the budget comes back as a task.
	message := "budget-" + uuid.NewString()
	started := time.Now()
	created, tool, err := client.CallTool(ctx, contract.Slow, contract.Input{Message: message})
	require.NoError(t, err)
	require.Nil(t, tool)
	require.Equal(t, "task", created.ResultType)
	require.GreaterOrEqual(t, time.Since(started), returnTaskBudget)
	require.NoError(t, client.CancelTask(ctx, created.TaskID))
}

func TestTaskCancelCancelsTheOperation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()

	client := taskClient(t, 0)
	message := "task-cancel-" + uuid.NewString()
	created, _, err := client.CallTool(ctx, contract.Slow, contract.Input{Message: message})
	require.NoError(t, err)
	require.Equal(t, "task", created.ResultType)

	require.NoError(t, client.CancelTask(ctx, created.TaskID))
	task := waitForTask(ctx, t, client, created.TaskID)
	require.Equal(t, "cancelled", task.Status)
	requireWorkflowStatus(ctx, t, contract.SlowWorkflowID(message), enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED)
}

// TestClientCancelCancelsTheOperation closes a stateless call. The disconnect
// cancels the call, so the connector cancels the operation.
func TestClientCancelCancelsTheOperation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()

	endpoint, err := env.Handler("go")
	require.NoError(t, err)
	url, err := env.Connector(harness.ConnectorOptions{
		Endpoint:        endpoint,
		Service:         contract.Service,
		ReturnTaskAfter: inbound.NeverReturnTask,
	})
	require.NoError(t, err)
	session, err := harness.ConnectMCP(ctx, url)
	require.NoError(t, err)
	defer func() { _ = session.Close() }()

	message := "client-cancel-" + uuid.NewString()
	callCtx, cancelCall := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := session.CallTool(callCtx, &mcp.CallToolParams{Name: contract.Slow, Arguments: contract.Input{Message: message}})
		done <- err
	}()
	requireWorkflowStatus(ctx, t, contract.SlowWorkflowID(message), enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)
	cancelCall()
	require.Error(t, <-done)
	requireWorkflowStatus(ctx, t, contract.SlowWorkflowID(message), enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED)
}

func TestUnknownToolIsToolError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
	defer cancel()

	client := taskClient(t, inbound.NeverReturnTask)
	_, tool, err := client.CallTool(ctx, "missing", contract.Input{Message: "x"})
	require.NoError(t, err)
	require.NotNil(t, tool)
	require.True(t, tool.IsError)
}
