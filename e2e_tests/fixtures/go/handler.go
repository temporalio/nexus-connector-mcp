// Package gofixtures holds the Go fixtures. The e2e tests run them in the test
// process: the contract handler Worker, the outbound proxy Worker, and the
// upstream MCP server.
package gofixtures

import (
	"context"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/temporalio/nexus-connector-mcp/go/nexusmcp"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/temporalio/nexus-connector-mcp/e2e_tests/contract"
)

// EchoWorkflow echoes its input.
func EchoWorkflow(_ workflow.Context, in contract.Input) (contract.Output, error) {
	return contract.Output{Message: in.Message, Kind: contract.KindWorkflow}, nil
}

// KeeperWorkflow runs until it is cancelled. Its update echoes the input.
func KeeperWorkflow(ctx workflow.Context) error {
	err := workflow.SetUpdateHandler(ctx, contract.EchoUpdateName, func(_ workflow.Context, in contract.Input) (contract.Output, error) {
		return contract.Output{Message: in.Message, Kind: contract.KindUpdate}, nil
	})
	if err != nil {
		return err
	}
	return workflow.Await(ctx, func() bool { return false })
}

// SlowWorkflow runs for an hour.
func SlowWorkflow(ctx workflow.Context, _ contract.Input) (contract.Output, error) {
	err := workflow.Sleep(ctx, time.Hour)
	return contract.Output{}, err
}

// EchoActivity echoes its input.
func EchoActivity(_ context.Context, in contract.Input) (contract.Output, error) {
	return contract.Output{Message: in.Message, Kind: contract.KindActivity}, nil
}

// keeperID returns the ID of the KeeperWorkflow that the update operation
// targets.
func keeperID(taskQueue string) string {
	return "contract-keeper-" + taskQueue
}

func newTemporalOperation(name string, start func(context.Context, temporalnexus.NexusClient, contract.Input, temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[contract.Output], error)) nexus.Operation[contract.Input, contract.Output] {
	return temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[contract.Input, contract.Output]{Name: name, Start: start})
}

// newContractService returns the contract service. The update operation
// targets the KeeperWorkflow of taskQueue.
func newContractService(taskQueue string) (*nexus.Service, error) {
	syncEcho := nexus.NewSyncOperation(contract.SyncEcho, func(_ context.Context, in contract.Input, _ nexus.StartOperationOptions) (contract.Output, error) {
		return contract.Output{Message: in.Message, Kind: contract.KindSync}, nil
	})
	workflowRun := temporalnexus.NewWorkflowRunOperation(contract.WorkflowRun, EchoWorkflow,
		func(_ context.Context, _ contract.Input, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			return client.StartWorkflowOptions{ID: opts.RequestID}, nil
		})
	startWorkflow := newTemporalOperation(contract.StartWorkflow,
		func(ctx context.Context, nc temporalnexus.NexusClient, in contract.Input, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[contract.Output], error) {
			return temporalnexus.StartWorkflow(ctx, nc, client.StartWorkflowOptions{ID: opts.RequestID}, EchoWorkflow, in)
		})
	startActivity := newTemporalOperation(contract.StartActivity,
		func(ctx context.Context, nc temporalnexus.NexusClient, in contract.Input, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[contract.Output], error) {
			return temporalnexus.StartActivity(ctx, nc, client.StartActivityOptions{ID: opts.RequestID, StartToCloseTimeout: time.Minute}, EchoActivity, in)
		})
	startUpdate := newTemporalOperation(contract.StartUpdate,
		func(ctx context.Context, nc temporalnexus.NexusClient, in contract.Input, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[contract.Output], error) {
			return temporalnexus.StartUpdateWorkflow[contract.Output](ctx, nc, client.UpdateWorkflowOptions{
				WorkflowID:   keeperID(taskQueue),
				UpdateName:   contract.EchoUpdateName,
				Args:         []any{in},
				WaitForStage: client.WorkflowUpdateStageAccepted,
			})
		})
	fail := nexus.NewSyncOperation(contract.Fail, func(_ context.Context, in contract.Input, _ nexus.StartOperationOptions) (contract.Output, error) {
		return contract.Output{}, nexus.NewOperationFailedErrorf("failed: %s", in.Message)
	})
	slow := temporalnexus.NewWorkflowRunOperation(contract.Slow, SlowWorkflow,
		func(_ context.Context, in contract.Input, _ nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			return client.StartWorkflowOptions{ID: contract.SlowWorkflowID(in.Message)}, nil
		})

	service := nexus.NewService(contract.Service)
	err := nexusmcp.Register(service,
		nexusmcp.Tool(syncEcho, nexusmcp.ToolOptions{Description: "Echo the message in a sync operation."}),
		nexusmcp.Tool(workflowRun, nexusmcp.ToolOptions{Description: "Echo the message in a Workflow run."}),
		nexusmcp.Tool(startWorkflow, nexusmcp.ToolOptions{Description: "Echo the message in a started Workflow."}),
		nexusmcp.Tool(startActivity, nexusmcp.ToolOptions{Description: "Echo the message in an Activity."}),
		nexusmcp.Tool(startUpdate, nexusmcp.ToolOptions{Description: "Echo the message in a Workflow update."}),
		nexusmcp.Tool(fail, nexusmcp.ToolOptions{Description: "Fail the operation."}),
		nexusmcp.Tool(slow, nexusmcp.ToolOptions{Description: "Run a Workflow for an hour."}),
	)
	return service, err
}

// StartHandler starts a Worker for the contract service on taskQueue, and the
// KeeperWorkflow that the update operation targets. The returned function
// stops the Worker.
func StartHandler(ctx context.Context, c client.Client, taskQueue string) (func(), error) {
	service, err := newContractService(taskQueue)
	if err != nil {
		return nil, err
	}
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterNexusService(service)
	w.RegisterWorkflowWithOptions(EchoWorkflow, workflow.RegisterOptions{Name: contract.EchoWorkflowName})
	w.RegisterWorkflowWithOptions(KeeperWorkflow, workflow.RegisterOptions{Name: contract.KeeperWorkflowName})
	w.RegisterWorkflowWithOptions(SlowWorkflow, workflow.RegisterOptions{Name: contract.SlowWorkflowName})
	w.RegisterActivityWithOptions(EchoActivity, activity.RegisterOptions{Name: contract.EchoActivityName})
	if err := w.Start(); err != nil {
		return nil, err
	}
	_, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       keeperID(taskQueue),
		TaskQueue:                taskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, contract.KeeperWorkflowName)
	if err != nil {
		w.Stop()
		return nil, err
	}
	return w.Stop, nil
}
