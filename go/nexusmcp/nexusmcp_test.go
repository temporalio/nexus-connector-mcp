package nexusmcp_test

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporalnexus"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/temporalio/nexus-connector-mcp/go/nexusmcp"
)

type lookupInput struct {
	Topic string `json:"topic" jsonschema:"the topic to look up"`
}

type lookupOutput struct {
	Number int `json:"number"`
}

func lookupWorkflow(workflow.Context, lookupInput) (lookupOutput, error) {
	return lookupOutput{Number: 7}, nil
}

func lookupActivity(context.Context, lookupInput) (lookupOutput, error) {
	return lookupOutput{Number: 7}, nil
}

// asyncLookup is a plain Nexus operation that starts async. Its handler keeps
// the operation state.
type asyncLookup struct {
	nexus.UnimplementedOperation[lookupInput, lookupOutput]
}

func (*asyncLookup) Name() string { return "custom_async" }

func (*asyncLookup) Start(context.Context, lookupInput, nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[lookupOutput], error) {
	return &nexus.HandlerStartOperationResultAsync{OperationToken: "lookup-token"}, nil
}

func newTemporalOperation[I, O any](name string, start func(context.Context, temporalnexus.NexusClient, I, temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[O], error)) nexus.Operation[I, O] {
	return temporalnexus.MustNewTemporalOperation(temporalnexus.TemporalOperationOptions[I, O]{Name: name, Start: start})
}

var (
	lookup = nexus.NewSyncOperation("lookup", func(context.Context, lookupInput, nexus.StartOperationOptions) (lookupOutput, error) {
		return lookupOutput{Number: 7}, nil
	})
	ping = nexus.NewSyncOperation("ping", func(context.Context, nexus.NoValue, nexus.StartOperationOptions) (string, error) {
		return "pong", nil
	})
	hidden = nexus.NewSyncOperation("hidden", func(_ context.Context, in lookupInput, _ nexus.StartOperationOptions) (string, error) {
		return in.Topic, nil
	})

	// Async operations. Each kind backs the operation with a different
	// Temporal primitive.
	runWorkflow = temporalnexus.NewWorkflowRunOperation("run_workflow", lookupWorkflow,
		func(_ context.Context, _ lookupInput, opts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			return client.StartWorkflowOptions{ID: opts.RequestID}, nil
		})
	startWorkflow = newTemporalOperation("start_workflow",
		func(ctx context.Context, nc temporalnexus.NexusClient, in lookupInput, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[lookupOutput], error) {
			return temporalnexus.StartWorkflow(ctx, nc, client.StartWorkflowOptions{ID: opts.RequestID}, lookupWorkflow, in)
		})
	startActivity = newTemporalOperation("start_activity",
		func(ctx context.Context, nc temporalnexus.NexusClient, in lookupInput, opts temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[lookupOutput], error) {
			return temporalnexus.StartActivity(ctx, nc, client.StartActivityOptions{ID: opts.RequestID, TaskQueue: "lookup", StartToCloseTimeout: time.Minute}, lookupActivity, in)
		})
	startUpdate = newTemporalOperation("start_update",
		func(ctx context.Context, nc temporalnexus.NexusClient, in lookupInput, _ temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[lookupOutput], error) {
			return temporalnexus.StartUpdateWorkflow[lookupOutput](ctx, nc, client.UpdateWorkflowOptions{
				WorkflowID:   "lookup-workflow",
				UpdateName:   "lookup",
				Args:         []any{in},
				WaitForStage: client.WorkflowUpdateStageAccepted,
			})
		})
	// asyncToken takes a pointer input and returns a string, so its tool has
	// no output schema.
	asyncToken = newTemporalOperation("async_token",
		func(context.Context, temporalnexus.NexusClient, *lookupInput, temporalnexus.StartTemporalOperationOptions) (temporalnexus.TemporalOperationResult[string], error) {
			return temporalnexus.NewAsyncResult[string]("lookup-token"), nil
		})
	customAsync = &asyncLookup{}
)

// asyncTools are the async operations by tool name.
var asyncTools = map[string]nexus.RegisterableOperation{
	"run_workflow":   runWorkflow,
	"start_workflow": startWorkflow,
	"start_activity": startActivity,
	"start_update":   startUpdate,
	"async_token":    asyncToken,
	"custom_async":   customAsync,
}

func newService(t *testing.T) *nexus.Service {
	t.Helper()
	svc := nexus.NewService("lucky")
	require.NoError(t, nexusmcp.Register(svc,
		nexusmcp.Tool(lookup, nexusmcp.ToolOptions{
			Title:       "Look up a number",
			Description: "Look up a number now.",
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
			Meta:        map[string]any{"example.com/owner": "team"},
		}),
		nexusmcp.Tool(ping, nexusmcp.ToolOptions{}),
		nexusmcp.Tool(runWorkflow, nexusmcp.ToolOptions{Description: "Run a Workflow."}),
		nexusmcp.Tool(startWorkflow, nexusmcp.ToolOptions{Description: "Start a Workflow."}),
		nexusmcp.Tool(startActivity, nexusmcp.ToolOptions{Description: "Start an Activity."}),
		nexusmcp.Tool(startUpdate, nexusmcp.ToolOptions{Description: "Start a Workflow Update."}),
		nexusmcp.Tool(asyncToken, nexusmcp.ToolOptions{Description: "Return an operation token."}),
		nexusmcp.Tool(customAsync, nexusmcp.ToolOptions{Description: "Start a custom async operation."}),
	))
	require.NoError(t, svc.Register(hidden))
	return svc
}

// listTools calls list_tools from a Workflow through the Nexus test
// environment, as the Workflow client does, and returns the tools by name.
//
// The test environment runs the Workflow and the Nexus operation in memory, so
// the tests need no Worker and no Temporal server. A standalone Nexus
// operation needs both, so it belongs in an integration test.
func listTools(t *testing.T, svc *nexus.Service) map[string]map[string]any {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterNexusService(svc)
	env.ExecuteWorkflow(func(ctx workflow.Context) (json.RawMessage, error) {
		var raw json.RawMessage
		err := workflow.NewNexusClient("endpoint", svc.Name).
			ExecuteOperation(ctx, "list_tools", nil, workflow.NexusOperationOptions{}).
			Get(ctx, &raw)
		return raw, err
	})
	require.NoError(t, env.GetWorkflowError())
	var raw json.RawMessage
	require.NoError(t, env.GetWorkflowResult(&raw))
	// The result has only the tools. MCP SDKs reject the empty cacheScope that
	// mcp.ListToolsResult encodes.
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	require.Equal(t, []string{"tools"}, slices.Collect(maps.Keys(fields)))

	var manifest struct {
		Tools []map[string]any `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(raw, &manifest))
	byName := map[string]map[string]any{}
	for _, tool := range manifest.Tools {
		byName[tool["name"].(string)] = tool
	}
	return byName
}

func TestListToolsListsOnlyMarkedOperations(t *testing.T) {
	t.Parallel()

	tools := listTools(t, newService(t))
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, name)
	}
	require.ElementsMatch(t, []string{
		"lookup", "ping", "run_workflow", "start_workflow", "start_activity", "start_update", "async_token", "custom_async",
	}, names)
}

func TestToolDefinitionFields(t *testing.T) {
	t.Parallel()

	tools := listTools(t, newService(t))

	lookup := tools["lookup"]
	require.Equal(t, "Look up a number", lookup["title"])
	require.Equal(t, "Look up a number now.", lookup["description"])
	require.Equal(t, true, lookup["annotations"].(map[string]any)["readOnlyHint"])
	require.Equal(t, map[string]any{"example.com/owner": "team"}, lookup["_meta"])
	input := lookup["inputSchema"].(map[string]any)
	require.Equal(t, "object", input["type"])
	require.Equal(t, []any{"topic"}, input["required"])
	require.Equal(t, "the topic to look up", input["properties"].(map[string]any)["topic"].(map[string]any)["description"])
	require.Equal(t, "object", lookup["outputSchema"].(map[string]any)["type"])

	// An operation without input has an empty-object schema.
	require.Equal(t, map[string]any{"type": "object", "properties": map[string]any{}}, tools["ping"]["inputSchema"])
}

// TestAsyncTools checks each kind of async operation. The tool definition
// comes from the operation types, and the service keeps the operation as
// given, so the async start and cancel stay with the Temporal SDK.
func TestAsyncTools(t *testing.T) {
	t.Parallel()

	svc := newService(t)
	tools := listTools(t, svc)
	for name, op := range asyncTools {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			tool := tools[name]
			require.NotNil(t, tool)
			input := tool["inputSchema"].(map[string]any)
			require.Equal(t, "object", input["type"])
			require.Equal(t, []any{"topic"}, input["required"])
			if name == "async_token" {
				// A pointer input still has an object schema. A string output
				// becomes text content, so it has no output schema.
				require.NotContains(t, tool, "outputSchema")
			} else {
				output := tool["outputSchema"].(map[string]any)
				require.Equal(t, []any{"number"}, output["required"])
			}
			require.Same(t, op, svc.Operation(name))
		})
	}
}

func TestCustomAsyncToolStartsAsync(t *testing.T) {
	t.Parallel()

	op := newService(t).Operation("custom_async").(nexus.Operation[lookupInput, lookupOutput])
	result, err := op.Start(t.Context(), lookupInput{Topic: "cats"}, nexus.StartOperationOptions{})
	require.NoError(t, err)
	require.Equal(t, &nexus.HandlerStartOperationResultAsync{OperationToken: "lookup-token"}, result)
}

func TestRegisterRejectsInvalidTools(t *testing.T) {
	t.Parallel()

	stringInput := nexus.NewSyncOperation("echo", func(_ context.Context, in string, _ nexus.StartOperationOptions) (string, error) {
		return in, nil
	})
	reserved := nexus.NewSyncOperation("__temporal_op", func(_ context.Context, in lookupInput, _ nexus.StartOperationOptions) (string, error) {
		return in.Topic, nil
	})
	listToolsOp := nexus.NewSyncOperation("list_tools", func(_ context.Context, in lookupInput, _ nexus.StartOperationOptions) (string, error) {
		return in.Topic, nil
	})

	for name, test := range map[string]struct {
		tool    nexusmcp.MarkedTool
		message string
	}{
		"non-object input": {nexusmcp.Tool(stringInput, nexusmcp.ToolOptions{}), "must be a struct"},
		"reserved name":    {nexusmcp.Tool(reserved, nexusmcp.ToolOptions{}), "not reserved"},
		"list_tools name":  {nexusmcp.Tool(listToolsOp, nexusmcp.ToolOptions{}), "not reserved"},
		"nil operation":    {nexusmcp.Tool[lookupInput, string](nil, nexusmcp.ToolOptions{}), "needs an operation"},
		"not from Tool":    {nil, "results of Tool"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := nexusmcp.Register(nexus.NewService("svc"), test.tool)
			require.ErrorContains(t, err, test.message)
		})
	}

	require.ErrorContains(t, nexusmcp.Register(nil), "needs a service")
}

func TestRegisterRejectsDuplicateOperations(t *testing.T) {
	t.Parallel()

	svc := nexus.NewService("svc")
	require.NoError(t, svc.Register(lookup))
	err := nexusmcp.Register(svc, nexusmcp.Tool(lookup, nexusmcp.ToolOptions{}))
	require.Error(t, err)
}
