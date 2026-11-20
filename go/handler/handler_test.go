package handler_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/temporalio/nexus-connector-mcp/go/handler"
)

type greetInput struct {
	Name string `json:"name" jsonschema:"who to greet"`
}

type greetOutput struct {
	Message string `json:"message"`
}

var (
	greet = nexus.NewSyncOperation("greet", func(_ context.Context, in greetInput, _ nexus.StartOperationOptions) (greetOutput, error) {
		return greetOutput{Message: "Hello, " + in.Name + "!"}, nil
	})
	shout = nexus.NewSyncOperation("shout", func(_ context.Context, in *greetInput, _ nexus.StartOperationOptions) (string, error) {
		return "HELLO, " + in.Name + "!", nil
	})
	ping = nexus.NewSyncOperation("ping", func(context.Context, nexus.NoValue, nexus.StartOperationOptions) (string, error) {
		return "pong", nil
	})
	hidden = nexus.NewSyncOperation("hidden", func(_ context.Context, in greetInput, _ nexus.StartOperationOptions) (string, error) {
		return in.Name, nil
	})
)

func newService(t *testing.T) *nexus.Service {
	t.Helper()
	svc := nexus.NewService("greeter")
	require.NoError(t, handler.Register(svc,
		handler.Tool(greet, handler.ToolOptions{
			Title:       "Greet",
			Description: "Greet someone.",
			Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
			Meta:        map[string]any{"example.com/owner": "team"},
		}),
		handler.Tool(shout, handler.ToolOptions{Description: "Shout a greeting."}),
		handler.Tool(ping, handler.ToolOptions{}),
	))
	require.NoError(t, svc.Register(hidden))
	return svc
}

// listTools calls list_tools from a Workflow through the Nexus test
// environment, as the Workflow client does.
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
	require.Len(t, tools, 3)
	require.Contains(t, tools, "greet")
	require.Contains(t, tools, "shout")
	require.Contains(t, tools, "ping")
}

func TestToolDefinitionFields(t *testing.T) {
	t.Parallel()

	tools := listTools(t, newService(t))

	greet := tools["greet"]
	require.Equal(t, "Greet", greet["title"])
	require.Equal(t, "Greet someone.", greet["description"])
	require.Equal(t, true, greet["annotations"].(map[string]any)["readOnlyHint"])
	require.Equal(t, map[string]any{"example.com/owner": "team"}, greet["_meta"])
	input := greet["inputSchema"].(map[string]any)
	require.Equal(t, "object", input["type"])
	require.Equal(t, []any{"name"}, input["required"])
	require.Equal(t, "who to greet", input["properties"].(map[string]any)["name"].(map[string]any)["description"])
	require.Equal(t, "object", greet["outputSchema"].(map[string]any)["type"])

	// A pointer input still has an object schema. A string output becomes
	// text content, so it has no output schema.
	shout := tools["shout"]
	require.Equal(t, "object", shout["inputSchema"].(map[string]any)["type"])
	require.NotContains(t, shout, "outputSchema")

	// An operation without input has an empty-object schema.
	require.Equal(t, map[string]any{"type": "object", "properties": map[string]any{}}, tools["ping"]["inputSchema"])
}

func TestRegisterRejectsInvalidTools(t *testing.T) {
	t.Parallel()

	stringInput := nexus.NewSyncOperation("echo", func(_ context.Context, in string, _ nexus.StartOperationOptions) (string, error) {
		return in, nil
	})
	reserved := nexus.NewSyncOperation("__temporal_op", func(_ context.Context, in greetInput, _ nexus.StartOperationOptions) (string, error) {
		return in.Name, nil
	})
	listToolsOp := nexus.NewSyncOperation("list_tools", func(_ context.Context, in greetInput, _ nexus.StartOperationOptions) (string, error) {
		return in.Name, nil
	})

	for name, test := range map[string]struct {
		tool    handler.MarkedTool
		message string
	}{
		"non-object input": {handler.Tool(stringInput, handler.ToolOptions{}), "must be a struct"},
		"reserved name":    {handler.Tool(reserved, handler.ToolOptions{}), "not reserved"},
		"list_tools name":  {handler.Tool(listToolsOp, handler.ToolOptions{}), "not reserved"},
		"nil operation":    {handler.Tool[greetInput, string](nil, handler.ToolOptions{}), "needs an operation"},
		"not from Tool":    {nil, "results of Tool"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := handler.Register(nexus.NewService("svc"), test.tool)
			require.ErrorContains(t, err, test.message)
		})
	}

	require.ErrorContains(t, handler.Register(nil), "needs a service")
}

func TestRegisterRejectsDuplicateOperations(t *testing.T) {
	t.Parallel()

	svc := nexus.NewService("svc")
	require.NoError(t, svc.Register(greet))
	err := handler.Register(svc, handler.Tool(greet, handler.ToolOptions{}))
	require.Error(t, err)
}
