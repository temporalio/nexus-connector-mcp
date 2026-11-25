package mcpwire_test

import (
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/temporalio/nexus-connector-mcp/go/inbound/internal/mcpwire"
)

func TestTaskShapes(t *testing.T) {
	t.Parallel()

	require.True(t, mcpwire.ClientSupportsTasks(mcp.Meta{mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{mcpwire.TasksExtension: map[string]any{}}}}))
	for _, meta := range []mcp.Meta{
		nil,
		{mcp.MetaKeyClientCapabilities: map[string]any{}},
		{mcp.MetaKeyClientCapabilities: map[string]any{"extensions": map[string]any{"io.example/other": map[string]any{}}}},
	} {
		require.False(t, mcpwire.ClientSupportsTasks(meta))
	}

	encoded, err := json.Marshal(mcpwire.NewCreateTaskResult("id"))
	require.NoError(t, err)
	for _, want := range []string{`"resultType":"task"`, `"taskId":"id"`, `"status":"working"`, `"ttlMs":null`, `"pollIntervalMs":2000`, `"createdAt"`, `"lastUpdatedAt"`} {
		require.Contains(t, string(encoded), want)
	}

	require.EqualValues(t, -32021, mcpwire.MissingTasksCapability.Code)
	require.JSONEq(t, `{"requiredCapabilities":{"extensions":{"io.modelcontextprotocol/tasks":{}}}}`, string(mcpwire.MissingTasksCapability.Data))
}

// TestTaskMethodsOverMCPClient runs tasks/get, tasks/cancel, and tasks/update
// through the MCP SDK client and server over an in-memory transport.
