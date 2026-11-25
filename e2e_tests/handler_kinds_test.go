//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-mcp/go/inbound"

	"github.com/temporalio/nexus-connector-mcp/e2e_tests/contract"
	"github.com/temporalio/nexus-connector-mcp/e2e_tests/harness"
)

// TestHandlerKinds calls each echo operation of each handler language through
// the inbound connector.
func TestHandlerKinds(t *testing.T) {
	t.Parallel()

	for _, lang := range harness.Langs {
		for _, operation := range contract.EchoOperations {
			t.Run(lang+"/"+operation, func(t *testing.T) {
				t.Parallel()
				endpoint, err := env.Handler(lang)
				require.NoError(t, err)
				url, err := env.Connector(harness.ConnectorOptions{Endpoint: endpoint, Service: contract.Service, ReturnTaskAfter: inbound.NeverReturnTask})
				require.NoError(t, err)
				session, err := harness.ConnectMCP(t.Context(), url)
				require.NoError(t, err)
				defer func() { _ = session.Close() }()

				ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
				defer cancel()
				result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: operation, Arguments: contract.Input{Message: "hi " + lang}})
				require.NoError(t, err)
				require.False(t, result.IsError, "tool error: %v", result.Content)
				require.Equal(t, contract.Expected(operation, "hi "+lang), structured(t, result))
			})
		}
	}
}

// structured decodes the structured content of result.
func structured(t *testing.T, result *mcp.CallToolResult) contract.Output {
	t.Helper()
	raw, err := json.Marshal(result.StructuredContent)
	require.NoError(t, err)
	var out contract.Output
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}
