//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/nexus-connector-mcp/e2e_tests/contract"
	"github.com/temporalio/nexus-connector-mcp/e2e_tests/harness"
)

// TestCrossLanguage calls a sync and an async operation of each handler
// language from each caller, and an operation that fails. Each caller lists
// the tools first.
func TestCrossLanguage(t *testing.T) {
	t.Parallel()

	for _, caller := range harness.Callers {
		for _, lang := range harness.Langs {
			for _, operation := range []string{contract.SyncEcho, contract.WorkflowRun, contract.Fail} {
				t.Run(caller+"/"+lang+"/"+operation, func(t *testing.T) {
					t.Parallel()
					endpoint, err := env.Handler(lang)
					require.NoError(t, err)
					ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
					defer cancel()

					message := "from " + caller + " to " + lang
					result, err := env.CallTool(ctx, caller, endpoint, contract.Service, operation, contract.Input{Message: message})
					require.NoError(t, err)
					require.Equal(t, contract.Tools, result.Tools)
					if operation == contract.Fail {
						require.True(t, result.IsError)
						require.Contains(t, result.Text, "failed: "+message)
						return
					}
					require.False(t, result.IsError, "tool error: %s", result.Text)
					var got contract.Output
					require.NoError(t, json.Unmarshal(result.StructuredContent, &got))
					require.Equal(t, contract.Expected(operation, message), got)
				})
			}
		}
	}
}
