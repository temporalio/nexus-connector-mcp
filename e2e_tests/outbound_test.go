//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/temporalio/nexus-connector-mcp/e2e_tests/contract"
	gofixtures "github.com/temporalio/nexus-connector-mcp/e2e_tests/fixtures/go"
	"github.com/temporalio/nexus-connector-mcp/e2e_tests/harness"
)

// upstreamTools are the tools that the upstream lists from the start.
var upstreamTools = []string{gofixtures.UpstreamBroken, gofixtures.UpstreamEcho, gofixtures.UpstreamToolError}

// TestOutbound calls each upstream tool through each proxy language from each
// caller.
func TestOutbound(t *testing.T) {
	t.Parallel()

	for _, caller := range harness.Callers {
		for _, proxy := range harness.ProxyLangs {
			for _, tool := range upstreamTools {
				t.Run(caller+"/"+proxy+"/"+tool, func(t *testing.T) {
					t.Parallel()
					endpoint, err := env.Proxy(proxy)
					require.NoError(t, err)
					ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
					defer cancel()

					message := "from " + caller + " through " + proxy
					result, err := env.CallTool(ctx, caller, endpoint, harness.UpstreamService, tool, contract.Input{Message: message})
					require.NoError(t, err)
					// Tests that add tools run in parallel, so the list can have more.
					require.Subset(t, result.Tools, upstreamTools)
					switch tool {
					case gofixtures.UpstreamEcho:
						require.False(t, result.IsError, "tool error: %s", result.Text)
						requireUpstreamEcho(t, result, message)
					case gofixtures.UpstreamToolError:
						require.True(t, result.IsError)
						require.Equal(t, "tool error: "+message, result.Text)
					case gofixtures.UpstreamBroken:
						require.True(t, result.IsError)
						require.Contains(t, result.Text, "UpstreamProtocolError")
					}
				})
			}
		}
	}
}

// TestOutboundToolNewUpstream calls a tool that the upstream adds after the
// proxy listed the tools. The proxy refreshes its tool list on the miss.
func TestOutboundToolNewUpstream(t *testing.T) {
	t.Parallel()

	for _, proxy := range harness.ProxyLangs {
		t.Run(proxy, func(t *testing.T) {
			t.Parallel()
			endpoint, err := env.Proxy(proxy)
			require.NoError(t, err)
			upstream, err := env.Upstream()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
			defer cancel()

			_, err = env.CallTool(ctx, "go-mcp", endpoint, harness.UpstreamService, gofixtures.UpstreamEcho, contract.Input{Message: "first"})
			require.NoError(t, err)
			late := "late_" + proxy + "_" + uuid.NewString()[:8]
			upstream.AddEcho(late)
			result, err := env.CallTool(ctx, "go-mcp", endpoint, harness.UpstreamService, late, contract.Input{Message: "late"})
			require.NoError(t, err)
			require.Contains(t, result.Tools, late)
			require.False(t, result.IsError, "tool error: %s", result.Text)
			requireUpstreamEcho(t, result, "late")
		})
	}
}

// TestOutboundUnknownTool calls a tool that the upstream does not list.
func TestOutboundUnknownTool(t *testing.T) {
	t.Parallel()

	for _, proxy := range harness.ProxyLangs {
		t.Run(proxy, func(t *testing.T) {
			t.Parallel()
			endpoint, err := env.Proxy(proxy)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), callTimeout)
			defer cancel()

			result, err := env.CallTool(ctx, "go-mcp", endpoint, harness.UpstreamService, "missing", contract.Input{Message: "x"})
			require.NoError(t, err)
			require.True(t, result.IsError)
		})
	}
}

// requireUpstreamEcho requires the structured output of an upstream echo.
func requireUpstreamEcho(t *testing.T, result harness.CallResult, message string) {
	t.Helper()
	var got contract.Output
	require.NoError(t, json.Unmarshal(result.StructuredContent, &got), "content: %s", result.Text)
	require.Equal(t, contract.Output{Message: message, Kind: gofixtures.KindUpstream}, got)
}
