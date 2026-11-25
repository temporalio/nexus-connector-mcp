//go:build e2e

package e2e

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-mcp/go/inbound"

	"github.com/temporalio/nexus-connector-mcp/e2e_tests/contract"
	"github.com/temporalio/nexus-connector-mcp/e2e_tests/harness"
)

// schemaCore is the part of a JSON schema that every language generates the
// same way from the contract types. The languages differ in the other
// keywords: additionalProperties (Go sets it on input and output, TypeScript
// on output only, Python on neither), title (Python), $schema (TypeScript),
// and descriptions (each language takes them from a different place).
type schemaCore struct {
	Type       string            `json:"type"`
	Properties map[string]string `json:"properties"`
	Required   []string          `json:"required"`
}

// toolCore is the part of a tool definition that every language generates the
// same way.
type toolCore struct {
	Name         string
	Description  string
	InputSchema  schemaCore
	OutputSchema schemaCore
}

// core returns the schema core of a JSON schema.
func core(t *testing.T, schema any) schemaCore {
	t.Helper()
	raw, err := json.Marshal(schema)
	require.NoError(t, err)
	var full struct {
		Type       string `json:"type"`
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	require.NoError(t, json.Unmarshal(raw, &full))
	out := schemaCore{Type: full.Type, Properties: map[string]string{}, Required: slices.Sorted(slices.Values(full.Required))}
	for name, property := range full.Properties {
		out.Properties[name] = property.Type
	}
	return out
}

// TestSchemaParity lists the tools of each handler language and requires the
// same tool names, descriptions, and schema cores.
func TestSchemaParity(t *testing.T) {
	t.Parallel()

	tools := map[string][]toolCore{}
	for _, lang := range harness.Langs {
		endpoint, err := env.Handler(lang)
		require.NoError(t, err)
		url, err := env.Connector(harness.ConnectorOptions{Endpoint: endpoint, Service: contract.Service, ReturnTaskAfter: inbound.NeverReturnTask})
		require.NoError(t, err)
		session, err := harness.ConnectMCP(t.Context(), url)
		require.NoError(t, err)
		listed, err := session.ListTools(t.Context(), nil)
		require.NoError(t, err)
		_ = session.Close()
		for _, tool := range listed.Tools {
			tools[lang] = append(tools[lang], toolCore{
				Name:         tool.Name,
				Description:  tool.Description,
				InputSchema:  core(t, tool.InputSchema),
				OutputSchema: core(t, tool.OutputSchema),
			})
		}
		slices.SortFunc(tools[lang], func(a, b toolCore) int { return strings.Compare(a.Name, b.Name) })
	}
	for _, lang := range harness.Langs[1:] {
		require.Equal(t, tools[harness.Langs[0]], tools[lang], "%s and %s differ", harness.Langs[0], lang)
	}
}
