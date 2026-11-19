package mcpwire_test

import (
	"encoding/json"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/temporalio/nexus-connector-mcp/go/inbound/internal/mcpwire"
)

func TestResultKeepsLargeIntegers(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{
		`{"id":9007199254740993,"nested":[9007199254740993]}`,
		`{"io.temporal/mcpCallToolResult":{"content":[{"type":"text","text":"kept"}],"structuredContent":{"id":9007199254740993},"isError":true}}`,
	} {
		var value any
		require.NoError(t, mcpwire.Decode([]byte(raw), &value))
		result, err := mcpwire.Result(value)
		require.NoError(t, err)
		encoded, err := json.Marshal(result)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `9007199254740993`)
		if result.IsError {
			require.Equal(t, "kept", result.Content[0].(*mcp.TextContent).Text)
		}
	}
}

func TestResultMapsValueKinds(t *testing.T) {
	t.Parallel()

	text, err := mcpwire.Result("hello")
	require.NoError(t, err)
	require.Equal(t, "hello", text.Content[0].(*mcp.TextContent).Text)
	require.Nil(t, text.StructuredContent)

	object, err := mcpwire.Result(map[string]any{"a": json.Number("1")})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"a": json.Number("1")}, object.StructuredContent)

	list, err := mcpwire.Result([]any{json.Number("1"), "b"})
	require.NoError(t, err)
	require.Equal(t, `[1,"b"]`, list.Content[0].(*mcp.TextContent).Text)

	empty, err := mcpwire.Result(nil)
	require.NoError(t, err)
	require.Equal(t, "", empty.Content[0].(*mcp.TextContent).Text)
}

func TestEnvelopeKeepsContentMetaAndErrorFlag(t *testing.T) {
	t.Parallel()

	result, err := mcpwire.Result(map[string]any{
		mcpwire.ResultEnvelopeKey: map[string]any{
			"content": []any{map[string]any{"type": "image", "data": "", "mimeType": "image/png"}},
			"isError": true,
			"_meta":   map[string]any{"trace": "id"},
		},
	})
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, "id", result.Meta["trace"])
	require.IsType(t, &mcp.ImageContent{}, result.Content[0])
}

func TestDecodeRejectsTrailingData(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{`{} {}`, `{} trailing`, `{"id":`} {
		var value any
		require.Error(t, mcpwire.Decode([]byte(raw), &value))
	}
}

func TestErrorSetsIsError(t *testing.T) {
	t.Parallel()

	result := mcpwire.Error("boom")
	require.True(t, result.IsError)
	require.Equal(t, "boom", result.Content[0].(*mcp.TextContent).Text)
}
