package inbound

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationIDs(t *testing.T) {
	t.Parallel()

	require.Regexp(t, `^mcp-inbound-[0-9a-f]{32}$`, newOperationID(idPrefix, ""))
	require.NotEqual(t, newOperationID(idPrefix, ""), newOperationID(idPrefix, ""))
	owner := strings.Repeat("cd", 32)
	for _, prefix := range []string{"mcp-inbound-http-stateless-weather-ep-get_weather", "mcp-inbound-" + strings.Repeat("long", 1000)} {
		id := newOperationID(prefix, owner)
		require.LessOrEqual(t, len(id), maxIDPrefixBytes+len(owner)+34)
		require.True(t, strings.HasPrefix(id, "mcp-inbound-"))
		require.Regexp(t, "-"+owner+"-[0-9a-f]{32}$", id)
	}
	// Two long prefixes that share the cut part keep distinct IDs.
	a := newOperationID("mcp-inbound-"+strings.Repeat("x", 1000)+"a", "")
	b := newOperationID("mcp-inbound-"+strings.Repeat("x", 1000)+"b", "")
	require.NotEqual(t, a[:len(a)-33], b[:len(b)-33])
}
