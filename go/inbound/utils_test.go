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

func TestCheckOperationID(t *testing.T) {
	t.Parallel()

	alice := ownerHash("https://idp.example.com#alice")
	bob := ownerHash("https://idp.example.com#bob")
	random := strings.Repeat("a", 2*requestIDBytes)
	id := "mcp-inbound-http-stateless-svc-ep-op-" + alice + "-" + random
	require.NoError(t, checkOperationID(id, alice))

	for _, owner := range []string{bob, ownerHash("https://other.example.com#alice")} {
		require.ErrorIs(t, checkOperationID(id, owner), errInvalidOperationID)
	}
	for _, malformed := range []string{
		"",
		"job",
		"mcp-inbound-" + random,
		"mcp-inbound-" + bob + "-" + random,
		id + "x",
		id + ".extra",
		"mcp-inbound-" + strings.Repeat("x", maxOperationIDBytes),
	} {
		require.ErrorIs(t, checkOperationID(malformed, alice), errInvalidOperationID, malformed)
	}

	// Without a verified caller, the ID needs only the prefix and the suffix.
	require.NoError(t, checkOperationID("mcp-inbound-stdio-stateful-svc-ep-op-"+random, ""))
	require.ErrorIs(t, checkOperationID("mcp-inbound-stdio-stateful-svc-ep-op-"+strings.Repeat("z", 2*requestIDBytes), ""), errInvalidOperationID)
	// An ID of the same shape from another workload is not a task.
	require.ErrorIs(t, checkOperationID("other-workload-"+random, ""), errInvalidOperationID)

	// Operation IDs that the connector generates pass the check.
	for _, owner := range []string{"", alice} {
		require.NoError(t, checkOperationID(newOperationID("mcp-inbound-http-stateless-svc-ep-op", owner), owner))
		require.NoError(t, checkOperationID(newOperationID("mcp-inbound-"+strings.Repeat("long", 300), owner), owner))
	}
}
