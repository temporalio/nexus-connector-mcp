package outbound_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/nexus-connector-mcp/go/outbound"
)

func TestNewPluginNotImplemented(t *testing.T) {
	t.Parallel()

	plugin, err := outbound.NewPlugin(outbound.Options{})
	require.Error(t, err)
	require.Nil(t, plugin)
}
