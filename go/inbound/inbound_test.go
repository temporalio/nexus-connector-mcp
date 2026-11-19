package inbound_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/temporalio/nexus-connector-mcp/go/inbound"
)

func TestNewServerNotImplemented(t *testing.T) {
	t.Parallel()

	server, err := inbound.NewServer(inbound.Options{})
	require.Error(t, err)
	require.Nil(t, server)
}
