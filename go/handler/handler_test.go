package handler_test

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"

	"github.com/temporalio/nexus-connector-mcp/go/handler"
)

func TestRegisterNotImplemented(t *testing.T) {
	t.Parallel()

	err := handler.Register(nexus.NewService("test"))
	require.Error(t, err)
}
