package inbound

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWaitSeparatesReadErrorsFromOperationFailures(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		err     error
		failure bool
	}{
		{serviceerror.NewUnavailable("retry read"), false},
		{status.Error(codes.PermissionDenied, "read denied"), false},
		{temporal.NewApplicationError("tool failed", "ToolError"), true},
	} {
		fake := newFakeClient()
		fake.readError = test.err
		out, err := wait(t.Context(), &fakeHandle{parent: fake, operation: "lookup"})
		if test.failure {
			require.NoError(t, err)
			require.ErrorIs(t, out.failure, test.err)
		} else {
			require.ErrorIs(t, err, test.err)
		}
	}
}

// pollDeadlineHandle ends two reads on an RPC deadline and then returns a
// result. An SDK poll RPC can end this way while the caller still waits.
type pollDeadlineHandle struct {
	client.NexusOperationHandle

	reads int
}

func (h *pollDeadlineHandle) Get(_ context.Context, out any) error {
	h.reads++
	if h.reads < 3 {
		return status.Error(codes.DeadlineExceeded, "poll deadline")
	}
	return json.Unmarshal([]byte(`"done"`), out)
}

func TestWaitReadsAgainAfterRPCDeadline(t *testing.T) {
	t.Parallel()

	out, err := wait(t.Context(), &pollDeadlineHandle{})
	require.NoError(t, err)
	require.JSONEq(t, `"done"`, string(out.value))
}
