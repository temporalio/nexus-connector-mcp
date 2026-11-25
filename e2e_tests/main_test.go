//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/temporalio/nexus-connector-mcp/e2e_tests/harness"
)

// callTimeout limits one tool call, so a broken path fails instead of
// hanging.
const callTimeout = time.Minute

// env is the shared environment of every e2e test.
var env *harness.Env

func TestMain(m *testing.M) {
	var err error
	env, err = harness.Start(context.Background())
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e:", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "e2e: logs in", env.LogDir)
	code := m.Run()
	env.Stop()
	os.Exit(code)
}
