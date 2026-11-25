// Package harness runs the e2e environment: the Temporal dev server, Nexus
// endpoints, and the fixture processes of each language.
package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/testsuite"
)

const (
	// DevServerVersion is the Temporal CLI version of the dev server. The
	// Temporal SDK repositories run their integration tests on this version.
	DevServerVersion = "v1.9.4-time-skipping"
	// Namespace is the namespace of every Worker, connector, and caller.
	Namespace = "default"
)

// devServerArgs turn on the features that the connectors use.
var devServerArgs = []string{
	// Inbound: each tool call is a standalone Nexus operation.
	"--dynamic-config-value", "nexusoperation.enableStandalone=true",
	// Outbound: each tool call is a Standalone Activity, and the Activity
	// completes its Nexus operation with a callback.
	"--dynamic-config-value", "activity.enableStandalone=true",
	"--dynamic-config-value", "frontend.activityAPIsEnabled=true",
	"--dynamic-config-value", "activity.enableCallbacks=true",
	// Update-backed Nexus operations complete with a callback.
	"--dynamic-config-value", "history.enableUpdateCallbacks=true",
	// The standalone features run on CHASM.
	"--dynamic-config-value", "history.enableChasm=true",
	"--dynamic-config-value", "history.enableCHASMCallbacks=true",
	"--dynamic-config-value", "history.enableTransitionHistory=true",
	"--dynamic-config-value", `nexusoperation.enabledCallbackKinds=["nexusHandler"]`,
	"--dynamic-config-value", "component.nexusoperations.recordCancelRequestCompletionEvents=true",
	// A new Nexus endpoint is usable at once.
	"--dynamic-config-value", `system.refreshNexusEndpointsMinWait="0s"`,
}

// Env is the shared e2e environment.
type Env struct {
	// Client is connected to the dev server.
	Client client.Client
	// HostPort is the frontend address of the dev server.
	HostPort string
	// Root is the repository root.
	Root string
	// LogDir holds the logs of the dev server and the fixture processes.
	LogDir string

	server *testsuite.DevServer

	mu       sync.Mutex
	fixtures map[string]*fixture
	stops    []func()
}

type fixture struct {
	once  sync.Once
	value any
	err   error
}

// Start starts the dev server, or connects to TEMPORAL_ADDRESS when it is
// set.
func Start(ctx context.Context) (*Env, error) {
	_, file, _, _ := runtime.Caller(0)
	env := &Env{
		Root:     filepath.Dir(filepath.Dir(filepath.Dir(file))),
		LogDir:   os.Getenv("E2E_LOG_DIR"),
		fixtures: map[string]*fixture{},
	}
	if env.LogDir == "" {
		dir, err := os.MkdirTemp("", "nexus-mcp-e2e-")
		if err != nil {
			return nil, err
		}
		env.LogDir = dir
	}
	if err := os.MkdirAll(env.LogDir, 0o755); err != nil {
		return nil, err
	}
	serverLog, err := os.Create(filepath.Join(env.LogDir, "temporal-dev-server.log"))
	if err != nil {
		return nil, err
	}
	logger := sdklog.NewStructuredLogger(slog.New(slog.NewTextHandler(serverLog, &slog.HandlerOptions{Level: slog.LevelWarn})))

	if address := os.Getenv("TEMPORAL_ADDRESS"); address != "" {
		env.HostPort = address
		env.Client, err = client.DialContext(ctx, client.Options{HostPort: address, Namespace: Namespace, Logger: logger})
		return env, err
	}
	env.server, err = testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		CachedDownload: testsuite.CachedDownload{Version: DevServerVersion},
		ClientOptions:  &client.Options{Namespace: Namespace, Logger: logger},
		LogLevel:       "warn",
		ExtraArgs:      devServerArgs,
		Stdout:         serverLog,
		Stderr:         serverLog,
	})
	if err != nil {
		return nil, fmt.Errorf("start dev server: %w", err)
	}
	env.Client = env.server.Client()
	env.HostPort = env.server.FrontendHostPort()
	return env, nil
}

// Stop stops the fixtures, then the dev server.
func (e *Env) Stop() {
	e.mu.Lock()
	stops := e.stops
	e.stops = nil
	e.mu.Unlock()
	for i := len(stops) - 1; i >= 0; i-- {
		stops[i]()
	}
	if e.server != nil {
		_ = e.server.Stop()
	} else if e.Client != nil {
		e.Client.Close()
	}
}

// OnStop runs stop when the environment stops.
func (e *Env) OnStop(stop func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stops = append(e.stops, stop)
}

// Fixture returns the fixture named key. The first call starts it with
// start. Later calls return the same value, so parallel tests share it.
func Fixture[T any](e *Env, key string, start func() (T, error)) (T, error) {
	e.mu.Lock()
	f, ok := e.fixtures[key]
	if !ok {
		f = &fixture{}
		e.fixtures[key] = f
	}
	e.mu.Unlock()
	f.once.Do(func() { f.value, f.err = start() })
	if f.err != nil {
		var zero T
		return zero, fmt.Errorf("fixture %s: %w", key, f.err)
	}
	return f.value.(T), nil
}

// LogFile creates a log file for name in LogDir.
func (e *Env) LogFile(name string) (io.WriteCloser, error) {
	return os.Create(filepath.Join(e.LogDir, name+".log"))
}
