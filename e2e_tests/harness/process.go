package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
)

const (
	// readyTimeout limits the start of a long-running fixture process.
	readyTimeout = 3 * time.Minute
	// readyPrefix starts the line that a long-running fixture process prints
	// when it is ready.
	readyPrefix = "READY"
)

// CreateEndpoint creates a Nexus endpoint named name that targets taskQueue.
// The name determines the task queue, so an endpoint that exists already from
// an earlier run on the same server is reused.
func (e *Env) CreateEndpoint(ctx context.Context, name, taskQueue string) error {
	_, err := e.Client.OperatorService().CreateNexusEndpoint(ctx, &operatorservice.CreateNexusEndpointRequest{
		Spec: &nexuspb.EndpointSpec{
			Name: name,
			Target: &nexuspb.EndpointTarget{
				Variant: &nexuspb.EndpointTarget_Worker_{Worker: &nexuspb.EndpointTarget_Worker{
					Namespace: Namespace,
					TaskQueue: taskQueue,
				}},
			},
		},
	})
	if _, ok := errors.AsType[*serviceerror.AlreadyExists](err); ok {
		return nil
	}
	return err
}

// command returns the command for a fixture program of lang. Python runs in
// the environment of the python package. TypeScript runs the compiled
// fixtures.
func (e *Env) command(ctx context.Context, lang, program string, args ...string) (*exec.Cmd, error) {
	var cmd *exec.Cmd
	switch lang {
	case "python":
		script := filepath.Join(e.Root, "e2e_tests", "fixtures", "python", program+".py")
		cmd = exec.CommandContext(ctx, "uv", append([]string{"run", "--project", filepath.Join(e.Root, "python"), "--locked", "python", script}, args...)...)
	case "typescript":
		script := filepath.Join(e.Root, "e2e_tests", "fixtures", "typescript", "lib", program+".js")
		cmd = exec.CommandContext(ctx, "node", append([]string{script}, args...)...)
	default:
		return nil, fmt.Errorf("no fixture programs for %q", lang)
	}
	cmd.Env = append(os.Environ(), "TEMPORAL_ADDRESS="+e.HostPort, "TEMPORAL_NAMESPACE="+Namespace)
	// uv and node can start child processes. Kill the whole process group,
	// and do not wait for pipes that a child keeps open.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 5 * time.Second
	return cmd, nil
}

// StartProcess starts a long-running fixture program and waits until it
// prints its READY line. It returns the rest of that line. The process stops
// with the environment. Its stderr goes to a log file.
func (e *Env) StartProcess(lang, program, name string, args ...string) (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd, err := e.command(ctx, lang, program, args...)
	if err != nil {
		cancel()
		return "", err
	}
	logFile, err := e.LogFile(name)
	if err != nil {
		cancel()
		return "", err
	}
	cmd.Stderr = logFile
	// The process exits when its stdin closes, so it also stops when the test
	// process dies.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return "", err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return "", err
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return "", err
	}
	e.OnStop(func() {
		_ = stdin.Close()
		cancel()
		_ = cmd.Wait()
		_ = logFile.Close()
	})

	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if rest, ok := strings.CutPrefix(line, readyPrefix); ok {
				ready <- strings.TrimSpace(rest)
				// Drain the rest of stdout, so the process does not block.
				for scanner.Scan() {
				}
				return
			}
		}
		close(ready)
	}()
	select {
	case rest, ok := <-ready:
		if !ok {
			return "", fmt.Errorf("%s exited before it was ready; see %s", name, filepath.Join(e.LogDir, name+".log"))
		}
		return rest, nil
	case <-time.After(readyTimeout):
		return "", fmt.Errorf("%s was not ready after %s; see %s", name, readyTimeout, filepath.Join(e.LogDir, name+".log"))
	}
}

// RunJSON runs a one-shot fixture program with input as JSON on stdin, and
// decodes its JSON stdout into out.
func (e *Env) RunJSON(ctx context.Context, lang, program string, input, out any) error {
	cmd, err := e.command(ctx, lang, program)
	if err != nil {
		return err
	}
	in, err := json.Marshal(input)
	if err != nil {
		return err
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdin = bytes.NewReader(in)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s %s: %w\nstderr:\n%s", lang, program, err, stderr.String())
	}
	if err := json.Unmarshal(stdout.Bytes(), out); err != nil {
		return errors.Join(fmt.Errorf("%s %s: decode output %q", lang, program, stdout.String()), err)
	}
	return nil
}
