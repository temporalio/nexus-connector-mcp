// Package contract defines the e2e contract service. The Go, Python, and
// TypeScript handlers implement it with the same service name, operation
// names, and input and output types. Each operation is backed by a different
// kind of Nexus operation.
//
// TODO(long-nt-tran): add a test for signal-with-start when we support a
// Nexus-fronted signal-with-start operation.
//
// TODO(long-nt-tran): add a test for update-with-start when we support a
// Nexus-fronted update-with-start operation.
package contract

const (
	// Service is the Nexus service name of the contract service.
	Service = "contract"

	// Operations that echo their input. Each one uses a different kind of
	// Nexus operation.
	SyncEcho      = "sync_echo"
	WorkflowRun   = "workflow_run"
	StartWorkflow = "start_workflow"
	StartActivity = "start_activity"
	StartUpdate   = "start_update"

	// Fail fails the operation.
	Fail = "fail"
	// Slow starts a Workflow that runs for an hour, with the ID
	// SlowWorkflowID. Tests use it for tasks and cancellation.
	Slow = "slow"

	// Names of the Temporal primitives that back the operations. The output
	// kind reports which one ran.
	KindSync     = "sync"
	KindWorkflow = "workflow"
	KindActivity = "activity"
	KindUpdate   = "update"

	// Names that every language registers.
	EchoWorkflowName   = "EchoWorkflow"
	KeeperWorkflowName = "KeeperWorkflow"
	SlowWorkflowName   = "SlowWorkflow"
	EchoActivityName   = "echo_activity"
	EchoUpdateName     = "echo"
)

// Input is the input of every operation.
type Input struct {
	Message string `json:"message" jsonschema:"the message to echo"`
}

// Output is the output of every echo operation.
type Output struct {
	Message string `json:"message"`
	// Kind is the Temporal primitive that produced the output.
	Kind string `json:"kind"`
}

// EchoOperations are the operations that echo their input, in matrix order.
var EchoOperations = []string{SyncEcho, WorkflowRun, StartWorkflow, StartActivity, StartUpdate}

// Tools are the tool names that list_tools returns, in sorted order.
var Tools = []string{Fail, Slow, StartActivity, StartUpdate, StartWorkflow, SyncEcho, WorkflowRun}

// kinds maps an echo operation to the primitive that backs it.
var kinds = map[string]string{
	SyncEcho:      KindSync,
	WorkflowRun:   KindWorkflow,
	StartWorkflow: KindWorkflow,
	StartActivity: KindActivity,
	StartUpdate:   KindUpdate,
}

// SlowWorkflowID returns the ID of the SlowWorkflow that the slow operation
// starts for message. Tests read the Workflow by this ID.
func SlowWorkflowID(message string) string {
	return "slow-" + message
}

// Expected returns the output of an echo operation for message.
func Expected(operation, message string) Output {
	return Output{Message: message, Kind: kinds[operation]}
}
