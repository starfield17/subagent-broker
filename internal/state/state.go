package state

import "fmt"

type Process string

const (
	ProcessQueued   Process = "queued"
	ProcessStarting Process = "starting"
	ProcessAlive    Process = "alive"
	ProcessExited   Process = "exited"
	ProcessOrphaned Process = "orphaned"
	ProcessUnknown  Process = "unknown"
)

type Protocol string

const (
	ProtocolInitializing      Protocol = "initializing"
	ProtocolThinking          Protocol = "thinking"
	ProtocolToolRunning       Protocol = "tool_running"
	ProtocolStreaming         Protocol = "streaming"
	ProtocolRetrying          Protocol = "retrying"
	ProtocolWaitingPermission Protocol = "waiting_permission"
	ProtocolWaitingUser       Protocol = "waiting_user"
	ProtocolWaitingScope      Protocol = "waiting_scope"
	ProtocolIdleBetweenTurns  Protocol = "idle_between_turns"
	ProtocolClosing           Protocol = "closing"
	ProtocolClosed            Protocol = "closed"
	ProtocolError             Protocol = "protocol_error"
)

type Progress string

const (
	ProgressActive         Progress = "active"
	ProgressQuiet          Progress = "quiet"
	ProgressSuspectedStall Progress = "suspected_stall"
	ProgressStalled        Progress = "stalled"
	ProgressUnknown        Progress = "unknown"
)

type Task string

const (
	TaskPlanned            Task = "planned"
	TaskRunning            Task = "running"
	TaskBlocked            Task = "blocked"
	TaskReportedComplete   Task = "reported_complete"
	TaskVerifying          Task = "verifying"
	TaskVerifiedSuccess    Task = "verified_success"
	TaskVerifiedPartial    Task = "verified_partial"
	TaskVerificationFailed Task = "verification_failed"
	TaskFailed             Task = "failed"
	TaskCancelled          Task = "cancelled"
)

type Dimensions struct {
	Process  Process  `json:"process"`
	Protocol Protocol `json:"protocol"`
	Progress Progress `json:"progress"`
	Task     Task     `json:"task"`
}

type TransitionError struct {
	Dimension string
	From      string
	To        string
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("invalid %s transition: %s -> %s", e.Dimension, e.From, e.To)
}

var processTransitions = map[Process]map[Process]bool{
	ProcessQueued:   {ProcessStarting: true, ProcessUnknown: true},
	ProcessStarting: {ProcessAlive: true, ProcessExited: true, ProcessUnknown: true},
	ProcessAlive:    {ProcessExited: true, ProcessOrphaned: true, ProcessUnknown: true},
	ProcessExited:   {ProcessUnknown: true},
	ProcessOrphaned: {ProcessExited: true, ProcessUnknown: true},
	ProcessUnknown:  {ProcessQueued: true, ProcessStarting: true, ProcessAlive: true, ProcessExited: true, ProcessOrphaned: true},
}

var protocolTransitions = map[Protocol]map[Protocol]bool{
	ProtocolInitializing: {
		ProtocolThinking: true, ProtocolToolRunning: true, ProtocolStreaming: true,
		ProtocolWaitingPermission: true, ProtocolWaitingUser: true, ProtocolWaitingScope: true,
		ProtocolIdleBetweenTurns: true, ProtocolClosing: true, ProtocolError: true,
	},
	ProtocolThinking: {
		ProtocolToolRunning: true, ProtocolStreaming: true, ProtocolRetrying: true,
		ProtocolWaitingPermission: true, ProtocolWaitingUser: true, ProtocolWaitingScope: true,
		ProtocolIdleBetweenTurns: true, ProtocolClosing: true, ProtocolError: true,
	},
	ProtocolToolRunning: {
		ProtocolThinking: true, ProtocolStreaming: true, ProtocolRetrying: true,
		ProtocolWaitingPermission: true, ProtocolWaitingUser: true, ProtocolWaitingScope: true,
		ProtocolIdleBetweenTurns: true, ProtocolClosing: true, ProtocolError: true,
	},
	ProtocolStreaming: {
		ProtocolThinking: true, ProtocolToolRunning: true, ProtocolRetrying: true,
		ProtocolWaitingPermission: true, ProtocolWaitingUser: true, ProtocolWaitingScope: true,
		ProtocolIdleBetweenTurns: true, ProtocolClosing: true, ProtocolError: true,
	},
	ProtocolRetrying: {
		ProtocolThinking: true, ProtocolToolRunning: true, ProtocolStreaming: true,
		ProtocolWaitingPermission: true, ProtocolWaitingUser: true, ProtocolWaitingScope: true,
		ProtocolIdleBetweenTurns: true, ProtocolClosing: true, ProtocolError: true,
	},
	ProtocolWaitingPermission: {ProtocolThinking: true, ProtocolToolRunning: true, ProtocolStreaming: true, ProtocolIdleBetweenTurns: true, ProtocolClosing: true, ProtocolError: true},
	ProtocolWaitingUser:       {ProtocolThinking: true, ProtocolToolRunning: true, ProtocolStreaming: true, ProtocolIdleBetweenTurns: true, ProtocolClosing: true, ProtocolError: true},
	ProtocolWaitingScope:      {ProtocolThinking: true, ProtocolToolRunning: true, ProtocolStreaming: true, ProtocolIdleBetweenTurns: true, ProtocolClosing: true, ProtocolError: true},
	ProtocolIdleBetweenTurns: {
		ProtocolThinking: true, ProtocolToolRunning: true, ProtocolStreaming: true,
		ProtocolWaitingPermission: true, ProtocolWaitingUser: true, ProtocolWaitingScope: true,
		ProtocolClosing: true, ProtocolError: true,
	},
	ProtocolClosing: {ProtocolClosed: true, ProtocolError: true},
	ProtocolClosed:  {},
	ProtocolError:   {ProtocolClosing: true, ProtocolClosed: true},
}

var progressTransitions = map[Progress]map[Progress]bool{
	ProgressActive:         {ProgressQuiet: true, ProgressUnknown: true},
	ProgressQuiet:          {ProgressActive: true, ProgressSuspectedStall: true, ProgressUnknown: true},
	ProgressSuspectedStall: {ProgressActive: true, ProgressQuiet: true, ProgressStalled: true, ProgressUnknown: true},
	ProgressStalled:        {ProgressActive: true, ProgressUnknown: true},
	ProgressUnknown:        {ProgressActive: true, ProgressQuiet: true, ProgressSuspectedStall: true, ProgressStalled: true},
}

var taskTransitions = map[Task]map[Task]bool{
	TaskPlanned:            {TaskRunning: true, TaskBlocked: true, TaskFailed: true, TaskCancelled: true},
	TaskRunning:            {TaskBlocked: true, TaskReportedComplete: true, TaskFailed: true, TaskCancelled: true},
	TaskBlocked:            {TaskRunning: true, TaskReportedComplete: true, TaskFailed: true, TaskCancelled: true},
	TaskReportedComplete:   {TaskVerifying: true, TaskFailed: true, TaskCancelled: true},
	TaskVerifying:          {TaskVerifiedSuccess: true, TaskVerifiedPartial: true, TaskVerificationFailed: true, TaskCancelled: true},
	TaskVerificationFailed: {TaskRunning: true, TaskFailed: true, TaskCancelled: true},
	TaskVerifiedSuccess:    {},
	TaskVerifiedPartial:    {TaskRunning: true},
	TaskFailed:             {},
	TaskCancelled:          {},
}

func ValidateProcessTransition(from, to Process) error {
	if from == to {
		return nil
	}
	if processTransitions[from][to] {
		return nil
	}
	return &TransitionError{Dimension: "process", From: string(from), To: string(to)}
}

func ValidateProtocolTransition(from, to Protocol) error {
	if from == to {
		return nil
	}
	if protocolTransitions[from][to] {
		return nil
	}
	return &TransitionError{Dimension: "protocol", From: string(from), To: string(to)}
}

func ValidateProgressTransition(from, to Progress) error {
	if from == to {
		return nil
	}
	if progressTransitions[from][to] {
		return nil
	}
	return &TransitionError{Dimension: "progress", From: string(from), To: string(to)}
}

func ValidateTaskTransition(from, to Task) error {
	if from == to {
		return nil
	}
	if taskTransitions[from][to] {
		return nil
	}
	return &TransitionError{Dimension: "task", From: string(from), To: string(to)}
}

func IsWaiting(p Protocol) bool {
	switch p {
	case ProtocolWaitingPermission, ProtocolWaitingUser, ProtocolWaitingScope:
		return true
	default:
		return false
	}
}

func MayEscalateToStall(p Protocol) bool {
	return !IsWaiting(p) && p != ProtocolClosed
}

func ValidateDimensions(d Dimensions) error {
	if _, ok := processTransitions[d.Process]; !ok {
		return fmt.Errorf("unknown process state %q", d.Process)
	}
	if _, ok := protocolTransitions[d.Protocol]; !ok {
		return fmt.Errorf("unknown protocol state %q", d.Protocol)
	}
	if _, ok := progressTransitions[d.Progress]; !ok {
		return fmt.Errorf("unknown progress state %q", d.Progress)
	}
	if _, ok := taskTransitions[d.Task]; !ok {
		return fmt.Errorf("unknown task state %q", d.Task)
	}
	if IsWaiting(d.Protocol) && (d.Progress == ProgressSuspectedStall || d.Progress == ProgressStalled) {
		return fmt.Errorf("waiting protocol state %q cannot be classified as %q", d.Protocol, d.Progress)
	}
	return nil
}
