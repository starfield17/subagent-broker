package stall

import (
	"sort"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/state"
)

// StallAssessment is a durable, explainable stall evaluation. QuietFor alone
// is never enough to claim high confidence.
type StallAssessment struct {
	State       string        `json:"state"`      // active|quiet|suspected_stall|stalled|none
	Confidence  string        `json:"confidence"` // none|low|medium|high
	Reason      string        `json:"reason"`
	Evidence    []string      `json:"evidence,omitempty"`
	QuietFor    time.Duration `json:"quiet_for"`
	EvaluatedAt time.Time     `json:"evaluated_at"`
}

// Assessment is retained as an alias for the existing clioutcome/status WIP.
type Assessment = StallAssessment

// Input collects independent signals for stall assessment.
type Input struct {
	Protocol            state.Protocol
	Progress            state.Progress
	Process             state.Process
	LastProgressAt      time.Time
	LastEventAt         time.Time // legacy alias for protocol activity
	LastProtocolEventAt time.Time
	LastStdoutAt        time.Time
	LastStderrAt        time.Time
	LastToolStartAt     time.Time
	LastToolFinishAt    time.Time
	HeartbeatAt         time.Time
	TurnStartedAt       time.Time
	TurnEndedAt         time.Time
	HasPendingMessage   bool
	Waiting             bool
	QuietAfter          time.Duration
	StallAfter          time.Duration
	Now                 time.Time
}

// Assess combines progress age with independent protocol/output/tool/process
// signals. A known waiting state and an exited process are explicitly outside
// the stall classifier.
func Assess(in Input) StallAssessment {
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	quietAfter := in.QuietAfter
	if quietAfter <= 0 {
		quietAfter = 30 * time.Second
	}
	stallAfter := in.StallAfter
	if stallAfter <= 0 {
		stallAfter = 2 * time.Minute
	}
	out := StallAssessment{EvaluatedAt: now, Confidence: "none", State: string(state.ProgressActive)}
	if in.LastProgressAt.IsZero() {
		out.State = "none"
		out.Reason = "no progress timestamp"
		return out
	}
	quiet := now.Sub(in.LastProgressAt)
	if quiet < 0 {
		quiet = 0
	}
	out.QuietFor = quiet

	if in.HasPendingMessage || in.Waiting || state.IsWaiting(in.Protocol) {
		out.State = "none"
		out.Confidence = "none"
		out.Reason = "worker is waiting for main agent or permission"
		out.Evidence = []string{"protocol=" + string(in.Protocol), "pending_message=" + boolString(in.HasPendingMessage)}
		return out
	}
	if in.Process == state.ProcessExited || in.Process == state.ProcessOrphaned {
		out.State = "none"
		out.Confidence = "none"
		out.Reason = "process lifecycle owns this case"
		out.Evidence = []string{"process=" + string(in.Process)}
		return out
	}

	protocolAt := in.LastProtocolEventAt
	if protocolAt.IsZero() {
		protocolAt = in.LastEventAt
	}
	outputAt := later(in.LastStdoutAt, in.LastStderrAt)
	toolAt := later(in.LastToolStartAt, in.LastToolFinishAt)
	protocolQuiet := staleFor(now, protocolAt, stallAfter)
	outputQuiet := staleFor(now, outputAt, stallAfter)
	toolQuiet := staleFor(now, toolAt, stallAfter)
	heartbeatQuiet := staleFor(now, in.HeartbeatAt, stallAfter)
	independent := 0
	if protocolQuiet {
		independent++
	}
	if outputQuiet {
		independent++
	}
	if toolQuiet {
		independent++
	}

	if quiet < quietAfter {
		out.State = string(state.ProgressActive)
		out.Reason = "recent progress"
		return out
	}

	if quiet < stallAfter {
		out.State = string(state.ProgressQuiet)
		out.Confidence = "low"
		out.Reason = "quiet timeout only"
		out.Evidence = []string{"no progress for " + quiet.String()}
		return out
	}

	// A single quiet timer at the stall threshold is explicitly low
	// confidence. Independent protocol/output/tool evidence promotes it.
	out.State = string(state.ProgressSuspectedStall)
	out.Confidence = "low"
	out.Reason = "quiet timeout only"
	out.Evidence = []string{"no progress for " + quiet.String()}
	if protocolQuiet {
		out.Evidence = append(out.Evidence, "no protocol event for "+age(now, protocolAt).String())
	}
	if outputQuiet {
		out.Evidence = append(out.Evidence, "no stdout/stderr for "+age(now, outputAt).String())
	}
	if toolQuiet {
		out.Evidence = append(out.Evidence, "no tool start/finish for "+age(now, toolAt).String())
	}
	if in.Process == state.ProcessAlive {
		out.Evidence = append(out.Evidence, "process still alive")
	}
	if !in.TurnStartedAt.IsZero() && in.TurnEndedAt.IsZero() {
		out.Evidence = append(out.Evidence, "turn still open for "+age(now, in.TurnStartedAt).String())
	}
	if !in.HeartbeatAt.IsZero() && heartbeatQuiet {
		out.Evidence = append(out.Evidence, "harness heartbeat quiet for "+age(now, in.HeartbeatAt).String())
	}
	if independent >= 2 || (independent >= 1 && in.Process == state.ProcessAlive) {
		out.Confidence = "medium"
		out.Reason = "multiple independent progress signals are quiet"
	}
	if quiet >= 2*stallAfter && in.Process == state.ProcessAlive && independent >= 1 {
		out.State = string(state.ProgressStalled)
		out.Confidence = "high"
		out.Reason = "no protocol or output progress for an extended period while process is alive"
	}
	sort.Strings(out.Evidence)
	return out
}

func staleFor(now, at time.Time, threshold time.Duration) bool {
	return !at.IsZero() && age(now, at) >= threshold
}

func age(now, at time.Time) time.Duration {
	if at.IsZero() {
		return 0
	}
	value := now.Sub(at)
	if value < 0 {
		return 0
	}
	return value
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

// EvidenceSummary is useful for compact status renderers and tests.
func EvidenceSummary(assessment StallAssessment) string {
	return strings.Join(assessment.Evidence, "; ")
}
