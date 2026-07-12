package supervisor

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/state"
	workerpkg "github.com/vnai/subagent-broker/internal/worker"
)

// ApplyEvent applies one durable event to a Snapshot checkpoint.
// It is pure: no disk, adapter, or process access.
func ApplyEvent(snapshot Snapshot, ev event.Event) (Snapshot, error) {
	if ev.Seq == 0 {
		return snapshot, fmt.Errorf("event seq must be greater than 0")
	}
	if snapshot.Run.RunID != "" && ev.RunID != "" && string(snapshot.Run.RunID) != ev.RunID {
		return snapshot, fmt.Errorf("event run_id %q does not match snapshot run_id %q", ev.RunID, snapshot.Run.RunID)
	}
	if snapshot.AppliedEventSeq > 0 && ev.Seq <= snapshot.AppliedEventSeq {
		return snapshot, fmt.Errorf("event seq %d already applied (checkpoint %d)", ev.Seq, snapshot.AppliedEventSeq)
	}
	if snapshot.AppliedEventSeq > 0 && ev.Seq != snapshot.AppliedEventSeq+1 {
		return snapshot, fmt.Errorf("event seq gap: expected %d, got %d", snapshot.AppliedEventSeq+1, ev.Seq)
	}

	next, err := cloneSnapshot(snapshot)
	if err != nil {
		return snapshot, err
	}
	if next.Run.RunID == "" && ev.RunID != "" {
		next.Run.RunID = domain.RunID(ev.RunID)
	}

	switch ev.Type {
	case event.RunStateChanged:
		from, to, reason, err := decodeTransition(ev.Payload)
		if err != nil {
			return snapshot, err
		}
		if string(next.Run.Status) != "" && string(next.Run.Status) != from {
			return snapshot, fmt.Errorf("run state from mismatch: have %s payload from %s", next.Run.Status, from)
		}
		next.Run.Status = domain.RunStatus(to)
		if reason != "" {
			next.LastError = reason
		}
		now := ev.Timestamp
		if now.IsZero() {
			now = time.Now().UTC()
		}
		if next.Run.StartedAt == nil && next.Run.Status != domain.RunPlanned {
			next.Run.StartedAt = &now
		}
		if next.Run.Status == domain.RunCompleted || next.Run.Status == domain.RunFailed || next.Run.Status == domain.RunCancelled || next.Run.Status == domain.RunDegraded {
			next.Run.EndedAt = &now
		}

	case event.WaveStateChanged:
		payload, err := decodeMap(ev.Payload)
		if err != nil {
			return snapshot, err
		}
		waveID, _ := payload["wave_id"].(string)
		from, _ := payload["from"].(string)
		to, _ := payload["to"].(string)
		reason, _ := payload["reason"].(string)
		if waveID == "" {
			return snapshot, fmt.Errorf("wave.state_changed requires wave_id")
		}
		if to == "" && payload["barrier_result"] == nil {
			return snapshot, fmt.Errorf("wave.state_changed requires to or barrier_result")
		}
		applyWave := func(w *domain.Wave) error {
			if to != "" {
				if w.Status != "" && from != "" && string(w.Status) != from && reason != "select_wave" && reason != "barrier_result" {
					// select_wave reuses the event type with wave id transition semantics
					if string(w.WaveID) == waveID && reason == "" {
						return fmt.Errorf("wave state from mismatch: have %s payload from %s", w.Status, from)
					}
				}
				if reason != "select_wave" && reason != "barrier_result" {
					w.Status = domain.WaveStatus(to)
				}
			}
			if br, ok := payload["barrier_result"].(string); ok && br != "" {
				w.BarrierResult = domain.BarrierResult(br)
			}
			return nil
		}
		if string(next.Wave.WaveID) == waveID || next.Wave.WaveID == "" {
			if next.Wave.WaveID == "" {
				next.Wave.WaveID = domain.WaveID(waveID)
			}
			if err := applyWave(&next.Wave); err != nil {
				return snapshot, err
			}
		}
		found := false
		for i := range next.Waves {
			if string(next.Waves[i].WaveID) == waveID {
				if err := applyWave(&next.Waves[i]); err != nil {
					return snapshot, err
				}
				if string(next.Wave.WaveID) == waveID {
					next.Wave = next.Waves[i]
				}
				found = true
				break
			}
		}
		if !found && reason == "select_wave" {
			next.Run.CurrentWave = domain.WaveID(waveID)
			for _, w := range next.Waves {
				if w.WaveID == domain.WaveID(waveID) {
					next.Wave = w
					break
				}
			}
		}
		if br, ok := payload["barrier_result"].(string); ok && br != "" {
			// already applied
			_ = br
		}

	case event.TaskStateChanged, event.TaskReportedComplete, event.TaskVerifiedSuccess, event.TaskVerificationFailed:
		payload, err := decodeMap(ev.Payload)
		if err != nil {
			return snapshot, err
		}
		taskID := ev.TaskID
		if taskID == "" {
			if v, ok := payload["task_id"].(string); ok {
				taskID = v
			}
		}
		from, _ := payload["from"].(string)
		to, _ := payload["to"].(string)
		if to == "" {
			// legacy event type without explicit to
			switch ev.Type {
			case event.TaskReportedComplete:
				to = string(state.TaskReportedComplete)
			case event.TaskVerifiedSuccess:
				to = string(state.TaskVerifiedSuccess)
			case event.TaskVerificationFailed:
				to = string(state.TaskVerificationFailed)
			}
		}
		if taskID == "" || to == "" {
			return snapshot, fmt.Errorf("task state event requires task_id and to")
		}
		index, err := findTaskIndex(&next, domain.TaskID(taskID))
		if err != nil {
			return snapshot, err
		}
		current := next.Tasks[index].Task.Status
		if from != "" && string(current) != from {
			return snapshot, fmt.Errorf("task %s state from mismatch: have %s payload from %s", taskID, current, from)
		}
		if err := state.ValidateTaskTransition(current, state.Task(to)); err != nil {
			return snapshot, err
		}
		next.Tasks[index].Task.Status = state.Task(to)
		next.Tasks[index].Dimensions.Task = state.Task(to)
		if next.Tasks[index].Worker != nil {
			next.Tasks[index].Worker.StatusDimensions.Task = state.Task(to)
		}

	case event.WorkerAttemptStarted:
		payload, err := decodeMap(ev.Payload)
		if err != nil {
			return snapshot, err
		}
		raw, ok := payload["worker"]
		if !ok {
			return snapshot, fmt.Errorf("worker.attempt_started requires worker payload")
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			return snapshot, err
		}
		var w domain.WorkerSession
		if err := json.Unmarshal(encoded, &w); err != nil {
			return snapshot, err
		}
		taskID := ev.TaskID
		if taskID == "" {
			taskID = string(w.TaskID)
		}
		index, err := findTaskIndex(&next, domain.TaskID(taskID))
		if err != nil {
			return snapshot, err
		}
		migrateAttempts(&next.Tasks[index])
		number := w.Attempt
		if number <= 0 {
			if v, ok := payload["attempt"].(float64); ok {
				number = int(v)
			}
		}
		mode := workerpkg.AttemptFresh
		if v, ok := payload["mode"].(string); ok && v != "" {
			mode = workerpkg.AttemptMode(v)
		}
		next.Tasks[index].Attempts = append(next.Tasks[index].Attempts, workerpkg.Attempt{
			Number: number, Mode: mode, Worker: w, Outcome: workerpkg.AttemptRunning, StartedAt: w.StartedAt,
		})
		next.Tasks[index].ActiveAttempt = number
		next.Tasks[index].Worker = &w
		next.Tasks[index].Dimensions = w.StatusDimensions

	case event.WorkerAttemptFinished:
		if ev.TaskID != "" {
			index, err := findTaskIndex(&next, domain.TaskID(ev.TaskID))
			if err == nil {
				payload, _ := decodeMap(ev.Payload)
				to, _ := payload["to"].(string)
				reason, _ := payload["reason"].(string)
				if to != "" {
					finishActiveAttempt(&next.Tasks[index], workerpkg.AttemptOutcome(to), reason, ev.Timestamp)
				}
			}
		}

	case event.RecoveryClassified, event.RecoveryResumed, event.CancelTreeRequested, event.CancelTreeCompleted:
		// Classification / cancel telemetry; state already applied by companion events.

	case event.TaskRuntimeUpdated:
		payload, err := decodeMap(ev.Payload)
		if err != nil {
			return snapshot, err
		}
		raw, ok := payload["task"]
		if !ok {
			return snapshot, fmt.Errorf("task.runtime_updated requires task payload")
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			return snapshot, err
		}
		var runtime TaskState
		if err := json.Unmarshal(encoded, &runtime); err != nil {
			return snapshot, err
		}
		index, err := findTaskIndex(&next, runtime.Task.TaskID)
		if err != nil {
			return snapshot, err
		}
		// Preserve scope expansions already on the checkpoint when the payload
		// is a stale worker-local copy.
		if len(next.Tasks[index].Task.WriteScope) > len(runtime.Task.WriteScope) {
			runtime.Task.WriteScope = append([]string(nil), next.Tasks[index].Task.WriteScope...)
			runtime.Task.AllowPublicInterfaceChange = next.Tasks[index].Task.AllowPublicInterfaceChange
		}
		next.Tasks[index] = runtime

	case event.ProcessStateChanged, event.ProcessSpawned, event.ProcessExited, event.ProcessOrphaned:
		// Best-effort process dimension updates from payload.
		if ev.TaskID != "" {
			index, err := findTaskIndex(&next, domain.TaskID(ev.TaskID))
			if err == nil {
				payload, _ := decodeMap(ev.Payload)
				if to, ok := payload["to"].(string); ok && to != "" {
					next.Tasks[index].Dimensions.Process = state.Process(to)
					if next.Tasks[index].Worker != nil {
						next.Tasks[index].Worker.StatusDimensions.Process = state.Process(to)
					}
				}
			}
		}

	case event.ProtocolStateChanged:
		if ev.TaskID != "" {
			index, err := findTaskIndex(&next, domain.TaskID(ev.TaskID))
			if err == nil {
				payload, _ := decodeMap(ev.Payload)
				if to, ok := payload["to"].(string); ok && to != "" {
					next.Tasks[index].Dimensions.Protocol = state.Protocol(to)
					if next.Tasks[index].Worker != nil {
						next.Tasks[index].Worker.StatusDimensions.Protocol = state.Protocol(to)
					}
				}
			}
		}

	case event.ProgressStateChanged:
		if ev.TaskID != "" {
			index, err := findTaskIndex(&next, domain.TaskID(ev.TaskID))
			if err == nil {
				payload, _ := decodeMap(ev.Payload)
				if to, ok := payload["to"].(string); ok && to != "" {
					from, _ := payload["from"].(string)
					if from != "" {
						if err := state.ValidateProgressTransition(state.Progress(from), state.Progress(to)); err != nil {
							return snapshot, err
						}
					}
					next.Tasks[index].Dimensions.Progress = state.Progress(to)
					if next.Tasks[index].Worker != nil {
						next.Tasks[index].Worker.StatusDimensions.Progress = state.Progress(to)
					}
				}
			}
		}

	case event.SupervisorHeartbeat:
		payload, _ := decodeMap(ev.Payload)
		if raw, ok := payload["identity"]; ok {
			encoded, _ := json.Marshal(raw)
			var identity domain.SupervisorIdentity
			if json.Unmarshal(encoded, &identity) == nil {
				next.Run.SupervisorIdentity = &identity
			}
		} else if next.Run.SupervisorIdentity != nil {
			if at, ok := payload["at"].(string); ok {
				if parsed, err := time.Parse(time.RFC3339Nano, at); err == nil {
					next.Run.SupervisorIdentity.HeartbeatAt = parsed
				}
			} else if !ev.Timestamp.IsZero() {
				next.Run.SupervisorIdentity.HeartbeatAt = ev.Timestamp
			}
		}

	default:
		// Telemetry / legacy events: advance checkpoint without mutating state.
	}

	next.AppliedEventSeq = ev.Seq
	if !ev.Timestamp.IsZero() {
		next.UpdatedAt = ev.Timestamp
	} else {
		next.UpdatedAt = time.Now().UTC()
	}
	return next, nil
}

// ReplayEvents applies events with seq > snapshot.AppliedEventSeq in order.
func ReplayEvents(snapshot Snapshot, events []event.Event) (Snapshot, error) {
	current := snapshot
	legacy := snapshot.AppliedEventSeq == 0
	var lastSeq uint64
	for _, ev := range events {
		if !legacy && ev.Seq <= snapshot.AppliedEventSeq {
			continue
		}
		if legacy {
			// Legacy snapshots have no checkpoint. We only advance AppliedEventSeq
			// over telemetry-safe events and refuse to invent full state rebuilds
			// from incomplete historical logs.
			if lastSeq != 0 && ev.Seq != lastSeq+1 {
				return snapshot, fmt.Errorf("legacy replay seq gap: %d after %d", ev.Seq, lastSeq)
			}
			lastSeq = ev.Seq
			// Still apply known state events when possible.
		}
		applied, err := ApplyEvent(current, ev)
		if err != nil {
			// For legacy mode, skip non-state events that fail identity checks
			// only when AppliedEventSeq is still zero and event is pure telemetry.
			if legacy && isTelemetry(ev.Type) {
				current.AppliedEventSeq = ev.Seq
				continue
			}
			return snapshot, err
		}
		current = applied
	}
	return current, nil
}

func isTelemetry(typeName string) bool {
	switch typeName {
	case event.RunStateChanged, event.WaveStateChanged, event.TaskStateChanged,
		event.TaskReportedComplete, event.TaskVerifiedSuccess, event.TaskVerificationFailed,
		event.TaskRuntimeUpdated, event.ProcessStateChanged, event.ProtocolStateChanged,
		event.ProgressStateChanged, event.SupervisorHeartbeat,
		event.WorkerAttemptStarted, event.WorkerAttemptFinished:
		return false
	default:
		return true
	}
}

func decodeMap(payload json.RawMessage) (map[string]any, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return map[string]any{}, nil
	}
	var value map[string]any
	if err := json.Unmarshal(payload, &value); err != nil {
		return nil, fmt.Errorf("decode event payload: %w", err)
	}
	return value, nil
}

func decodeTransition(payload json.RawMessage) (from, to, reason string, err error) {
	m, err := decodeMap(payload)
	if err != nil {
		return "", "", "", err
	}
	from, _ = m["from"].(string)
	to, _ = m["to"].(string)
	reason, _ = m["reason"].(string)
	if to == "" {
		return "", "", "", fmt.Errorf("state transition payload requires to")
	}
	return from, to, reason, nil
}
