package cliread

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vnai/subagent-broker/internal/domain"
	"github.com/vnai/subagent-broker/internal/event"
	"github.com/vnai/subagent-broker/internal/process"
	"github.com/vnai/subagent-broker/internal/state"
	"github.com/vnai/subagent-broker/internal/supervisor"
)

// ReadSource identifies where a read was obtained.
type ReadSource string

const (
	ReadSourceIPC  ReadSource = "ipc"
	ReadSourceDisk ReadSource = "disk"
)

type IdentityStatus string

const (
	IdentityValid       IdentityStatus = "valid"
	IdentityUnavailable IdentityStatus = "unavailable"
	IdentityMismatched  IdentityStatus = "mismatched"
)

// ReadMetadata is deliberately shared by status, events, and wait.
type ReadMetadata struct {
	Source             ReadSource     `json:"source"`
	Mode               string         `json:"mode"` // live|degraded
	Degraded           bool           `json:"degraded"`
	Reason             string         `json:"reason,omitempty"`
	SnapshotTime       time.Time      `json:"snapshot_time,omitempty"`
	SupervisorAlive    bool           `json:"supervisor_alive"`
	IdentityValid      bool           `json:"identity_valid"`
	SupervisorIdentity IdentityStatus `json:"supervisor_identity"`
	TailRepaired       bool           `json:"tail_repaired,omitempty"`
	QuarantinePath     string         `json:"quarantine_path,omitempty"`
}

type SnapshotView struct {
	Snapshot supervisor.Snapshot `json:"snapshot"`
	Meta     ReadMetadata        `json:"meta"`
}

type EventsView struct {
	Events []event.Event `json:"events"`
	Meta   ReadMetadata  `json:"meta"`
}

type WaitView struct {
	Snapshot supervisor.Snapshot `json:"snapshot"`
	Matched  bool                `json:"matched"`
	Meta     ReadMetadata        `json:"meta"`
}

var (
	// ErrSupervisorUnavailable means that disk data was returned but a
	// non-terminal target cannot be trusted as live.
	ErrSupervisorUnavailable = errors.New("supervisor unavailable; disk data is degraded")
	ErrWaitTimeout           = errors.New("supervisor wait timed out")
)

type supervisorInfo struct {
	Identity *domain.SupervisorIdentity
	Alive    bool
	Status   IdentityStatus
	Reason   string
	Endpoint string
}

// LoadSnapshot is the single IPC-first snapshot read used by status and the
// disk fallback path of wait.
func LoadSnapshot(runDir string) (SnapshotView, error) {
	info := inspectSupervisor(runDir)
	meta := metadataFor(info)
	if info.Alive && info.Status == IdentityValid {
		snapshot, err := fetchSnapshotIPC(runDir, info.Endpoint)
		if err == nil {
			meta.Source = ReadSourceIPC
			meta.Mode = "live"
			meta.Degraded = false
			meta.Reason = "live supervisor"
			meta.SnapshotTime = snapshot.UpdatedAt
			return SnapshotView{Snapshot: snapshot, Meta: meta}, nil
		}
		meta.Reason = "ipc failed: " + err.Error() + "; disk snapshot may be stale; recovery required"
	}

	snapshot, err := readSnapshotDisk(runDir)
	if err != nil {
		return SnapshotView{Meta: meta}, err
	}
	meta.Source = ReadSourceDisk
	meta.Mode = "degraded"
	meta.Degraded = true
	meta.SnapshotTime = snapshot.UpdatedAt
	if meta.Reason == "" {
		meta.Reason = "supervisor unavailable"
	}
	return SnapshotView{Snapshot: snapshot, Meta: meta}, nil
}

// LoadEvents uses the same liveness and IPC-first policy as LoadSnapshot. The
// Supervisor owns the journal read while IPC is available; disk replay is only
// a marked degraded fallback and uses the repairable journal implementation.
func LoadEvents(runDir string, sinceSeq uint64) (EventsView, error) {
	info := inspectSupervisor(runDir)
	meta := metadataFor(info)
	if info.Alive && info.Status == IdentityValid {
		result, err := fetchEventsIPC(runDir, info.Endpoint, sinceSeq)
		if err == nil {
			meta.Source = ReadSourceIPC
			meta.Mode = "live"
			meta.Degraded = false
			meta.Reason = "live supervisor"
			meta.SnapshotTime = result.SnapshotTime
			meta.TailRepaired = result.TailRepaired
			meta.QuarantinePath = result.QuarantinePath
			return EventsView{Events: result.Events, Meta: meta}, nil
		}
		meta.Reason = "ipc failed: " + err.Error() + "; disk journal may lag; recovery required"
	}

	replay, err := event.Replay(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		return EventsView{Meta: meta}, err
	}
	items := make([]event.Event, 0, len(replay.Events))
	var snapshotTime time.Time
	for _, item := range replay.Events {
		if item.Seq > sinceSeq {
			items = append(items, item)
		}
		if item.Timestamp.After(snapshotTime) {
			snapshotTime = item.Timestamp
		}
	}
	meta.Source = ReadSourceDisk
	meta.Mode = "degraded"
	meta.Degraded = true
	meta.TailRepaired = replay.TailRepaired
	meta.QuarantinePath = replay.QuarantinePath
	meta.SnapshotTime = snapshotTime
	if meta.Reason == "" {
		meta.Reason = "supervisor unavailable; disk journal may lag"
	}
	return EventsView{Events: items, Meta: meta}, nil
}

// Wait performs one Supervisor long-poll. It falls back to one repaired disk
// read only when IPC cannot be used; callers must not treat a non-terminal
// degraded result as continued live progress.
func Wait(runDir string, params supervisor.WaitParams) (WaitView, error) {
	info := inspectSupervisor(runDir)
	meta := metadataFor(info)
	if info.Alive && info.Status == IdentityValid {
		response, err := callIPC(runDir, info.Endpoint, "wait", params)
		if err == nil {
			snapshot, decodeErr := snapshotResult(response.Result)
			if decodeErr != nil {
				return WaitView{Meta: meta}, decodeErr
			}
			meta.Source = ReadSourceIPC
			meta.Mode = "live"
			meta.Degraded = false
			meta.Reason = "live supervisor"
			meta.SnapshotTime = snapshot.UpdatedAt
			view := WaitView{Snapshot: snapshot, Matched: response.OK, Meta: meta}
			if response.OK {
				return view, nil
			}
			if isTimeoutError(response.Error) {
				return view, ErrWaitTimeout
			}
			return view, errors.New(response.Error)
		}
		meta.Reason = "ipc failed: " + err.Error() + "; disk snapshot may be stale; recovery required"
	}

	view, err := LoadSnapshot(runDir)
	if err != nil {
		return WaitView{Meta: meta}, err
	}
	// LoadSnapshot may have raced with a Supervisor restart; preserve the
	// degraded reason from the actual fallback read.
	view.Meta.Degraded = true
	view.Meta.Mode = "degraded"
	if view.Meta.Reason == "" {
		view.Meta.Reason = meta.Reason
	}
	result := WaitView{Snapshot: view.Snapshot, Meta: view.Meta, Matched: diskTargetMatched(view.Snapshot, params)}
	if result.Matched {
		return result, nil
	}
	return result, ErrSupervisorUnavailable
}

func diskTargetMatched(snapshot supervisor.Snapshot, params supervisor.WaitParams) bool {
	condition := params.For
	if condition == "" {
		condition = "run"
	}
	switch condition {
	case "run":
		switch snapshot.Run.Status {
		case domain.RunCompleted, domain.RunFailed, domain.RunCancelled, domain.RunDegraded:
			return true
		}
	case "wave":
		waveID := params.WaveID
		if waveID == "" {
			waveID = string(snapshot.Run.CurrentWave)
		}
		for _, value := range snapshot.Waves {
			if string(value.WaveID) == waveID {
				return value.Status == domain.WaveVerified || value.Status == domain.WaveWaiting || value.Status == domain.WaveBlocked || value.Status == domain.WaveFailed || value.Status == domain.WaveCancelled
			}
		}
		return string(snapshot.Wave.WaveID) == waveID && (snapshot.Wave.Status == domain.WaveVerified || snapshot.Wave.Status == domain.WaveWaiting || snapshot.Wave.Status == domain.WaveBlocked || snapshot.Wave.Status == domain.WaveFailed || snapshot.Wave.Status == domain.WaveCancelled)
	case "task":
		for _, runtime := range snapshot.Tasks {
			if string(runtime.Task.TaskID) == params.TaskID {
				return runtime.Task.Status == state.TaskVerifiedSuccess || runtime.Task.Status == state.TaskVerifiedPartial || runtime.Task.Status == state.TaskVerificationFailed || runtime.Task.Status == state.TaskFailed || runtime.Task.Status == state.TaskCancelled || (runtime.Task.Status == state.TaskBlocked && (params.ReturnOnBlocked || runtime.BlockKind == supervisor.BlockKindFinal))
			}
		}
	}
	return false
}

func metadataFor(info supervisorInfo) ReadMetadata {
	return ReadMetadata{
		Source: ReadSourceDisk, Mode: "degraded", Degraded: true,
		Reason: info.Reason, SupervisorAlive: info.Alive,
		IdentityValid: info.Status == IdentityValid, SupervisorIdentity: info.Status,
	}
}

func inspectSupervisor(runDir string) supervisorInfo {
	identity, err := loadSupervisorIdentity(runDir)
	if err != nil || identity == nil || identity.PID <= 0 || identity.ProcessStartToken == "" {
		return supervisorInfo{Identity: identity, Status: IdentityUnavailable, Reason: "supervisor identity unavailable; non-terminal snapshot may be stale; recovery required", Endpoint: supervisor.SocketPath(runDir)}
	}
	current, err := process.Inspect(context.Background(), identity.PID)
	if err != nil {
		return supervisorInfo{Identity: identity, Status: IdentityUnavailable, Reason: "supervisor process unavailable; non-terminal snapshot may be stale; recovery required", Endpoint: endpointFor(runDir, identity)}
	}
	if !(process.Identity{PID: identity.PID, StartToken: identity.ProcessStartToken}).SameProcess(current) {
		return supervisorInfo{Identity: identity, Status: IdentityMismatched, Reason: "supervisor pid reused (start token mismatch); recovery required", Endpoint: endpointFor(runDir, identity)}
	}
	endpoint := endpointFor(runDir, identity)
	if _, err := os.Stat(endpoint); err != nil {
		return supervisorInfo{Identity: identity, Alive: true, Status: IdentityValid, Reason: "supervisor endpoint unavailable", Endpoint: endpoint}
	}
	return supervisorInfo{Identity: identity, Alive: true, Status: IdentityValid, Endpoint: endpoint}
}

func endpointFor(runDir string, identity *domain.SupervisorIdentity) string {
	if identity != nil && strings.TrimSpace(identity.IPCEndpoint) != "" {
		return identity.IPCEndpoint
	}
	return supervisor.SocketPath(runDir)
}

func loadSupervisorIdentity(runDir string) (*domain.SupervisorIdentity, error) {
	if data, err := os.ReadFile(filepath.Join(runDir, "state.json")); err == nil {
		var snapshot supervisor.Snapshot
		if json.Unmarshal(data, &snapshot) == nil && snapshot.Run.SupervisorIdentity != nil {
			return snapshot.Run.SupervisorIdentity, nil
		}
	}
	data, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		return nil, err
	}
	var runValue domain.Run
	if err := json.Unmarshal(data, &runValue); err != nil {
		return nil, err
	}
	return runValue.SupervisorIdentity, nil
}

func fetchSnapshotIPC(runDir, endpoint string) (supervisor.Snapshot, error) {
	response, err := callIPC(runDir, endpoint, "status", nil)
	if err != nil {
		return supervisor.Snapshot{}, err
	}
	if !response.OK {
		return supervisor.Snapshot{}, fmt.Errorf("%s", response.Error)
	}
	return snapshotResult(response.Result)
}

func readSnapshotDisk(runDir string) (supervisor.Snapshot, error) {
	data, err := os.ReadFile(filepath.Join(runDir, "state.json"))
	if err != nil {
		return supervisor.Snapshot{}, err
	}
	var snapshot supervisor.Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return supervisor.Snapshot{}, err
	}
	return snapshot, nil
}

func fetchEventsIPC(runDir, endpoint string, sinceSeq uint64) (supervisor.EventsResult, error) {
	response, err := callIPC(runDir, endpoint, "events", map[string]any{"since_seq": sinceSeq})
	if err != nil {
		return supervisor.EventsResult{}, err
	}
	if !response.OK {
		return supervisor.EventsResult{}, fmt.Errorf("%s", response.Error)
	}
	raw, err := json.Marshal(response.Result)
	if err != nil {
		return supervisor.EventsResult{}, err
	}
	var result supervisor.EventsResult
	if err := json.Unmarshal(raw, &result); err != nil {
		var items []event.Event
		if arrayErr := json.Unmarshal(raw, &items); arrayErr != nil {
			return supervisor.EventsResult{}, err
		}
		result.Events = items
	}
	return result, nil
}

func snapshotResult(result any) (supervisor.Snapshot, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return supervisor.Snapshot{}, err
	}
	var snapshot supervisor.Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return supervisor.Snapshot{}, err
	}
	return snapshot, nil
}

// CallIPC is the shared mutation client. Barrier accept/reject callers use it
// so they cannot bypass the Supervisor by writing artifacts themselves.
func CallIPC(runDir, method string, params any) (supervisor.Response, error) {
	info := inspectSupervisor(runDir)
	if !info.Alive || info.Status != IdentityValid {
		return supervisor.Response{}, fmt.Errorf("%s: %s", info.Status, info.Reason)
	}
	return callIPC(runDir, info.Endpoint, method, params)
}

func callIPC(runDir, endpoint, method string, params any) (supervisor.Response, error) {
	if endpoint == "" {
		endpoint = supervisor.SocketPath(runDir)
	}
	conn, err := net.DialTimeout("unix", endpoint, 2*time.Second)
	if err != nil {
		return supervisor.Response{}, err
	}
	defer conn.Close()

	data, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		return supervisor.Response{}, err
	}
	var runValue domain.Run
	if err := json.Unmarshal(data, &runValue); err != nil {
		return supervisor.Response{}, err
	}
	request := supervisor.Request{
		SchemaVersion: supervisor.SchemaVersion,
		RequestID:     fmt.Sprintf("cli-%d", time.Now().UnixNano()),
		RunID:         string(runValue.RunID), Method: method,
	}
	if params != nil {
		request.Params, err = json.Marshal(params)
		if err != nil {
			return supervisor.Response{}, err
		}
	}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return supervisor.Response{}, err
	}
	if wait, ok := params.(supervisor.WaitParams); ok && wait.Timeout > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(wait.Timeout + time.Second))
	} else if _, ok := params.(supervisor.WaitParams); !ok {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	}
	var response supervisor.Response
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return supervisor.Response{}, err
	}
	return response, nil
}

func isTimeoutError(value string) bool {
	value = strings.ToLower(value)
	return strings.Contains(value, "deadline exceeded") || strings.Contains(value, "timed out") || strings.Contains(value, "timeout")
}

// HasRunIdentity reports whether this is a real run directory. It is useful to
// keep small unit-test fixtures (which only contain state.json) distinguishable
// from a confirmed dead Supervisor.
func HasRunIdentity(runDir string) bool {
	_, err := os.Stat(filepath.Join(runDir, "run.json"))
	return err == nil
}
