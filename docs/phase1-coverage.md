# Phase 1 Coverage

| Phase 1 deliverable | Implementation | Verification |
|---|---|---|
| Claude Code stream-json Adapter | `internal/adapter/claude` | scripted stream parser tests and real smoke |
| Structured result collection | `internal/report`, `internal/adapter/claude` | envelope validation, object and text validation compatibility test |
| Detached Supervisor | `internal/supervisor`, `internal/process` | Fake lifecycle test, race test, real smoke |
| Process identity and process-group control | `internal/process` | process identity tests and real detached dispatch |
| Persistent Run, Wave, and Task state | `internal/supervisor/runtime.go` | state files, event replay, runtime integration test |
| Local IPC | `internal/supervisor/ipc.go` | status, wait, collect, events, and cancel CLI paths |
| Timeout and cancellation handling | `internal/supervisor/runtime.go` | lifecycle state transitions and cancellation paths |
| Recovery reconciliation | `internal/supervisor/runtime.go`, `cmd/subagent-broker` | recovery code path and process-token checks |
| Phase 1 CLI | `cmd/subagent-broker` | build, doctor, dispatch, wait, status, collect, events, cancel, recover |
| Real harness evidence | `examples/phase1-smoke-tasks.json` | installed Claude smoke completed with `verified_success` |

## Explicit Phase 1 limits

- Dispatch accepts exactly one Task.
- Claude Code is the only runtime Adapter; the other descriptors remain inventory entries.
- Wave parallelism and complete Wave barrier execution are deferred.
- Permission routing, Git baseline/diff execution, and full Worker takeover are deferred.
- No worktrees or nested agents are created.
