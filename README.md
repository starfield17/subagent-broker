# subagent-broker — Phase 1 Runtime

This repository implements the Phase 0 architecture skeleton and the Phase 1 runtime slice from the Multi-Harness Parallel Subagent Skill manual.

## Included

- Core domain models for Project, Run, Wave, Task, WorkerSession, Message, Event, and Result Envelope.
- Four-dimensional state model with explicit transition validation.
- Project-first Broker Home layout and atomic file publication helpers.
- Soft-scope parsing, conservative overlap detection, Wave preflight, and Run-level scope audit.
- Harness Adapter contract, registry, independent capability descriptors for Claude Code, Codex, Grok Build, and OpenCode.
- Deterministic Fake Harness with scripted scenarios for lifecycle and protocol tests.
- Append-only event store with monotonic Run sequence and incomplete-tail recovery.
- Result/question semantic validation and atomic Markdown publication.
- Claude Code stream-json Adapter with structured results, normalized events, stderr capture, and session identity.
- Detached run-scoped Supervisor with process-group control, Unix-socket IPC, persistence, timeout handling, and recovery reconciliation.
- A production CLI for one-task Claude dispatch, status, events, wait, collect, cancel, recover, and doctor operations.
- A real Claude smoke fixture in `examples/phase1-smoke-tasks.json`.
- Twelve accepted Architecture Decision Records (ADRs).
- Unit, contract, race, and real smoke verification.

## Phase 1 boundary

Phase 1 dispatches exactly one Task through Claude Code. Same-Wave parallel execution, complete Wave barriers, the other three Harness runtimes, permission routing, Git baseline/diff execution, and full Worker takeover remain later-phase work. V1 does not create worktrees or nested agents.

## Verify

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/subagent-broker
```

## Run a Phase 1 smoke

```bash
go build -o /tmp/subagent-broker ./cmd/subagent-broker
/tmp/subagent-broker doctor
/tmp/subagent-broker dispatch \
  --project /path/to/project \
  --goal "Run the Phase 1 smoke task" \
  --tasks examples/phase1-smoke-tasks.json \
  --permission-mode acceptEdits \
  --model sonnet
```

The dispatch command prints the Run ID and Run directory. Use that Run ID with `status`, `wait`, `collect`, `events`, or `cancel`:

```bash
/tmp/subagent-broker wait --project /path/to/project --run <run-id>
/tmp/subagent-broker collect --project /path/to/project --run <run-id>
/tmp/subagent-broker events --project /path/to/project --run <run-id>
```

Set `BROKER_HOME` or pass `--broker-home` to keep Broker state outside the project repository.

## Package map

- `internal/project`: project identity, canonical paths, project keys, Run IDs.
- `internal/run`: Run construction and validation.
- `internal/wave`: same-Wave preflight and barrier result types.
- `internal/task`: Task Contract validation and Markdown rendering.
- `internal/state`: four orthogonal state dimensions and transitions.
- `internal/scope`: soft-scope glob matching and overlap checks.
- `internal/adapter`: Adapter interface, capability model, registry, descriptors.
- `internal/adapter/fake`: deterministic scripted Fake Harness.
- `internal/event`: normalized append-only events and replay.
- `internal/message`: persistent message/question models and publication.
- `internal/report`: Result Envelope validation and report publication.
- `internal/storage`: Broker Home layout and atomic I/O.
- `internal/verify`: Run-level scope audit.
- `internal/process`: process identity abstractions for later process-tree management.
- `internal/supervisor`: run-scoped Supervisor, persistence, recovery, and IPC.
- `internal/doctor`: descriptor-level compatibility inventory.

See `docs/phase0-coverage.md` and `docs/phase1-coverage.md` for requirement-to-code matrices.
