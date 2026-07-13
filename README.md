# subagent-broker — Phase 4 Runtime

This repository implements the architecture skeleton and the Phase 1 through Phase 4 runtime slices from the Multi-Harness Parallel Subagent Skill manual.

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
- Native Codex App Server, Grok ACP, and OpenCode Server Adapters with protocol-specific lifecycle, event, permission, usage, diff, and Result Envelope handling.
- Detached run-scoped Supervisor with process-group control, Unix-socket IPC, persistence, timeout handling, and recovery reconciliation.
- Ordered multi-Wave plans with concurrent same-Wave Claude Workers and a configurable concurrency limit.
- Content-based workspace baselines, Run/Wave scope audits, integration checks, Barrier artifacts, and final verification.
- Persistent inbox, direct instruction delivery, structured question/answer, scope expansion, and permission routing.
- A production CLI for dispatch, status, events, wait, collect, inbox, send, cancel, recover, and doctor operations.
- A real Claude smoke fixture in `examples/phase1-smoke-tasks.json`.
- Sixteen accepted Architecture Decision Records (ADRs).
- Unit, contract, race, and real smoke verification.

## Phase 4 boundary

Tasks may select `claude-code`, `codex`, `grok-build`, or `opencode` through `harness_preference`; a Run may contain mixed Harnesses. The Run-level `--harness` flag is the default for Tasks without an explicit preference. A single `--executable` override is accepted only for uniform-Harness Runs; mixed Runs use each Harness's PATH default. Full takeover of a still-running Worker after Supervisor failure remains hardening work. V1 does not create worktrees or nested agents.

## Verify

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/subagent-broker
```

## Run a parallel Wave smoke

```bash
go build -o /tmp/subagent-broker ./cmd/subagent-broker
/tmp/subagent-broker doctor
/tmp/subagent-broker dispatch \
  --project /path/to/project \
  --goal "Run the Phase 2 parallel smoke" \
  --tasks examples/phase2-parallel-plan.json \
  --permission-mode acceptEdits \
  --model sonnet
```

The dispatch command prints the Run ID and Run directory. Use that Run ID with `status`, `wait`, `collect`, `events`, or `cancel`:

```bash
/tmp/subagent-broker wait --project /path/to/project --run <run-id>
/tmp/subagent-broker collect --project /path/to/project --run <run-id>
/tmp/subagent-broker events --project /path/to/project --run <run-id>
/tmp/subagent-broker inbox --project /path/to/project --run <run-id>
/tmp/subagent-broker send --project /path/to/project --run <run-id> --message <message-id> --answer "answer"
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
- `internal/adapter/claude`: Claude stream-json and hook/MCP bridge.
- `internal/adapter/codex`: Codex App Server JSONL adapter.
- `internal/adapter/grok`: Grok ACP stdio adapter.
- `internal/adapter/opencode`: OpenCode loopback HTTP/SSE adapter.
- `internal/adapter/fake`: deterministic scripted Fake Harness.
- `internal/event`: normalized append-only events and replay.
- `internal/message`: append-only message storage, inbox models, and question publication.
- `internal/interaction`: Worker-facing MCP tools and Claude permission hook bridge.
- `internal/report`: Result Envelope validation and report publication.
- `internal/storage`: Broker Home layout and atomic I/O.
- `internal/verify`: Run-level scope audit.
- `internal/process`: process identity abstractions for later process-tree management.
- `internal/supervisor`: run-scoped Supervisor, persistence, recovery, and IPC.
- `internal/doctor`: descriptor-level compatibility inventory.

See `docs/phase0-coverage.md` through `docs/phase4-coverage.md` for requirement-to-code matrices.
