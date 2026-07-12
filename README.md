# subagent-broker — Phase 0 Architecture Skeleton

This repository implements **Phase 0: architecture skeleton** from the Multi-Harness Parallel Subagent Skill manual.

## Included

- Core domain models for Project, Run, Wave, Task, WorkerSession, Message, Event, and Result Envelope.
- Four-dimensional state model with explicit transition validation.
- Project-first Broker Home layout and atomic file publication helpers.
- Soft-scope parsing, conservative overlap detection, Wave preflight, and Run-level scope audit.
- Harness Adapter contract, registry, independent capability descriptors for Claude Code, Codex, Grok Build, and OpenCode.
- Deterministic Fake Harness with scripted scenarios for lifecycle and protocol tests.
- Append-only event store with monotonic Run sequence and incomplete-tail recovery.
- Result/question semantic validation and atomic Markdown publication.
- Twelve accepted Architecture Decision Records (ADRs).
- Unit and contract tests.

## Explicitly deferred

Phase 0 does **not** implement a production CLI, real Harness connections, a runnable Supervisor, IPC, process-tree control, Wave execution, recovery takeover, or Git integration. Those belong to later phases. No worktree support is present.

## Verify

```bash
go test ./...
go vet ./...
```

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
- `internal/supervisor`: Phase 0 Supervisor boundaries only.
- `internal/doctor`: descriptor-level compatibility inventory.

See `docs/phase0-coverage.md` for a requirement-to-code matrix.
