# Phase 0 Coverage

| Manual Phase 0 deliverable | Implementation |
|---|---|
| Domain model | `internal/project`, `internal/run`, `internal/wave`, `internal/task`, `internal/process`, `internal/message`, `internal/report` |
| Four-dimensional state machine | `internal/state` with transition validation and waiting/stall rules |
| File layout | `internal/storage.Layout`, Broker Home resolution, atomic writes |
| Adapter interface | `internal/adapter.Adapter`, capability model, registry, four Harness descriptors |
| Fake Harness | `internal/adapter/fake`, built-in deterministic scenarios, contract tests |
| ADR | `docs/adr/0001` through `0012` |

## Supporting architecture safeguards

- Conservative same-Wave scope overlap detection: `internal/scope` and `internal/wave`.
- Append-only monotonic events and damaged-tail replay: `internal/event`.
- Validated Result Envelope and formal Markdown publication marker: `internal/report`.
- Validated question publication: `internal/message`.
- Run-level unauthorized-file audit: `internal/verify`.
- UUIDv7-based sortable Run IDs: `internal/project`.

## Deferred by design

The following are deliberately absent because the manual assigns them to later phases:

- real Claude Code, Codex, Grok Build, or OpenCode protocol connections;
- production `dispatch/status/wait/collect/cancel` CLI;
- active Run Supervisor and local IPC;
- OS process-group / Windows Job Object implementation;
- Git baseline/diff execution;
- Worker takeover after Supervisor failure;
- real permission routing and session resume;
- Wave execution and Barrier verification commands.
