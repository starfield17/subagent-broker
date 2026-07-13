# Phase 4 Coverage

| Deliverable | Implementation | Verification |
|---|---|---|
| Codex runtime Adapter | `internal/adapter/codex`: App Server JSONL initialize, thread start/resume, turn start/steer/interrupt, event and approval mapping | scripted App Server contract plus authenticated local Result Envelope smoke |
| Grok Build runtime Adapter | `internal/adapter/grok`: ACP stdio initialize/authenticate/session new/load/prompt/cancel and update mapping | scripted ACP contract plus authenticated local Result Envelope smoke |
| OpenCode runtime Adapter | `internal/adapter/opencode`: per-Worker loopback server, HTTP session/prompt/abort/permission/diff/history, SSE event stream | local server/provider Result Envelope smoke and doctor health probe |
| Mixed Harness routing | Task preference overrides Run default; persisted Worker Harness drives execution, messages, and recovery | mixed-preference routing tests and existing Supervisor regression suite |
| Unified doctor | default all-Harness probe with optional `--harness` filter and stable JSON envelope | installed/authenticated local doctor output |
| Unified contracts | common Result Envelope, normalized events, cancellation, exit, capability and protocol fixture checks | `go test ./...`, race tests, and native contract tests |

Phase 4 does not claim compatibility for versions outside each Adapter's tested range. Uninstalled, unauthenticated, or provider-unavailable Harnesses remain explicit preflight failures. Control credentials are persisted for CLI access; Worker credentials are process-lifetime only.
