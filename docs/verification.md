# Verification Record

The Phase 0 skeleton and Phase 1 runtime were checked with the repository's configured Go toolchain using:

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/subagent-broker
```

All packages compiled; unit and contract tests passed; `go vet` reported no findings; race-enabled tests passed.

The Phase 1 real smoke used `examples/phase1-smoke-tasks.json` with the installed Claude Code CLI. It completed with a verified Task and Wave, published a structured report, emitted normalized lifecycle events, and created `smoke-output.txt` containing `phase1-ok`.

Phase 2 and Phase 3 were verified against Claude Code 2.1.197:

- `examples/phase2-parallel-plan.json` started two disjoint Workers concurrently, verified both reports, audited both changed files, passed the Wave Barrier, and passed final checks.
- `examples/phase3-question-plan.json` published a persistent question, accepted an answer through `send`, resumed the same turn, and completed successfully.
- `examples/phase3-scope-plan.json` blocked before an out-of-scope edit, updated the Task Contract after approval, and passed the Barrier with the expanded lease.
- `examples/phase3-permission-plan.json` routed two Bash tool calls through persistent permission requests, continued after approval, and completed successfully.

These smokes are installed-environment checks, not compatibility claims for every Claude Code release or provider configuration.

## Phase 4 local verification

The Phase 4 runtime was verified with the locally installed tools:

- Claude Code `2.1.197`
- Codex `0.144.1`
- Grok Build `0.2.99`
- OpenCode `1.17.15`

`subagent-broker doctor` probes all four Harnesses and reports installed, authenticated, compatible status and capabilities without printing credentials. Scripted Adapter contracts cover Codex App Server JSONL and Grok ACP multiplexing; the native contract smoke completed authenticated `StartSession`/`CollectFinalResult` flows for Claude Code, Codex, Grok Build, and OpenCode. A full Codex dispatch completed through Supervisor, Barrier verification, and report publication; OpenCode also passed a real loopback server health probe.

Capability claims remain protocol-specific: Codex active-turn steering is sent through `turn/steer`; Grok Build and OpenCode expose next-turn/resume delivery rather than immediate steering. Versions outside the tested versions are reported as `compatibility_unverified`.
