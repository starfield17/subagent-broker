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

## Phase 4 correctness patches (8.1–8.6)

Executed on 2026-07-13 against this repository after patches 8.1–8.6:

```bash
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/subagent-broker
```

Results:

| Command | Result |
|---|---|
| `go test ./...` | all packages PASS |
| `go test -race ./...` | all packages PASS |
| `go vet ./...` | no findings |
| `go build ./cmd/subagent-broker` | success |

Supervisor-level integration coverage (`internal/supervisor/phase4_integration_test.go` and related unit tests) exercises:

1. Native permission bridge: durable `permission_request`, allow/deny reach `RespondPermission`, adapter failure not recorded as Answered
2. Next-turn delivery: queue during turn, physical `SendMessage` at turn boundary, exactly-once Delivered
3. Resume harness routing: persisted Worker harness wins over Task preference / Run default
4. Event backpressure: critical lifecycle events survive channel saturation; progress may drop
5. Cancellation / multi-turn protocol unit tests: Grok cancel notification (no response waiter), OpenCode idle keeps server, newest Result Envelope selection

**Not claimed by these commands** (no live native harness re-smoke for the new paths in this patch series):

- Live Codex/Grok/OpenCode permission request → Main Agent answer → Worker continue
- Live next-turn instruction flush against a real multi-turn native session
- Live cancel tree honesty under real process-group failure modes beyond existing process tests

A simple historical `StartSession`/`CollectFinalResult` smoke is **not** treated as evidence for permission, resume, next-turn, or cancellation claims.

## Patch A runtime/recovery invariants

- A durably recorded Task execution failure must terminalize its Wave and Run before the Supervisor exits normally.
- Only Tasks that actually started are Worker recovery candidates. Never-started Tasks retain their pre-start state.

## Patch A validation

Executed on 2026-07-13:

```bash
gofmt -w .
test -z "$(gofmt -l .)"
go test ./...
go test -race ./...
go test -race ./internal/supervisor/... -count=10
go vet ./...
go build ./cmd/subagent-broker
```

Results: formatting check, all tests, race tests, Supervisor race stress, vet, and build passed. No live authenticated Harness smoke was run for Patch A.

## Phase 4 PR 8.7 — native permission wire protocols

Executed on 2026-07-13 after aligning Grok ACP and OpenCode 1.17.15 permission responses:

```bash
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/subagent-broker
```

Results: all packages PASS under `go test` and `go test -race`; `go vet` clean; build success.

Protocol fixtures covered (scripted / unit, not live authenticated harnesses):

| Fixture | Coverage |
|---|---|
| Grok ACP `session/request_permission` with numeric JSON-RPC `id` | option parse, allow → `selected` + native `optionId`, exact response shape |
| Grok ACP with string JSON-RPC `id` | id encoding preserved; allow_always / reject_always fallback |
| Grok missing compatible option | delivery fails; Broker message not `Answered` |
| OpenCode `permission.asked` with object-valued `tool` | request id extracted; allow/deny → `POST /permission/{id}/reply` with `{"reply":"once"|"reject"}` |
| OpenCode HTTP failure | not recorded as `Answered` |
| Codex accept/decline | still `{"decision":"accept"}` / `{"decision":"decline"}` |
| Claude hook path | protocol events do not enter native `RespondPermission` |

**Live authenticated Grok/OpenCode permission smoke was not performed in this patch.**

## Phase 4 PR 8.8 — next-turn delivery lifecycle

Executed after making turn-boundary ownership lifecycle-correct:

```bash
gofmt -w .
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/subagent-broker
```

Results: all packages PASS under `go test` and `go test -race`; `go vet` clean; build success.

Supervisor multi-turn lifecycle fixtures (`next_turn_lifecycle_test.go` via `runWorkerSession`, not direct flush-only tests):

| Fixture | Coverage |
|---|---|
| Queued next-turn after first `ResultSubmitted` | session kept alive; second turn consumed; final envelope is turn 2 |
| No queued instruction | first result remains final; terminate path |
| Two queued instructions | FIFO one-per-boundary; final envelope is turn 3 |
| Next-turn start failure | instruction `Failed`; first result remains final |
| `TurnFailed` | does not start next queued instruction |
| Second-turn permission | `SendMessage` returns; permission bridged/resolved; no deadlock; final result |

Grok async prompt fixtures:

| Fixture | Coverage |
|---|---|
| `SendMessage` before prompt RPC response | returns without waiting for completion; progress events readable |
| `session/prompt_completed` notification | maps to `TurnCompleted`; authoritative result after RPC response |
| Concurrent `SendMessage` | rejected while prompt in flight |
| Multi-turn keep-alive | second `SendMessage` after first completion |

OpenCode two-turn fixtures:

| Fixture | Coverage |
|---|---|
| First/second `session.idle` | freezes successive envelopes; session not closed |
| Concurrent prompt | rejected while in flight |

**Live authenticated next-turn smoke against real Grok/OpenCode was not performed.**

## Phase 4 PR 8.9 — single-owner native event streams

Executed after serializing `Session.Events` ownership:

```bash
gofmt -w .
go test ./...
go test -race ./...
go test -race ./internal/adapter/... -count=10
go vet ./...
go build ./cmd/subagent-broker
```

Results: all packages PASS under `go test` and `go test -race` (including adapter stress `-count=10`); `go vet` clean; build success.

### EventStream invariants tested (`internal/adapter/protocol/stream_test.go`)

- Critical events survive public-channel saturation (progress may drop with counter)
- Progress drop accounting under concurrent pressure
- Publish vs `CloseGracefully` race (no panic; post-close publish rejected)
- Publish vs `Abort` race (publishers unblock without a consumer)
- Concurrent `CloseGracefully`/`Abort` idempotence
- Serialized FIFO for a single producer

### Adapters migrated

| Adapter | Ownership model |
|---|---|
| Codex | `EventStream`; protocol reader graceful close; terminate aborts |
| Grok | `EventStream`; reader + async prompt completion producers; WaitGroup coordination |
| OpenCode | `EventStream`; SSE producer graceful close; `watchExit`/terminate aborts if stalled |
| Fake | `EventStream`; initial + follow-up batch producers; terminate aborts |
| Claude | **Not migrated** — single `readProcess` goroutine is the sole sender/closer of `Session.Events` (already satisfies the invariant) |

**Live authenticated harness smoke was not performed** (concurrency-only PR).

## Phase 4 PR 8.10 — retryable attempt-bound permission delivery

Executed after making native permission delivery durable and session-bound:

```bash
gofmt -w .
go test ./...
go test -race ./...
go test -race ./internal/supervisor/... -count=5
go test -race ./internal/message/... -count=5
go vet ./...
go build ./cmd/subagent-broker
```

Results: all packages PASS; race stress used `-count=5` for supervisor/message (reduced from 20 for runtime); `go vet` clean; build success.

### Coverage

| Area | Tests |
|---|---|
| Retryable delivery | failure remains `Queued` with frozen `Resolution`; identical retry succeeds; conflicting retry rejected |
| Binding | harness/session/worker/attempt mismatch never calls adapter; attempt-1 not delivered to attempt-2 |
| Dedup | full identity tuple; same request id across attempts is distinct |
| Legacy journals | `AttemptNumber == 0` is not a wildcard |
| Router | `RecordResolutionIntent` idempotent/conflict; delivery attempts clear error on success |
| PR 8.7–8.9 | permission wire, next-turn, EventStream regressions remain green |

Crash window is **at-least-once** (intent persisted before adapter send; retry may resend). **Not exactly-once.**

**Live authenticated permission retry smoke was not performed.**

## Phase 4.9.0 — control-plane auth and multi-turn linearizability

```bash
gofmt -w .
test -z "$(gofmt -l .)"
go test ./...
go test -race ./...
go test -race ./internal/adapter/... ./internal/message/... ./internal/supervisor/... -count=5
go vet ./...
go build ./cmd/subagent-broker
```

GitHub Actions (`.github/workflows/ci.yml`) runs the same core checks on `pull_request` and `push` to `main` (Linux only; process-tree support is Linux-specific).

Security notes:

* Control and Worker Unix sockets are separate; method authorization is role-based.
* Control credentials use `crypto/rand` (256-bit) and are **persisted** under `control/auth.token` (0600) for CLI access across supervisor restarts; never Worker env/argv.
* Worker credentials are process-lifetime credentials, env-injected only, and revoked on attempt end; they must **not** be persisted.
* Application-level tokens do not provide hard isolation from every malicious same-UID process.
* True protection of Broker Home from a malicious Worker requires a later OS sandbox / separate security principal.
* PR 9.1 removes accidental argv leakage of Worker tokens but does not claim full same-UID confidentiality.
* Permission delivery remains **at-least-once** across the crash window (not exactly-once).

**Live authenticated harness smoke was not performed.**

## Phase 4.9.1 — decision correctness, credential hygiene, and Supervisor ownership

Executed after fixing lost-wakeup races, unifying resolution semantics, removing credential leaks, and adding Supervisor lease:

```bash
gofmt -w .
test -z "$(gofmt -l .)"
go test ./...
go test -race ./...
go test -race ./internal/adapter/... ./internal/message/... ./internal/supervisor/... -count=5
go test -race ./internal/supervisor \
  -run 'TestRequestMessage|TestResolve|TestScope|TestSupervisorLease' \
  -count=50
go test -race ./internal/adapter/opencode \
  -run 'Test.*Turn|Test.*Idle|Test.*Historical|Test.*Generation' \
  -count=50
go vet ./...
go build ./cmd/subagent-broker
```

### Security notes (updated):

* Control credentials are **persisted** for CLI access across supervisor restarts.
* Worker credentials are process-lifetime and must not be persisted.
* Application-level tokens do not provide hard isolation from every malicious same-UID process.
* True protection of Broker Home from a malicious Worker requires a later OS sandbox / separate security principal.
* PR 9.1 removes accidental argv leakage of Worker tokens (previously present in `--mcp-config` JSON and hook command).
* Permission and instruction delivery remain **at-least-once** across documented crash windows (not exactly-once).
* No live authenticated smoke claim unless actually executed.

**No live authenticated harness smoke was performed.**
