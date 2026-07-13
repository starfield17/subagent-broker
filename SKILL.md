---
name: subagent-broker
description: Coordinate parallel subagents through the subagent broker.
---

# Multi-Harness Parallel Subagent Broker Skill

## Status

This repository is a **Phase 4 runtime**. It executes ordered Waves of same-checkout Tasks through four native Harness Adapters and provides Barrier verification plus a persistent Main Agent/Worker message path.

## Main Agent responsibilities

The Main Agent owns decomposition, parallel-safety decisions, Task Contracts, cross-task design choices, answers to Worker questions, and final verification.

Only place tasks in the same Wave when:

- write scopes do not overlap;
- no task depends on another task's expected output in that Wave;
- no shared public interface or global dependency file is being changed by multiple tasks;
- each task has meaningful local validation;
- partial work from one task cannot invalidate another task's validation.

## Task Contract minimum

Every task must state:

1. objective and completion criteria;
2. allowed write paths/globs;
3. forbidden paths or global objects;
4. known read dependencies;
5. responsibilities of parallel tasks;
6. whether public-interface changes are allowed;
7. required local validation;
8. scope-expansion behavior;
9. final report requirements;
10. prohibited Git operations;
11. prohibition on nested subagents;
12. project root, Run ID, and Task ID.

Workers may read the project, but may only write within the approved scope. When scope is insufficient, the Worker must stop the out-of-scope edit and submit a scope-expansion request.

## Architectural invariants

- One logical Task may have multiple WorkerSessions.
- Same-Wave write scopes must not overlap.
- The Supervisor is the sole runtime writer of global Run state.
- Events are append-only and Run-sequenced.
- Formal Markdown is atomically published only after validation.
- `report.md` means a valid report exists, not that verification succeeded.
- A durably recorded Task execution failure must terminalize its Wave and Run before the Supervisor exits normally.
- Only Tasks that actually started are Worker recovery candidates. Never-started Tasks retain their pre-start state.
- Waiting for user, permission, or scope is not a stall.
- Suspected stall is not an automatic kill condition.
- Git describes code changes; it is not the control protocol or liveness oracle.
- Broker state stays outside the project repository.
- V1 does not create worktrees and does not allow nested agents by default.
- Adapter capability declarations must be truthful.

## Operator workflow

Build and probe all available Harnesses before dispatching:

```bash
go build -o /tmp/subagent-broker ./cmd/subagent-broker
/tmp/subagent-broker doctor
```

Dispatch accepts the legacy Task array as one Wave or an ordered plan containing `waves`, per-Wave `integration_checks`, and optional `final_checks`.

```bash
/tmp/subagent-broker dispatch \
  --project /path/to/project \
  --goal "Complete the requested task" \
  --tasks /path/to/tasks.json \
  --permission-mode acceptEdits \
  --model sonnet
```

The command starts a detached Supervisor and prints the Run ID. The Supervisor is the only writer of Run state and owns each Harness process/session. Inspect or control the Run with:

```bash
/tmp/subagent-broker status --project /path/to/project --run <run-id>
/tmp/subagent-broker wait --project /path/to/project --run <run-id>
/tmp/subagent-broker collect --project /path/to/project --run <run-id>
/tmp/subagent-broker events --project /path/to/project --run <run-id>
/tmp/subagent-broker cancel --project /path/to/project --run <run-id>
/tmp/subagent-broker inbox --project /path/to/project --run <run-id>
/tmp/subagent-broker send --project /path/to/project --run <run-id> --message <message-id> --answer "answer"
```

If the Supervisor itself stops before the Run is terminal, start reconciliation with `recover`. Recovery never treats a reused PID as the original Worker without a matching process start token.

Phase 4 supports `claude-code`, `codex`, `grok-build`, and `opencode` at runtime. Put `harness_preference` on a Task to override the Run default; mixed Harnesses in one Run are supported. Claude questions/scope requests use Broker-provided MCP tools, while permission, steering, history, diff, and usage are routed through each native protocol where available. The Fake Harness remains the deterministic lifecycle test adapter for unit and race tests.
