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
- External Harness output is normalized at the Adapter protocol boundary before entering the canonical Result Envelope model.
- Question answers and permission/scope decisions are disjoint resolution types.
- Absence of a decision must never be interpreted as denial.
- A durably recorded Task execution failure must terminalize its Wave and Run before the Supervisor exits normally.
- Only Tasks that actually started are Worker recovery candidates. Never-started Tasks retain their pre-start state.
- Waiting for user, permission, or scope is not a stall.
- Suspected stall is not an automatic kill condition.
- Git describes code changes; it is not the control protocol or liveness oracle.
- Broker state stays outside the project repository.
- Every workspace change remains observable in Workspace Snapshots and
  ChangedFiles.
- Changed paths are classified as authorized, ephemeral, unauthorized, or
  owner-uncertain. Ephemeral does not mean ignored or Task-owned.
- Failure evidence records observed workspace and process facts. It is not a
  Result Envelope, formal verification result, or automatic adoption of
  residual files.
- Failure evidence and cache observations are retained under Broker storage;
  no workspace cleanup is performed automatically.
- V1 does not create worktrees and does not allow nested agents by default.
- Adapter capability declarations must be truthful.

## Operator workflow

Build and statically probe all available Harnesses before dispatching. The
ordinary `doctor` command does not start an authenticated model session:

```bash
go build -o /tmp/subagent-broker ./cmd/subagent-broker
/tmp/subagent-broker doctor
```

Doctor reports evidence levels separately. A declared or probe-reported
capability is not runtime-verified, and a requested model is not an observed
runtime model. Unknown provider/model identity remains unknown. Doctor never
prints or persists authentication tokens, API keys, raw authorization headers,
full environments, or control/Worker credentials.

The opt-in live smoke makes one authenticated model request per selected
Harness, so it may consume tokens, incur provider cost, and require network
access. Each smoke uses a fresh isolated temporary workspace and removes it
only after cleanup is confirmed. Use `--keep-workspace` to retain it for
inspection:

```bash
subagent-broker doctor --harness all

subagent-broker doctor \
  --harness codex \
  --smoke \
  --timeout 2m

subagent-broker doctor \
  --harness claude-code \
  --smoke \
  --model <model-name> \
  --keep-workspace
```

Live smoke defaults are `--smoke=false`, `--timeout=2m`, and
`--keep-workspace=false`. `--executable` and `--model` require a single
Harness; `--harness all --smoke` runs selected Harnesses serially. The basic
smoke exercises session startup, structured events, the current-turn terminal
boundary, structured final output, result Task/Worker binding, and cleanup.
It does not verify resume, steering, bidirectional streaming, permissions,
interrupts, cancellation, diffs, usage, hooks, history, native subagents,
native server mode, or ACP-specific behavior.

Dispatch accepts the legacy Task array as one Wave or an ordered plan containing `waves`, per-Wave `integration_checks`, and optional `final_checks`.

```bash
/tmp/subagent-broker dispatch \
  --project /path/to/project \
  --goal "Complete the requested task" \
  --tasks /path/to/tasks.json \
  --ephemeral-path "generated-cache/**" \
  --permission-mode acceptEdits \
  --model sonnet
```

Dispatch freezes the normalized audit policy into the Run configuration.
The narrow built-in ephemeral patterns are:

- `**/__pycache__/**`
- `**/.pytest_cache/**`
- `**/*.pyc`

Additional project-relative patterns may be supplied with repeatable
`--ephemeral-path`. These patterns classify observations; they do not exclude
files from workspace capture or authorize Task deliverables. No automatic
cleanup is performed.

The command starts a detached Supervisor and prints the Run ID. The Supervisor is the only writer of Run state and owns each Harness process/session. Inspect or control the Run with:

```bash
/tmp/subagent-broker status --project /path/to/project --run <run-id>
/tmp/subagent-broker wait --project /path/to/project --run <run-id>
/tmp/subagent-broker collect --project /path/to/project --run <run-id>
/tmp/subagent-broker events --project /path/to/project --run <run-id>
/tmp/subagent-broker cancel --project /path/to/project --run <run-id>
/tmp/subagent-broker inbox --project /path/to/project --run <run-id>
subagent-broker send ... --message <question-id> --answer "..."
subagent-broker send ... --message <permission-id> --approve
subagent-broker send ... --message <permission-id> --deny --reason "..."
subagent-broker send ... --message <scope-id> --approve
subagent-broker send ... --message <scope-id> --deny --reason "..."
```

Result Envelope output uses `scope_expansion: null` when no expansion is requested. When present, it is an object with `paths`, `reason`, and `consequence`; it is never an array. Resolution JSON uses a tagged `answer` or `decision` union, and a missing decision is not a denial.

If the Supervisor itself stops before the Run is terminal, start reconciliation with `recover`. Recovery never treats a reused PID as the original Worker without a matching process start token.

Phase 4 supports `claude-code`, `codex`, `grok-build`, and `opencode` at runtime. Put `harness_preference` on a Task to override the Run default; mixed Harnesses in one Run are supported. Claude questions/scope requests use Broker-provided MCP tools, while permission, steering, history, diff, and usage are routed through each native protocol where available. The Fake Harness remains the deterministic lifecycle test adapter for unit and race tests.
