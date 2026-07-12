# Multi-Harness Parallel Subagent Broker Skill

## Status

This repository is a **Phase 0 architecture skeleton**. It defines contracts and test fixtures, but intentionally does not expose production dispatch commands or connect to real Harnesses yet.

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
- Waiting for user, permission, or scope is not a stall.
- Suspected stall is not an automatic kill condition.
- Git describes code changes; it is not the control protocol or liveness oracle.
- Broker state stays outside the project repository.
- V1 does not create worktrees and does not allow nested agents by default.
- Adapter capability declarations must be truthful.
