# ADR-0005: Do not use Git worktrees in V1

- Status: Accepted
- Date: 2026-07-12

## Context

Independent worktrees postpone textual conflicts but do not solve semantic dependencies, public-interface races, or incompatible design decisions.

## Decision

All writing Workers share the same project checkout. Same-Wave tasks must have non-overlapping write scopes and no expected-output dependency. The broker never creates a worktree as an automatic conflict escape hatch.

## Consequences

Parallel safety is solved during task decomposition and preflight. Speculative competing implementations require a separate future mode and ADR.
