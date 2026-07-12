# ADR-0010: Forbid nested agents by default in V1

- Status: Accepted
- Date: 2026-07-12

## Context

The broker already provides one orchestration layer. Nested subagents obscure ownership, exceed concurrency budgets, complicate cancellation, and weaken scope inheritance.

## Decision

`allow_nested_agents` is false and Phase 0 preflight rejects a Task that enables it. Task Contracts prohibit invoking this broker, native Harness subagents, or another orchestrator.

## Consequences

Task execution remains a single auditable Worker layer. A future nested-agent feature requires depth limits, inherited scope, event ancestry, and cancellation semantics.
