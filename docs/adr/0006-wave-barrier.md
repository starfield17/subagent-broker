# ADR-0006: Coordinate parallelism with Wave and Barrier

- Status: Accepted
- Date: 2026-07-12

## Context

Unbounded task concurrency makes shared-workspace verification unreliable and permits hidden dependencies between partial implementations.

## Decision

A Run contains ordered Waves. Only logically independent tasks execute in one Wave. Before the next Wave, a Barrier waits for writes to stop, collects reports, audits scopes and diffs, performs integration checks, and records a result.

## Consequences

Parallelism is explicit and reviewable. A later Wave cannot start after a failed or blocked Barrier unless the Main Agent explicitly resolves it.
