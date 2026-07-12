# ADR-0012: Use JSONL and atomic snapshots instead of mandatory SQLite in V1

- Status: Accepted
- Date: 2026-07-12

## Context

V1 requires append-only facts, replay, inspectable artifacts, and simple local deployment. A relational database is not yet necessary to prove the lifecycle.

## Decision

Use append-only `events.jsonl`, atomically replaced JSON snapshots, atomically published Markdown, and small JSON indexes. The Supervisor's in-memory state is authoritative while running; events are authoritative for recovery.

## Consequences

The event store must tolerate and isolate an incomplete final line. SQLite may be introduced later only with an ADR and a migration/replay strategy.
