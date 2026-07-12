# ADR-0002: Use a Run-scoped Supervisor

- Status: Accepted
- Date: 2026-07-12

## Context

Workers require parent-managed bidirectional I/O, reliable cancellation, state ownership, and persistent events. A global daemon adds unrelated lifecycle and security complexity; detached processes lose control.

## Decision

Each Run receives one temporary Supervisor. It is the sole runtime writer of global Run state, owns Worker sessions for that Run, persists transitions, and exits after the Run reaches a terminal state.

## Consequences

Future CLI commands communicate with a Run-bound local IPC endpoint. Snapshot-only degraded diagnostics remain possible when the Supervisor is unavailable.
