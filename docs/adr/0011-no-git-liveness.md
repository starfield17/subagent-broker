# ADR-0011: Do not use Git diff as a liveness oracle

- Status: Accepted
- Date: 2026-07-12

## Context

A Worker may be thinking, compiling, retrying an API call, waiting for permission, or making progress without changing files. Conversely, stale file changes do not prove a process is alive.

## Decision

Liveness and progress use native lifecycle events, protocol states, process identity, streams, tool status, and file activity in that order. Git is limited to baseline snapshots, changed-file sets, scope audit, and verification.

## Consequences

Waiting states are not stalls, quiet thresholds are contextual, and `suspected_stall` never implies automatic termination.
