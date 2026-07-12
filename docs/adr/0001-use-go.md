# ADR-0001: Use Go for the execution runtime

- Status: Accepted
- Date: 2026-07-12

## Context

The broker must own scheduling, process control, state transitions, Adapter behavior, persistence, reporting, and CLI semantics across platforms. Splitting the runtime across languages would complicate lifecycle guarantees and deployment.

## Decision

All deterministic runtime code is implemented in Go. Harness CLIs or servers may be invoked externally, but no Python, Node.js, Rust, or shell runtime is used to maintain correctness.

## Consequences

The repository has one build toolchain and one concurrency/runtime model. Platform-specific process code will use Go build constraints in later phases.
