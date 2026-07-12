# ADR-0009: Separate reported completion from verification

- Status: Accepted
- Date: 2026-07-12

## Context

A Worker can produce a coherent report while its implementation fails integration checks, exceeds scope, or leaves hidden incompatibilities.

## Decision

A valid Result Envelope moves a Task to `reported_complete`. Only explicit verification may produce `verified_success`, `verified_partial`, or `verification_failed`.

## Consequences

Process exit code and Worker claims cannot directly mark success. Verification failures preserve evidence and do not trigger automatic rollback.
