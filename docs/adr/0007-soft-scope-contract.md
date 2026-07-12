# ADR-0007: Enforce scope as a soft contract

- Status: Accepted
- Date: 2026-07-12

## Context

OS ACLs, container mounts, and proxy filesystems would add platform and tooling complexity. They also cannot decide semantic ownership.

## Decision

Task Contracts declare allowed write scopes. The broker performs conservative preflight overlap checks, audits structured file events, and compares Run-level changed files with the union of approved scopes. Out-of-scope work requires a persisted expansion request.

## Consequences

Scope is cooperative rather than a sandbox, but violations are explicit. When evidence cannot identify an owner, the result is `scope_violation_owner_uncertain`, never a fabricated attribution.
