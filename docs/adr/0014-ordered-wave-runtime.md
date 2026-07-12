# ADR-0014: Execute ordered Waves with content-based barriers

- Status: Accepted
- Date: 2026-07-12

## Decision

A Run persists an ordered plan. Tasks in one Wave execute concurrently up to a configured limit. Every Wave captures a content baseline before Workers start and runs a Barrier after all Workers stop writing. The Barrier audits actual file changes against approved leases and runs integration checks before the next Wave starts.

## Consequences

Pre-existing dirty files are not attributed to a Run unless their content changes after the baseline. Failed checks and unauthorized files retain evidence and never trigger automatic rollback.
