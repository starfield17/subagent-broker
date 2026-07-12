# ADR-0003: Store runtime data in Broker Home

- Status: Accepted
- Date: 2026-07-12

## Context

Logs, events, IPC metadata, questions, and reports are operational state. Writing them into the user's repository risks Git pollution, accidental commits, and leakage.

## Decision

Operational state is stored under `BROKER_HOME`, defaulting to `~/.subagent-broker` or the platform-equivalent user location. Source installation and state storage remain separate concepts.

## Consequences

The repository is not modified with `.subagent` directories. Broker Home must use user-only permissions, retention rules, and credential redaction.
