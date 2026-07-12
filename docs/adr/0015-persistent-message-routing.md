# ADR-0015: Route Worker interaction through persistent Broker messages

- Status: Accepted
- Date: 2026-07-12

## Decision

Questions and scope requests use two typed tools exposed by a run-local stdio MCP server implemented in the Broker binary. Claude tool permissions use a run-local `PreToolUse` gate because non-interactive print mode does not invoke an interactive permission dialog. Every request is persisted before the Worker waits.

## Consequences

The Main Agent may answer through `inbox` and `send` while the original Claude turn remains blocked. Scope approval updates the authoritative Task Contract before the Worker continues. Other Harnesses can use their native permission APIs in later phases without changing the message model.
