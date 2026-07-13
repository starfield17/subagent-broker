# ADR 0016: Native Phase 4 Harness Adapters

## Status

Accepted.

## Decision

Implement Codex through its App Server JSONL protocol, Grok Build through ACP stdio, and OpenCode through a per-Worker loopback HTTP/SSE server. Keep the existing Adapter interface and normalize native events into the Broker event model.

Task `harness_preference` is authoritative for a Worker; the Run-level Harness is only the default. Persisted Worker Harness identity is authoritative during recovery. Claude-specific MCP and hook installation is not injected into other Harnesses.

## Consequences

- A Run can safely contain Tasks using different Harnesses.
- Capability declarations are adapter-specific and are downgraded when session facts or contract verification do not support them.
- Native server/session processes remain owned by the Supervisor and are terminated through the existing process identity boundary.
- Protocol versions outside the tested range are reported as `compatibility_unverified` rather than silently assumed compatible.
