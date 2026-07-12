# Phase 3 Coverage

| Deliverable | Implementation | Verification |
|---|---|---|
| Persistent inbox | append-only message store and `inbox` CLI | replay tests and real question smoke |
| Question and answer | Worker MCP `ask_main_agent` plus `send --answer` | blocking request integration test and real Claude smoke |
| Direct send | capability-aware active-turn delivery | Adapter/Fake capability tests |
| Scope expansion | Worker MCP request, conflict preflight, atomic Contract/lease update | real approved-scope smoke and Barrier audit |
| Permission routing | Claude `PreToolUse` gate bridged through Supervisor messages | real Bash approval smoke |

The Broker reports unsupported delivery rather than replacing an unavailable native session with a new unrelated Agent.
