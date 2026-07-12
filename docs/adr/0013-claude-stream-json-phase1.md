# ADR 0013: Use Claude Code stream-json for the Phase 1 runtime

## Status

Accepted.

## Context

Phase 1 needs one executable Harness path that can be verified locally while preserving the Adapter boundary for later Harness implementations. Claude Code exposes a bidirectional JSONL protocol, structured output, session identifiers, and a process that can be managed as a child process.

## Decision

The first runtime Adapter is Claude Code through its stream-json interface. The Adapter:

- starts Claude in print mode with verbose stream-json input and output;
- sends the Task Contract as a JSONL user message;
- normalizes session, assistant, tool, result, and error messages into Broker events;
- captures stderr separately from the structured stream;
- records the native session ID, PID, and process start token;
- validates the final Result Envelope before publication; and
- terminates the session after a terminal result because the interactive process remains available for another turn.

The Supervisor owns the detached process group, persists state and events, and exposes local JSONL IPC. Phase 1 limits dispatch to one Task so the lifecycle and recovery boundaries can be verified before Wave parallelism is enabled.

## Consequences

The repository can verify a complete dispatch-to-report lifecycle with the installed Claude CLI. Provider configuration and CLI release compatibility remain environment concerns and are reported by `doctor` and the real smoke. Other Harnesses can be added without changing Supervisor state ownership.
