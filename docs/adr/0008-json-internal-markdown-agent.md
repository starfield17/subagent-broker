# ADR-0008: Use JSON internally and Markdown for Agent-facing artifacts

- Status: Accepted
- Date: 2026-07-12

## Context

State machines and recovery need structured data, while the Main Agent should not consume large raw JSON streams for routine decisions.

## Decision

Adapters normalize native activity into versioned structured events and envelopes. After schema and semantic validation, the broker atomically publishes concise Markdown questions, reports, status, and Run summaries.

## Consequences

Markdown is never replayed as internal state. The existence of a formal Markdown file is a publication guarantee, not a verification guarantee.
