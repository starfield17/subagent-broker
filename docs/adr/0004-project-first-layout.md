# ADR-0004: Organize Broker Home project-first

- Status: Accepted
- Date: 2026-07-12

## Context

The same project may have many Runs, and shell path strings are not stable identities because of relative paths, symlinks, work directories, and platform differences.

## Decision

Broker Home is organized as `projects/<readable-slug>--<path-hash>/runs/<run-id>`. Project identity is based on a canonical project or Git-root path.

## Consequences

Run lookup can start from the current project. Human-readable slugs aid diagnostics while a hash preserves uniqueness.
