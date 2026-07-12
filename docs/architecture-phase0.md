# Phase 0 Architecture

## Dependency direction

```text
project/run/wave/task/message/report/process
                  │
                  ▼
              domain/state
                  │
                  ▼
adapter/fake -> adapter -> event/report
                  │
                  ▼
         supervisor boundaries
                  │
                  ▼
             storage files
```

Adapters do not write Run state. They expose native session operations and normalize events; a later Run-scoped Supervisor will validate transitions and become the single writer.

## Publication rule

For a formal question or report, the structured object is validated first. Metadata is atomically written, then Markdown is atomically renamed into place as the publication marker. An invalid object never creates the formal Markdown path.

## Parallel-safety rule

Scope overlap detection is intentionally conservative. Compatible static prefixes are treated as possibly overlapping unless disjointness is provable. False-positive rejection is acceptable; a false-negative same-Wave write conflict is not.

## Phase boundary

No production Runtime or CLI is hidden behind these interfaces. Fake Harness tests prove the contracts before any real Harness Adapter is added.
