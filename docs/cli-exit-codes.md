# CLI outcome and exit codes

The CLI uses one taxonomy for target results and command errors:

| Code | Meaning |
| ---: | --- |
| 0 | success |
| 2 | usage/configuration error |
| 3 | run, Wave, or Task not found |
| 10 | preflight failed |
| 20 | partial |
| 21 | blocked or warning acceptance is pending |
| 22 | failed or verification failed |
| 23 | cancelled |
| 24 | wait timeout |
| 25 | Supervisor communication error; non-terminal disk fallback is degraded |
| 26 | compatibility/capability unavailable |
| 70 | internal error |

`status`, `events`, and `wait` use the same IPC-first read client. A disk
fallback always reports `data_source=disk`, `mode=degraded`, and a reason. A
terminal Run may be reported from its durable snapshot; a non-terminal Run is
never treated as live after Supervisor communication is lost.

Barrier decisions are Supervisor operations:

```text
subagent-broker barrier show   --run RUN --wave WAVE
subagent-broker barrier accept --run RUN --wave WAVE --reason "..."
subagent-broker barrier reject --run RUN --wave WAVE --reason "..."
```

The CLI sends accept/reject over IPC. The Supervisor validates the current
verification and input hash, records actor/reason, and automatically resumes
Barrier evaluation and Wave dispatch after an accepted warning or a resolved
pending decision.
