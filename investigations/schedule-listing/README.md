# Schedule listing amplification

## Root cause and change

In v0.14.2, `runRunCmd` calls `runScheduleUpdates` once during startup.
That function calls `UpdateSchedules` for each loaded definition, including
those without a schedule. `deleteOldSchedules` walks an unfiltered SDK list
iterator, issuing `ListSchedules` RPCs for all pages on every definition.

This patch shares one `ScheduleUpdater` within that startup pass. It loads all
schedule IDs once and updates that local set after successful deletion or
creation. Every subsequent pass gets a new updater. An empty catalog makes no
schedule RPCs. Metadata validation and the single-definition `UpdateSchedules`
API remain available.

The v0.14.2 watch path (`launchWorkers`) does **not** reconcile schedules.
This patch does not add reconciliation to hot reloads.

## Measured results

The committed test uses the actual Temporal Go SDK v1.46.0 over an in-memory
gRPC transport. The server counts RPCs, not SDK method invocations. It is a
controlled protocol test, not a measurement of production server latency.

| Unscheduled definitions | Original ListSchedules RPCs | Patched RPCs |
| ---: | ---: | ---: |
| 0 | 0 | 0 |
| 1 | 1 | 1 |
| 955 | 955 | 1 |
| 1090 | 1090 | 1 |

These rows use an empty schedule list (one page). With P pages and W
workflows, the repeated listing changes from approximately W x P requests
to P requests per startup. Creates and deletes for existing schedules still
occur. Retries can add requests in both versions.

`before.log.gz` and `after.log.gz` contain the raw test output. The baseline
intentionally fails the new one-listing assertion. Tests also cover pagination,
removing a schedule from an unscheduled definition, replacement, creation,
unrelated schedule preservation, shared schedule IDs within one pass, fresh
listings between passes, and errors in listing, deletion and creation.

## Reproduce

From this checkout, with Go installed:

```sh
bash investigations/schedule-listing/reproduce.sh /tmp/zigflow-schedule-evidence
```

The script archives v0.14.2 into a temporary directory, copies the regression
test into it, runs the failing baseline, and runs the passing patched tests.
It preserves that directory and both logs. It uses no external service or
credentials. On macOS, use `TMPDIR=/private/tmp` if other repository tests
encounter the `/var` versus `/private/var` temporary-path issue.

## Deliberate limits

- This does not address duplicate subworkflow names or change workflow commands.
- The listing is a snapshot, not a distributed lock. Concurrent replica
  schedule replacement was already racy and remains outside this patch.
- External changes after the snapshot are observed on the next reconciliation
  pass. The updater tracks its own successful changes immediately.
- Schedule replacement still deletes then creates, preserving v0.14.2 behaviour.
- A page-fetch failure aborts before the snapshot is used to mutate schedules.
- This does not eliminate parsing, validation, worker startup, or probe delays.
