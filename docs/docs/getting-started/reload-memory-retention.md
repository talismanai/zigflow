# Workflow reload memory retention

Watch reloads stop all workers before clearing Temporal's process-wide sticky
workflow cache, then start the replacements. Suspended executions remain in
Temporal and resume by replaying their histories. Reloads must preserve workflow
names and replay compatibility for executions that are still open.

Definitions are loaded, validated and built before stopping the current workers.
Invalid replacements leave the current generation running. If replacement startup
fails, Zigflow rebuilds the last successfully started catalogue. If that recovery
also fails, watch mode returns an error so the process can be restarted. It does
not report a successful reload with no workers.

## Evidence behind the change

The investigation used deployed commit `a3d40cb876bbdbd36859d9e8a5a579e12946a6b8`,
Go 1.26.4 and Temporal Go SDK 1.46.0, including the activity-registry cleanup from
[PR #1](https://github.com/talismanai/zigflow/pull/1).

### Production heap profile

The US production secondary worker ending in `qq55t` had no process restart
between the observed RSS increase from 1.50 GiB at 19:30 BRT to 5.89 GiB at
22:00 BRT on 23 November 2026. A settled, forced-GC profile was captured at
**2026-11-24 01:16:10 UTC** (22:16 BRT), more than ten minutes after its last
logged reload at 22:05:40 BRT. The profile reported:

| Measurement | Value |
| --- | ---: |
| Sampled live heap (`inuse_space`) | 3455.76 MiB |
| Cumulative allocations through `handleDebounce` | 3111.28 MiB (90.03%) |
| Cumulative allocations through `loadWorkflows` | 2396.09 MiB (69.34%) |
| Cumulative allocations through `buildWorkersByTaskQueue` | 836.30 MiB (24.20%) |
| Cumulative allocations through `DoTaskBuilder.Build` | 796.64 MiB (23.05%) |
| Runtime heap allocation after collection | 3,669,681,072 bytes |
| RSS after collection | 6,571,622,400 bytes |
| Goroutines | 573 |

Cumulative rows overlap and must not be added together. These are allocation
stacks, not object ownership paths. They identify the retained definitions and
builders; source inspection and the controlled reproduction below establish why
those objects remain reachable. The profile does not prove that every byte of RSS
or every future memory increase has the same cause. No production cache was
purged or worker restarted for this experiment.

### Reference chain and the earlier fix

In SDK 1.46.0 the shared sticky cache retains execution contexts. Each context
references its workflow task handler, which references the worker registry. The
registry holds all registered workflow closures. Zigflow's closures capture task
builders and their workflow definitions. One idle execution can therefore retain
an entire retired catalogue, including definitions that never ran.

Relevant SDK source:

- [Shared worker cache and purge](https://github.com/temporalio/sdk-go/blob/v1.46.0/internal/internal_worker_cache.go)
- [Execution context and task handler](https://github.com/temporalio/sdk-go/blob/v1.46.0/internal/internal_task_handlers.go)
- [Worker shutdown](https://github.com/temporalio/sdk-go/blob/v1.46.0/internal/internal_worker.go)
- [Public purge precondition](https://github.com/temporalio/sdk-go/blob/v1.46.0/worker/worker.go)

The earlier fix released Zigflow's separate activity-registration map. Its heap
experiment registered definitions and reloaded workers, but never started a
workflow execution. Consequently it did not populate this sticky-cache retention
path. That cleanup remains necessary and is preserved.

A diagnostic using three real worker generations on the deployed source retained
all six definitions after stopping every worker and running GC, including three
never-executed definitions with 16 MiB synthetic payloads. Purging the cache then
released all six and reduced `HeapAlloc` from 57.32 MiB to 6.47 MiB. Merely changing
the start/stop order is therefore insufficient.

### Regression and negative control

`TestReloadSuspendedWorkflows` uses real Zigflow YAML loading, task builders,
`handleDebounce` and a local Temporal server. It creates four generations, with
one signal-suspended execution and one unused definition per generation. Only the
unused definition changes, so existing histories remain replay-compatible. Each
unused definition contains an 8 MiB synthetic payload to make retained catalogues
visible in heap profiles.

Before sending any signals, the test uses weak references to assert that retired
definitions are collectible. It then resumes all four executions and checks the
exact signal outputs, original run IDs, completed histories and absence of failed
workflow tasks or continue-as-new events. The oldest execution crosses three
reloads. A second integration test injects replacement startup failure after polling starts, rebuilds
the previous catalogue and resumes a real suspended execution on that rebuilt
worker. Both tests passed three consecutive runs with the race detector enabled.
`task e2e` includes these tests.

A negative control replaces only the cache-purge function with a no-op using a Go
source overlay. It keeps the new stop-before-start order and all the same test
assertions. Results from Go 1.26.4, SDK 1.46.0 and local Temporal 1.7.0:

| Generation | Control heap (MiB) | Fixed heap (MiB) | Control retired definitions alive | Fixed retired definitions alive |
| --- | ---: | ---: | ---: | ---: |
| 0 | 19.78 | 20.50 | 0/0 | 0/0 |
| 1 | 26.43 | 20.74 | 2/2 | 0/2 |
| 2 | 35.50 | 19.89 | 4/4 | 0/4 |
| 3 | 44.49 | 19.82 | 6/6 | 0/6 |

The negative control fails all three collection assertions; the fixed test passes.
All four executions resume correctly in both runs. The final sampled heap
attributes 32.00 MiB to `loadWorkflows` in the control, versus 8.00 MiB with the
fix: four synthetic catalogues versus the one current catalogue. Exact byte
counts depend on sampling and runtime bookkeeping; the regression asserts object
collection and execution correctness, not a fragile heap threshold.

To reproduce against an isolated local Temporal server:

```sh
mkdir -p /tmp/zigflow-reload-profiles
TEMPORAL_ADDRESS=127.0.0.1:7233 \
  ZIGFLOW_PROFILE_DIR=/tmp/zigflow-reload-profiles \
  go test -tags=e2e ./cmd/run -run '^TestReloadSuspendedWorkflows$' -count=1 -v
go tool pprof -top -cum -sample_index=inuse_space \
  /tmp/zigflow-reload-profiles/reload-03.heap.pb.gz
```

For a negative control, copy `cmd/run/workers.go` outside the checkout, replace
`var purgeStickyWorkflowCache = worker.PurgeStickyWorkflowCache` with
`var purgeStickyWorkflowCache = func() {}`, and create a Go overlay JSON mapping
the absolute original path to that copy under `Replace`. Repeat the command with
`go test -overlay=/absolute/path/to/overlay.json ...`, using a separate profile
output directory. Expect a failing collection regression. Do not deploy the
negative control.

## Production tradeoffs and verification

- There is a polling gap between stopping the previous workers and starting the
  replacements. Shutdown can wait for running activities up to the configured
  graceful shutdown timeout. An activity interrupted at that boundary remains
  subject to its existing Temporal retry and timeout settings.
- The cache starts cold after each reload. Resuming executions replay history,
  increasing CPU and task latency temporarily. Frequent reloads or long histories
  can make this expensive. Loading and building replacements still temporarily
  overlaps the current catalogue in memory.
- Cache eviction does not make incompatible edits to active workflows safe, and
  deleting a definition still prevents its open executions from being processed
  by these workers. Use the existing versioning/compatibility discipline.
- The purge is process-wide and requires every worker to be stopped. The watch
  lifecycle owns all workers in this process. Any future independently managed
  worker must join that lifecycle before this assumption remains safe.
- Go may keep freed pages for reuse. RSS need not fall immediately. Compare
  post-GC live heaps on the same pod/process across several reloads, alongside
  replay failures, task latency, backlog, CPU and activity retries. Start with a
  limited rollout and verify production behaviour before expanding it.

The local experiment proves this specific retention path and replay-compatible
resumption. A production rollout and repeated post-GC profiles are still required
to establish how much of the observed production growth this fix removes.
