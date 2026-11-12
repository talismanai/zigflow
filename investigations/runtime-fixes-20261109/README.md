# Combined v0.14.2 runtime patch evidence

Local experiments on 9 November 2026. No production, development or staging
services were used. All workflows are synthetic.

## Scope and provenance

This PR contains the worker-retention fix and its synthetic profiling harness.
The recorded measurements were collected before the work was split into PRs,
using the combined local patch based on v0.14.2. That patch also contained the
schedule-listing optimisation; the heap experiment calls `launchWorkers`,
which does not run schedule reconciliation. The profiling harness and memory
cleanup code are unchanged in this PR.

## Changes in the measured combined build

1. Release activity-registration keys when workers stop or fail to build/start.
2. Share one paginated schedule listing during startup reconciliation.

The base is upstream v0.14.2. The Go module and dependency lock files are
unchanged. Workflow execution builders and workflow/activity names are unchanged.
The duplicate subworkflow naming mitigation belongs to a separate change.

## Heap experiment

`cmd/run/retention_profile_test.go` is an opt-in experiment. It generates 955
minimal HTTP-task definitions, starts a real worker against a local Temporal
server, then performs ten worker replacements using the same `launchWorkers`
and `stopWorkerList` functions used by watch mode. No workflow executions are
started, so no HTTP task is executed. It does not simulate filesystem events
or a production workload running for days.

Each snapshot forces GC, reads `runtime.MemStats`, and records heap allocation,
object count and GC count. Snapshots 0 and 10 also write a sampled Go heap
profile with `runtime/pprof`. Both runs used Go 1.27.0 on macOS arm64; the
container build separately uses Go 1.26.4 for Linux amd64.

| Build | Initial heap MiB | Heap after 10 replacements MiB | Net growth MiB |
| --- | ---: | ---: | ---: |
| Original v0.14.2 | 13.23 | 40.84 | 27.61 |
| Combined patch | 14.67 | 16.77 | 2.10 |

The samples are in `before/heap.csv` and `after/heap.csv`; raw profiles are in
the same directories. The unpatched heap-object count rose from 47,397 to
291,415; the patched count changed from 47,342 to 54,755. GC continued in
both runs. This supports removal of the retained-worker growth mechanism.
It does not prove the absence of every leak, or predict production RSS.

The baseline profile attributes retained allocations to Temporal worker
structures, workflow/activity registries and parsed workflow builders.
Sampled profile totals can differ from MemStats and include profiling/runtime
allocations. Use the CSV for the totals in the table, and profiles for
allocation-site attribution; profiles do not directly show retaining edges.

## Reproduce the heap comparison

Start a dedicated local Temporal server, for example:

```sh
docker run --rm --name zigflow-retention-temporal \
  -p 127.0.0.1:17233:7233 temporalio/temporal:latest \
  server start-dev --ip 0.0.0.0
```

In another terminal, from this checkout:

```sh
mkdir -p /tmp/zigflow-heap-after
ZIGFLOW_PROFILE_ADDRESS=127.0.0.1:17233 \
ZIGFLOW_PROFILE_DIR=/tmp/zigflow-heap-after \
go test -tags=profile ./cmd/run -run '^TestWorkerRetentionProfile$' -count=1 -v
```

For the baseline, archive v0.14.2 into a fresh directory, copy only
`cmd/run/retention_profile_test.go` there, and run the same command with a
separate output directory. The test refuses non-loopback addresses. Run the
two variants sequentially against the dedicated server.

Inspect allocation sites with:

```sh
go tool pprof -top -sample_index=inuse_space \
  -base before/reload-00.heap.pb.gz before/reload-10.heap.pb.gz
go tool pprof -top -sample_index=inuse_space \
  -base after/reload-00.heap.pb.gz after/reload-10.heap.pb.gz
```

A standalone `pprof` binary can be used if the installed Go distribution does
not include that tool. This PR includes only synthetic-workflow evidence.

The separate schedule-listing PR contains its own RPC experiment and evidence.
