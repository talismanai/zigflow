---
sidebar_label: Memory profiling
---

# Memory profiling

The Talisman maintenance build supports an optional profiling server. It lets
operators capture Go allocation stacks from the running worker, including across
watch reloads. This instrumentation diagnoses memory retention; it does not fix
retention or limit memory consumption.

## Enable the endpoint

Add this argument to the worker's existing `run` command:

```text
--pprof-listen-address=127.0.0.1:6060
```

The equivalent environment variable is `PPROF_LISTEN_ADDRESS`. An empty value
(the default) disables the server. Only literal loopback IPs are accepted;
`0.0.0.0`, pod IPs and hostnames are rejected. IPv6 loopback `[::1]:6060` is also
accepted when supported by the host. The address must bind successfully before
workflow registration begins. Shutdown closes the listener, including on startup
failure. Watch reloads reuse the same process and profiling server.

Only these GET endpoints are exposed on the profiling listener:

| Endpoint | Purpose |
| --- | --- |
| `/debug/pprof/heap` | Sampled live allocations and cumulative allocation counts |
| `/debug/pprof/allocs` | Same memory samples, defaulting to cumulative allocations |
| `/debug/pprof/goroutine` | Current goroutine stacks |

The health and Prometheus servers use separate HTTP routers. CPU profiling,
execution tracing, command-line and index endpoints are not exposed on this
listener. The standard Go profiling package also registers handlers on the
process's default router, so future HTTP servers must continue to use explicit
routers rather than `http.DefaultServeMux`.

The endpoint has no authentication. Loopback is shared by containers in the pod;
access should be limited through Kubernetes permissions. Do not add a Service or
Ingress for it. Treat profiles as internal diagnostic data.

## Build and rollout

Build the reviewed maintenance commit with `Dockerfile.talisman`, publish an
immutable image tag and update GitOps through a separate PR. Enable profiling on
one selected worker first. Changing a Deployment's arguments enables it for all
its replicas; an individual canary needs a separate Deployment.

Linking the heap handler keeps allocation sampling reachable in the binary.
Sampling starts with the process, even when the HTTP endpoint is disabled, and
has a runtime cost. The Go default samples approximately one allocation per
512 KiB allocated. This change does not alter the sample rate, GC configuration
or compiler optimisations. With `GODEBUG=memprofilerate=0`, enabling the endpoint
fails explicitly instead of returning misleading empty memory profiles. Startup
logs report the bound address and actual `memorySampleRate`.

Debug symbols alone do not enable memory sampling. The previous build had symbols
but its runtime reported `MemProfileRate=0`. Profiles cannot reconstruct
allocations made before sampling was active. This requires a newly built image
and a worker restart, rather than attaching another debugger container.

## Capture before and after growth

On your workstation, select a running pod using the instrumented image:

```bash
kubectl -n zigflow-prod get pods
kubectl -n zigflow-prod port-forward pod/POD_NAME 6060:6060
```

In another terminal, capture a baseline:

```bash
curl --fail --max-time 60 -o before.heap.pb.gz   'http://127.0.0.1:6060/debug/pprof/heap?gc=1'
curl --fail --max-time 60 -o before.goroutine.pb.gz   'http://127.0.0.1:6060/debug/pprof/goroutine'
```

After a reload or a comparable interval of real work, capture again from the
**same process**:

```bash
curl --fail --max-time 60 -o after.heap.pb.gz   'http://127.0.0.1:6060/debug/pprof/heap?gc=1'
curl --fail --max-time 60 -o after.goroutine.pb.gz   'http://127.0.0.1:6060/debug/pprof/goroutine'
```

`gc=1` requests a garbage collection before capture. Use occasional snapshots;
it can add latency. Without it, heap samples reflect the most recent completed
GC. Record UTC timestamps, pod UID, restart count, image digest, reload timestamps
and workload volume. A rollout or restart invalidates a same-process comparison.

Analyse locally using the matching executable when available:

```bash
go tool pprof -top -sample_index=inuse_space   -base before.heap.pb.gz after.heap.pb.gz
go tool pprof -top -sample_index=inuse_objects   -base before.heap.pb.gz after.heap.pb.gz
go tool pprof -top -sample_index=alloc_space   -base before.heap.pb.gz after.heap.pb.gz
go tool pprof -http=127.0.0.1:8081 -sample_index=inuse_space   -base before.heap.pb.gz after.heap.pb.gz
```

- Growing `inuse_space` identifies allocation stacks with more retained bytes.
- Growing `inuse_objects` identifies stacks retaining more objects.
- `alloc_space` differences show allocation churn during the interval, including
  objects already collected. High churn alone does not prove a leak.
- Goroutine profiles can reveal old worker goroutines that remain after reload.

Heap profiles identify allocation sites, not the references keeping objects
alive. Compare several post-GC snapshots under similar load, alongside sticky
cache size, goroutines, reloads and RSS. Go heap samples do not account for every
resident page, native allocation, runtime overhead or container file cache.
