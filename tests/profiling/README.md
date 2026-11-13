# Local profiling smoke test

This test uses synthetic workflow data, a temporary Temporal server and an
internal Docker network. It does not access Kubernetes, cloud credentials or
shared Temporal services. It cleans up its own containers and network on exit.
Python 3 and Docker Compose are required. Download the Temporal and Go test
images beforehand; the smoke test does not pull application images.

```bash
docker pull temporalio/temporal
docker pull golang:1.26.4
docker buildx build --platform linux/amd64 --load \
  -f Dockerfile.talisman \
  --build-arg VERSION=0.14.2-profiling-local \
  -t talisman-zigflow:profiling-local .
python3 tests/profiling/smoke.py --output /tmp/zigflow-profiles
```

The test verifies:

- Startup reports a positive, default sampling rate of 524288 bytes.
- A synthetic workflow executes successfully against local Temporal.
- Heap, allocation and goroutine profiles are available before and after a
  successful watch reload, and the Go 1.26.4 profiler can parse their output.
- Health and metrics listeners return 404 for the heap endpoint.
- CPU, trace, command-line and index routes return 404 on the profiling port.
- A different container cannot access the worker's loopback profiling listener.
- Explicitly disabled sampling causes profiling startup to fail clearly.

The output directory contains the profiles, parsed summaries, worker log,
workflow result and disabled-sampling error. Use a fresh output directory for
each run. Sampling totals vary from run to run; this is an instrumentation test,
not a demonstration that a memory leak is fixed.

Additional unit tests cover disabled-by-default behaviour, address rejection,
bind errors, GET-only routes, shutdown and startup-failure cleanup:

```bash
go test ./cmd/run -run TestProfiling -count=1
go test -race ./cmd/run -run TestProfiling -count=1
```

## Local validation on 13 November 2026

The Docker image built with Go 1.26.4 passed both smoke-test checks:

```text
PASS: workflow, reload, three readable profiles, sampling rate, private listener and port isolation
PASS: disabled allocation sampling rejected at startup
```

The heap summary resolved names such as
`go.temporal.io/sdk/internal.newLocalActivityTunnel` and
`go.temporal.io/sdk/internal.(*baseWorker).runEagerTaskDispatcher`.
This confirms that the instrumented image can collect and symbolise allocations.
It does not establish which allocation site explains production memory growth.

Validation gates on the implementation branch:

- Full unit suite passed with Go 1.26.4 and `TMPDIR=/private/tmp` (the canonical
  path avoids an existing macOS temporary-directory symlink assertion).
- Focused profiling race tests, `go vet ./...`, gofumpt, Markdown lint and
  golangci-lint v2.12.2 against the maintenance base passed.
- `task e2e` failed in `tests/e2e/tests/complete/test.go:87` with a query timeout,
  followed by a nil-pointer panic at line 94. The unchanged maintenance base
  `13122889d0ba527599bf36595f95f42e856e303a` reproduced the same failure.
  The example E2E stage was consequently not reached.
- The full pre-commit runner could not install its Node environment: nodeenv
  selected an alpha release it could not parse. A local system-Node retry lacked
  npm. Markdown lint was run separately using the pinned v0.23.0 container tool;
  the full hook suite remains unverified.
