# Talisman Zigflow v0.14.2 build

This PR adds only the custom build recipe and instructions. The two runtime
fixes are reviewed in separate PRs targeting `codex/v0.14.2-maintenance`:

- [Worker-memory fix](https://github.com/talismanai/zigflow/pull/1): release
  stopped workers from the activity-registration map.
- [Schedule-listing fix](https://github.com/talismanai/zigflow/pull/2): list
  Temporal schedules once per startup reconciliation pass.

The maintenance branch starts at upstream v0.14.2
(`ee1f3865feb631fed905863aa51807de8c949710`). It does not include later engine
releases, DSL naming changes, or the Procedures duplicate-subworkflow
mitigation. `go.mod` and `go.sum` remain at v0.14.2. The fork default branch
`main` follows newer upstream code and is not the source for this build.

Review and merge the two fix PRs and this build PR into the maintenance branch
before building the combined candidate. Merging this recipe alone does not
apply either fix. Every PR can be reviewed independently; no direct main update
or history rewrite is required. Moving to newer upstream code is a separate
upgrade that needs its own compatibility review.

## Build locally

After the three reviewed PRs are merged, from a clean checkout of
`codex/v0.14.2-maintenance`:

```sh
docker build -f Dockerfile.talisman --platform linux/amd64 \
  --build-arg VERSION=0.14.2-talisman.1 \
  --build-arg GIT_COMMIT="$(git rev-parse HEAD)" \
  -t talisman-zigflow:0.14.2-talisman.1 .
docker run --rm --network none --platform linux/amd64 \
  talisman-zigflow:0.14.2-talisman.1 version
```

The Docker build compiles the source in the current checkout. The `VERSION`
argument labels the resulting binary; it does not select a Git tag or fetch
source. The `GIT_COMMIT` argument records the exact checked-out revision.

The Dockerfile pins the Go 1.26.4 builder and the upstream 0.14.2 runtime by
image digest. It replaces only `/app/app` and build metadata, preserving the
runtime's Node/Python tools, user, entrypoint and default environment. The
upstream `WORKFLOW_FILE=/app/workflow.yaml` default still applies; set `--file`
or the corresponding existing deployment configuration explicitly.

The binary retains its symbol table and debug information. No profiling HTTP
listener is enabled by this build. Symbol availability alone does not guarantee
compatibility with every external profiler.

The image is built locally. These commands do not publish it or deploy it.
Before a rollout, publish an immutable image to the approved organisation
registry and use its digest in GitOps. Keep the prior image digest for rollback.

## Evidence and validation

- `investigations/schedule-listing`: actual SDK RPC counts, regression tests
  and an automated baseline-versus-patch reproduction.
- `investigations/runtime-fixes-20261109`: synthetic worker replacement test,
  post-GC heap samples and raw heap profiles for both revisions.

Those evidence directories arrive with their respective fix PRs. The
previously tested combined local candidate was commit
`3724f9c183d22c241b211d3759aa05043a517159`, before the changes were split for
review. A newly merged candidate has a new commit ID and should be rebuilt
and tested at that revision.

Validation results are recorded with the experiment evidence. The patch has
not been deployed to a shared environment. A production rollout still needs
representative workload/replay validation and resource/probe settings suitable
for the full catalog. Fixing the retained-worker map does not make watch mode
an upstream-recommended production deployment strategy.
