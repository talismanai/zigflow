# Validation record

Date: 2026-11-09. Base: v0.14.2. No shared environment was changed.

These results describe the combined local patch before it was separated into
review PRs. Image and schedule tests below therefore refer to that combined
build, not to this memory-only PR. Per-PR checks are recorded in the PR body.

## Passed

- `go test ./...` (with local socket access and `TMPDIR=/private/tmp`).
- `go vet ./...`.
- Configured `golangci-lint` with `--build-tags=profile --new-from-rev=v0.14.2 ./...`: zero issues.
- `go test -race ./cmd/run ./pkg/zigflow/tasks -run 'TestSchedule|TestForgetWorker' -count=1`.
- Central integration suite: `go test -parallel 2 -count=1 -tags=e2e ./tests/e2e/...`,
  against a dedicated local Compose project `zigflow-talisman-e2e`.
- Real SDK RPC regression and baseline reproduction.
- Local before/after worker-retention profiles with 955 synthetic definitions.
- Linux amd64 image build using pinned Go 1.26.4 and the upstream 0.14.2 runtime.
- Offline container `version` check and a synthetic workflow execution through
  the container against the local Temporal server. Result:
  `{"status":"patched-image-ok"}`.

## Incomplete repository-wide gates

The full example suite was attempted. Nine example packages failed. Failures
included external demo HTTPS certificate validation, missing generated gRPC
example code, and unavailable host Node. The isolated central suite passed;
these example failures must not be reported as a full E2E pass.

The required `pre-commit run` was attempted. Its Node environment installer
failed parsing a prerelease version (`0-alpha`). A temporary configuration
selecting stable Node 24.14.0 got past that error but then the markdown-toc
hook installation failed with npm `EALLOWGIT`. The tracked pre-commit
configuration was not weakened or changed. Formatting, vet and the configured
Go linter were run separately.

The Linux image was smoke-tested, while the recorded heap comparison ran in
Go tests on macOS. This is not a production soak test, a full catalog replay,
or validation of GitOps rollout settings. The unchanged engine source and
module files reduce upgrade scope but do not replace those rollout checks.
