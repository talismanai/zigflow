//go:build e2e

/*
 * Copyright 2025 - 2026 Zigflow authors <https://github.com/zigflow/zigflow/graphs/contributors>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package run

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/fsnotify/fsnotify"
	"github.com/serverlessworkflow/sdk-go/v3/model"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/cloudevents"
	"github.com/zigflow/zigflow/pkg/telemetry"
	"github.com/zigflow/zigflow/pkg/zigflow/tasks"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// TestReloadSuspendedWorkflows exercises the actual file reload, DSL builders and
// SDK sticky cache against a local Temporal server. Do not run in parallel: the
// cache is process-wide. Every execution and task queue belongs only to this test.
func TestReloadSuspendedWorkflows(t *testing.T) {
	c := reloadTestClient(t)
	defer c.Close()
	queue := fmt.Sprintf("reload-regression-%d", time.Now().UnixNano())
	opts := &runOptions{DirectoryPath: t.TempDir(), DirectoryGlob: testDirectoryGlob, GracefulShutdownTimeout: time.Second}
	var current *workerGeneration
	var directRuns, loopRuns []client.WorkflowRun
	defer func() {
		if current != nil {
			stopWorkers(current.workers)
		}
		for _, run := range append(directRuns, loopRuns...) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "reload regression cleanup")
			cancel()
		}
	}()
	watcher, err := fsnotify.NewWatcher()
	require.NoError(t, err)
	defer func() { require.NoError(t, watcher.Close()) }()
	var used, unused []weak.Pointer[model.Workflow]
	// Observe definitions at the registration boundary without keeping them alive.
	originalWorkflow := newWorkflow
	defer func() { newWorkflow = originalWorkflow }()
	currentDefinitions := make(map[string]weak.Pointer[model.Workflow])
	newWorkflow = func(w worker.Worker, definition *model.Workflow, env map[string]any, events *cloudevents.Events,
		telemetry *telemetry.Telemetry, taskOpts *tasks.TaskOpts,
	) error {
		currentDefinitions[definition.Document.Name] = weak.Make(definition)
		return originalWorkflow(w, definition, env, events, telemetry, taskOpts)
	}
	// Keep the normal SDK cache enabled throughout. There is no cache purge,
	// including in cleanup: completion/eviction must release retired definitions.
	for generation := range 4 {
		writeReloadCatalogue(t, opts.DirectoryPath, queue, generation)
		if current == nil {
			current = startReloadTestGeneration(t, c, opts)
		} else {
			for name, ref := range currentDefinitions {
				if name == "reload-unused" {
					unused = append(unused, ref)
				} else {
					used = append(used, ref)
				}
			}
			clear(currentDefinitions)
			changed := map[string]struct{}{filepath.Join(opts.DirectoryPath, "unused.yaml"): {}}
			previous := current
			current, _ = handleDebounce(watcher, c, opts, nil, changed, current)
			require.NotSame(t, previous, current, "reload must start a replacement")
		}
		require.Eventually(t, func() bool {
			runtime.GC()
			return liveReloadDefinitions(unused) == 0
		}, 5*time.Second, 25*time.Millisecond, "retired registry must not pin unrelated definitions")
		require.Equal(t, len(used), liveReloadDefinitions(used), "suspended executions must retain their own definitions")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		direct, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: fmt.Sprintf("%s-direct-%d", queue, generation), TaskQueue: queue,
		}, "reload-hold", nil, nil)
		require.NoError(t, err)
		directRuns = append(directRuns, direct)
		loop, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: fmt.Sprintf("%s-loop-%d", queue, generation), TaskQueue: queue,
		}, "reload-loop", nil, nil)
		cancel()
		require.NoError(t, err)
		loopRuns = append(loopRuns, loop)
		waitReloadTimer(t, c, direct.GetID(), direct.GetRunID())
		waitReloadTimer(t, c, loop.GetID()+"_for_0", "")
		recordReloadHeap(t, generation, liveReloadDefinitions(used)+liveReloadDefinitions(unused), len(used)+len(unused))
		t.Logf("retired used=%d/%d unrelated=%d/%d", liveReloadDefinitions(used), len(used), liveReloadDefinitions(unused), len(unused))
	}
	for i, run := range directRuns {
		resumeReloadExecution(t, c, run, fmt.Sprintf("resume-%d", i))
	}
	for _, run := range loopRuns {
		resumeReloadLoopExecution(t, c, run)
	}
	require.Eventually(t, func() bool {
		runtime.GC()
		return liveReloadDefinitions(used)+liveReloadDefinitions(unused) == 0
	}, 10*time.Second, 25*time.Millisecond, "completed executions must release all retired definitions while replacements run")
	t.Log("8 root and 8 child executions completed with exact outputs and no workflow task failures; all 9 retired definitions collected")
}

func reloadTestClient(t *testing.T) client.Client {
	t.Helper()
	address := os.Getenv("TEMPORAL_ADDRESS")
	if address == "" {
		address = "127.0.0.1:7233"
	}
	host, _, err := net.SplitHostPort(address)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost", "::1", "0.0.0.0"}, host,
		"reload regression must use a local test server")
	c, err := client.Dial(client.Options{HostPort: address, Namespace: client.DefaultNamespace})
	require.NoError(t, err)
	return c
}

func waitReloadTimer(t *testing.T, c client.Client, id, runID string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return reloadHistoryCountsByID(t, c, id, runID)[enums.EVENT_TYPE_TIMER_STARTED] == 1
	}, 20*time.Second, 25*time.Millisecond, "signal listener must be durably suspended")
}

func resumeReloadLoopExecution(t *testing.T, c client.Client, run client.WorkflowRun) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range 2 {
		childID := fmt.Sprintf("%s_for_%d", run.GetID(), i)
		waitReloadTimer(t, c, childID, "")
		child := c.GetWorkflow(ctx, childID, "")
		payload := fmt.Sprintf("child-%d", i)
		require.NoError(t, c.SignalWorkflow(ctx, childID, "", "resume", payload))
		var result map[string]any
		require.NoError(t, child.Get(ctx, &result))
		require.Equal(t, map[string]any{"received": payload}, result["output"])
		require.Zero(t, reloadHistoryCounts(t, c, child)[enums.EVENT_TYPE_WORKFLOW_TASK_FAILED])
	}
	initialRunID := run.GetRunID()
	var result map[string]any
	require.NoError(t, run.Get(ctx, &result))
	require.Equal(t, map[string]any{"completed": true}, result)
	require.Equal(t, initialRunID, run.GetRunID())
	require.Zero(t, reloadHistoryCounts(t, c, run)[enums.EVENT_TYPE_WORKFLOW_TASK_FAILED])
}

func writeReloadCatalogue(t *testing.T, dir, queue string, generation int) {
	t.Helper()
	header := func(name string) string {
		return fmt.Sprintf("document:\n  dsl: 1.0.0\n  taskQueue: %s\n  workflowType: %s\n  version: 1.0.0\n", queue, name)
	}
	direct := header("reload-hold") + `do:
  - approval:
      metadata:
        timeout: 1h
      listen:
        to:
          one:
            with:
              id: resume
              type: signal
  - result:
      set:
        received: ${ $data.approval }
`
	loop := header("reload-loop") + `do:
  - iteration:
      for:
        in: ${ 2 }
      do:
        - approval:
            metadata:
              timeout: 1h
            listen:
              to:
                one:
                  with:
                    id: resume
                    type: signal
        - result:
            set:
              received: ${ $data.approval }
  - finish:
      set:
        completed: true
`
	// Also exercises per-task activity registration, the separate root fixed by PR1.
	unused := header("reload-unused") + "  metadata:\n    payload: " + strings.Repeat(fmt.Sprintf("g%07d", generation), 1<<20) + `
do:
  - request:
      call: http
      with:
        method: get
        endpoint: https://example.invalid/
`
	for name, definition := range map[string]string{"hold.yaml": direct, "loop.yaml": loop, "unused.yaml": unused} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(definition), 0o600))
	}
}

func startReloadTestGeneration(t *testing.T, c client.Client, opts *runOptions) *workerGeneration {
	t.Helper()
	registrations, err := prepareRegistrations(opts)
	require.NoError(t, err)
	workers, err := buildWorkersByTaskQueue(c, registrations, nil, opts)
	require.NoError(t, err)
	started, err := startAllWorkers(workers)
	require.NoError(t, err)
	return &workerGeneration{workers: started}
}

//go:noinline
func liveReloadDefinitions(refs []weak.Pointer[model.Workflow]) int {
	count := 0
	for _, ref := range refs {
		if ref.Value() != nil {
			count++
		}
	}
	return count
}

func reloadHistoryCounts(t *testing.T, c client.Client, run client.WorkflowRun) map[enums.EventType]int {
	t.Helper()
	return reloadHistoryCountsByID(t, c, run.GetID(), run.GetRunID())
}

func reloadHistoryCountsByID(t *testing.T, c client.Client, id, runID string) map[enums.EventType]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	history := c.GetWorkflowHistory(ctx, id, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	counts := make(map[enums.EventType]int)
	for history.HasNext() {
		event, err := history.Next()
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return counts
		}
		require.NoError(t, err)
		counts[event.GetEventType()]++
	}
	return counts
}

func recordReloadHeap(t *testing.T, generation, alive, total int) {
	t.Helper()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("generation=%d retired definitions=%d/%d HeapAlloc=%.2f MiB", generation, alive, total, float64(m.HeapAlloc)/(1<<20))
	if output := os.Getenv("ZIGFLOW_PROFILE_DIR"); output != "" {
		root, err := os.OpenRoot(output)
		require.NoError(t, err)
		defer func() { require.NoError(t, root.Close()) }()
		f, err := root.Create(fmt.Sprintf("reload-%02d.heap.pb.gz", generation))
		require.NoError(t, err)
		require.NoError(t, pprof.WriteHeapProfile(f))
		require.NoError(t, f.Close())
	}
}

// Rollback retains the SAME running worker even if the failed replacement had
// already started polling. It must release every failed catalogue registration.
func TestReloadRollbackResumesSuspendedWorkflow(t *testing.T) {
	for _, afterStart := range []bool{false, true} {
		t.Run(fmt.Sprintf("after-pollers=%v", afterStart), func(t *testing.T) {
			c := reloadTestClient(t)
			defer c.Close()
			queue := fmt.Sprintf("reload-rollback-%d", time.Now().UnixNano())
			opts := &runOptions{DirectoryPath: t.TempDir(), DirectoryGlob: testDirectoryGlob, GracefulShutdownTimeout: time.Second}
			writeReloadCatalogue(t, opts.DirectoryPath, queue, 0)
			current := startReloadTestGeneration(t, c, opts)
			defer func() { stopWorkers(current.workers) }()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "reload-hold", nil, nil)
			require.NoError(t, err)
			defer func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cleanupCancel()
				_ = c.TerminateWorkflow(cleanupCtx, run.GetID(), run.GetRunID(), "reload rollback cleanup")
			}()
			waitReloadTimer(t, c, run.GetID(), run.GetRunID())
			previous := current
			originalNewWorker := newWorker
			defer func() { newWorker = originalNewWorker }()
			created := 0
			newWorker = func(c client.Client, queue string, opts worker.Options) worker.Worker {
				created++
				return &failStartReloadWorker{Worker: originalNewWorker(c, queue, opts), afterStart: afterStart}
			}
			writeReloadCatalogue(t, opts.DirectoryPath, queue, 1)
			var failedDefinitions []weak.Pointer[model.Workflow]
			current, failedDefinitions, err = attemptFailedReload(t, c, opts, current)
			require.ErrorContains(t, err, "previous workers remain running")
			require.Same(t, previous, current)
			require.Equal(t, 1, created, "rollback must not rebuild or restart the old worker")
			require.Eventually(t, func() bool {
				runtime.GC()
				return liveReloadDefinitions(failedDefinitions) == 0
			}, 5*time.Second, 25*time.Millisecond, "failed replacement must release its catalogue")
			resumeReloadExecution(t, c, run, "resumed-after-rollback")
		})
	}
}

//go:noinline
func attemptFailedReload(t *testing.T, c client.Client, opts *runOptions, current *workerGeneration) (
	*workerGeneration, []weak.Pointer[model.Workflow], error,
) {
	t.Helper()
	registrations, err := prepareRegistrations(opts)
	require.NoError(t, err)
	refs := make([]weak.Pointer[model.Workflow], 0, len(registrations))
	for _, reg := range registrations {
		refs = append(refs, weak.Make(reg.Definition))
	}
	next, err := replaceWorkers(c, opts, nil, current, registrations)
	return next, refs, err
}

type failStartReloadWorker struct {
	worker.Worker
	afterStart bool
}

func (w *failStartReloadWorker) Start() error {
	if w.afterStart {
		if err := w.Worker.Start(); err != nil {
			return err
		}
	}
	return errors.New("injected replacement startup failure")
}

func resumeReloadExecution(t *testing.T, c client.Client, run client.WorkflowRun, payload string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "resume", payload))
	var result map[string]any
	require.NoError(t, run.Get(ctx, &result), "suspended execution must replay and finish in its original run")
	require.Equal(t, map[string]any{"received": payload}, result)
	counts := reloadHistoryCounts(t, c, run)
	require.Equal(t, 1, counts[enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED])
	require.Equal(t, 1, counts[enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED])
	require.Zero(t, counts[enums.EVENT_TYPE_WORKFLOW_TASK_FAILED], "replay must remain deterministic")
	require.Zero(t, counts[enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW])
}
