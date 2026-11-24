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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// TestReloadSuspendedWorkflows exercises the actual file reload, DSL builders and
// SDK sticky cache against a local Temporal server. Do not run in parallel: the
// cache is process-wide. Every execution and task queue belongs only to this test.
func TestReloadSuspendedWorkflows(t *testing.T) {
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
	defer c.Close()
	queue := fmt.Sprintf("reload-regression-%d", time.Now().UnixNano())
	opts := &runOptions{DirectoryPath: t.TempDir(), DirectoryGlob: testDirectoryGlob, GracefulShutdownTimeout: time.Second}
	var current *workerGeneration
	var runs []client.WorkflowRun
	defer func() {
		if current != nil {
			stopWorkers(current.workers)
		}
		worker.PurgeStickyWorkflowCache()
		for _, run := range runs {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = c.TerminateWorkflow(ctx, run.GetID(), run.GetRunID(), "reload regression cleanup")
			cancel()
		}
	}()
	watcher, err := fsnotify.NewWatcher()
	require.NoError(t, err)
	defer func() { require.NoError(t, watcher.Close()) }()
	var retired []weak.Pointer[model.Workflow]
	for generation := range 4 {
		writeReloadCatalogue(t, opts.DirectoryPath, queue, generation)
		if current == nil {
			current = startReloadTestGeneration(t, c, opts)
		} else {
			for _, reg := range current.registrations {
				retired = append(retired, weak.Make(reg.Definition))
			}
			changed := map[string]struct{}{filepath.Join(opts.DirectoryPath, "unused.yaml"): {}}
			current, _, err = handleDebounce(watcher, c, opts, nil, changed, current)
			require.NoError(t, err)
		}
		// Collection, not an RSS/byte threshold: even unused definitions from
		// the retired catalogue must disappear while their executions remain open.
		assert.Eventually(t, func() bool {
			runtime.GC()
			return liveReloadDefinitions(retired) == 0
		}, 5*time.Second, 50*time.Millisecond, "retired catalogues must be collectible before signals arrive")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		run, runErr := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			ID: fmt.Sprintf("%s-%d", queue, generation), TaskQueue: queue,
		}, "reload-hold", nil, nil)
		cancel()
		require.NoError(t, runErr)
		runs = append(runs, run)
		require.Eventually(t, func() bool {
			return reloadHistoryCounts(t, c, run)[enums.EVENT_TYPE_TIMER_STARTED] == 1
		}, 10*time.Second, 50*time.Millisecond, "signal listener must be durably suspended")
		recordReloadHeap(t, generation, liveReloadDefinitions(retired), len(retired))
	}
	for i, run := range runs {
		resumeReloadExecution(t, c, run, fmt.Sprintf("resume-%d", i))
	}
	t.Logf("all %d suspended executions resumed with their original run IDs and exact signal payloads", len(runs))
}

func writeReloadCatalogue(t *testing.T, directory, queue string, generation int) {
	t.Helper()
	definition := fmt.Sprintf(`document:
  dsl: 1.0.0
  taskQueue: %s
  workflowType: reload-hold
  version: 1.0.0
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
`, queue)
	require.NoError(t, os.WriteFile(filepath.Join(directory, "hold.yaml"), []byte(definition), 0o600))
	unused := fmt.Sprintf(`document:
  dsl: 1.0.0
  taskQueue: %s
  workflowType: reload-unused
  version: 1.0.0
  metadata:
    payload: %s
do:
  - wait:
      wait:
        hours: 1
`, queue, strings.Repeat(fmt.Sprintf("g%07d", generation), 1<<20))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "unused.yaml"), []byte(unused), 0o600))
}

func startReloadTestGeneration(t *testing.T, c client.Client, opts *runOptions) *workerGeneration {
	t.Helper()
	registrations, err := prepareRegistrations(opts)
	require.NoError(t, err)
	workers, err := buildWorkersByTaskQueue(c, registrations, nil, opts)
	require.NoError(t, err)
	started, err := startAllWorkers(workers)
	require.NoError(t, err)
	return &workerGeneration{workers: started, registrations: registrations}
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	history := c.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	counts := make(map[enums.EventType]int)
	for history.HasNext() {
		event, err := history.Next()
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

// A real rebuilt worker must replay a suspended execution even after replacement
// startup fails. Stopped SDK workers cannot simply be started again.
func TestReloadRollbackResumesSuspendedWorkflow(t *testing.T) {
	address := os.Getenv("TEMPORAL_ADDRESS")
	if address == "" {
		address = "127.0.0.1:7233"
	}
	host, _, err := net.SplitHostPort(address)
	require.NoError(t, err)
	require.Contains(t, []string{"127.0.0.1", "localhost", "::1", "0.0.0.0"}, host)
	c, err := client.Dial(client.Options{HostPort: address, Namespace: client.DefaultNamespace})
	require.NoError(t, err)
	defer c.Close()
	queue := fmt.Sprintf("reload-rollback-%d", time.Now().UnixNano())
	opts := &runOptions{DirectoryPath: t.TempDir(), DirectoryGlob: testDirectoryGlob, GracefulShutdownTimeout: time.Second}
	writeReloadCatalogue(t, opts.DirectoryPath, queue, 0)
	current := startReloadTestGeneration(t, c, opts)
	defer func() {
		if current != nil {
			stopWorkers(current.workers)
		}
		worker.PurgeStickyWorkflowCache()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: queue, TaskQueue: queue}, "reload-hold", nil, nil)
	require.NoError(t, err)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = c.TerminateWorkflow(cleanupCtx, run.GetID(), run.GetRunID(), "reload rollback cleanup")
	}()
	require.Eventually(t, func() bool {
		return reloadHistoryCounts(t, c, run)[enums.EVENT_TYPE_TIMER_STARTED] == 1
	}, 10*time.Second, 50*time.Millisecond)

	previous := current
	originalNewWorker := newWorker
	defer func() { newWorker = originalNewWorker }()
	created := 0
	newWorker = func(c client.Client, queue string, opts worker.Options) worker.Worker {
		w := originalNewWorker(c, queue, opts)
		created++
		if created == 1 {
			return &failStartReloadWorker{Worker: w}
		}
		return w
	}
	writeReloadCatalogue(t, opts.DirectoryPath, queue, 1)
	registrations, err := prepareRegistrations(opts)
	require.NoError(t, err)
	current, err = replaceWorkers(c, opts, nil, current, registrations)
	require.ErrorContains(t, err, "previous catalogue restored")
	require.NotNil(t, current)
	require.Equal(t, 2, created, "rollback must construct a fresh SDK worker")
	require.Same(t, previous.registrations[0], current.registrations[0])
	resumeReloadExecution(t, c, run, "resumed-after-rollback")
}

type failStartReloadWorker struct{ worker.Worker }

func (w *failStartReloadWorker) Start() error {
	if err := w.Worker.Start(); err != nil {
		return err
	}
	// Model an SDK startup error after pollers have already started.
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
