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
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/serverlessworkflow/sdk-go/v3/model"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/cloudevents"
	"github.com/zigflow/zigflow/pkg/telemetry"
	"github.com/zigflow/zigflow/pkg/zigflow/tasks"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

type reloadTestWorker struct {
	worker.Worker
	name      string
	events    *[]string
	startErr  error
	functions []any
}

func (w *reloadTestWorker) RegisterActivity(any) {}
func (w *reloadTestWorker) RegisterWorkflowWithOptions(fn any, _ workflow.RegisterOptions) {
	w.functions = append(w.functions, fn)
}

func (w *reloadTestWorker) Start() error {
	*w.events = append(*w.events, "start:"+w.name)
	return w.startErr
}
func (w *reloadTestWorker) Stop() { *w.events = append(*w.events, "stop:"+w.name) }

func TestReplaceWorkersLifecycle(t *testing.T) {
	const startNextA = "start:next-a"
	for _, scenario := range []struct {
		name        string
		buildFails  bool
		failedStart string
		wantEvents  []string
	}{
		{name: "start every replacement before retiring old workers", wantEvents: []string{
			startNextA, "start:next-b", "start:next-c", "stop:old-a", "stop:old-b", "stop:old-c",
		}},
		{name: "build failure discards registrations without stopping unstarted SDK worker", buildFails: true},
		{
			name: "first start failure keeps old workers and discards unstarted replacements", failedStart: "next-a",
			wantEvents: []string{startNextA, "stop:next-a"},
		},
		{
			name: "partial start failure stops attempted replacements only", failedStart: "next-b",
			wantEvents: []string{startNextA, "start:next-b", "stop:next-a", "stop:next-b"},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			originalWorker, originalWorkflow := newWorker, newWorkflow
			t.Cleanup(func() { newWorker, newWorkflow = originalWorker, originalWorkflow })
			var events []string
			var created []*reloadTestWorker
			newWorker = func(_ client.Client, queue string, _ worker.Options) worker.Worker {
				w := &reloadTestWorker{name: "next-" + queue, events: &events}
				if w.name == scenario.failedStart {
					w.startErr = errors.New("injected startup failure")
				}
				created = append(created, w)
				return w
			}
			newWorkflow = func(w worker.Worker, _ *model.Workflow, _ map[string]any, _ *cloudevents.Events,
				_ *telemetry.Telemetry, _ *tasks.TaskOpts,
			) error {
				w.RegisterWorkflowWithOptions(func() string { return "definition" }, workflow.RegisterOptions{Name: "test"})
				if scenario.buildFails {
					return errors.New("injected build failure after registration")
				}
				return nil
			}
			replacement := []*workflowRegistration{{TaskQueue: "a"}, {TaskQueue: "b"}, {TaskQueue: "c"}}
			current := &workerGeneration{}
			for _, queue := range []string{"a", "b", "c"} {
				current.workers = append(current.workers, &reloadTestWorker{name: "old-" + queue, events: &events})
			}
			next, err := replaceWorkers(nil, &runOptions{}, nil, current, replacement)
			require.Equal(t, scenario.wantEvents, events)
			if scenario.buildFails || scenario.failedStart != "" {
				require.Error(t, err)
				require.Same(t, current, next, "rollback must preserve the original running workers")
				for _, w := range created {
					for _, fn := range w.functions {
						require.PanicsWithValue(t, "Zigflow workflow entered after its worker retired", func() { fn.(func() string)() },
							"even unstarted replacements must release their registrations")
					}
				}
			} else {
				require.NoError(t, err)
				require.NotSame(t, current, next)
				require.Len(t, next.workers, 3)
				stopWorkers(next.workers)
			}
		})
	}
}

// Cancellation can arrive while replacement Start is blocked. The watch loop
// finishes that handoff (or rolls it back), then stops the current generation.
func TestWatchShutdownDuringReload(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "successful replacement"
		if fail {
			name = "failed replacement"
		}
		t.Run(name, func(t *testing.T) {
			originalWorker, originalWorkflow := newWorker, newWorkflow
			t.Cleanup(func() { newWorker, newWorkflow = originalWorker, originalWorkflow })
			var events []string
			oldSDK := &catalogueTestWorker{
				start: func() error { return nil },
				stop:  func() { events = append(events, "stop:old") },
			}
			old := &catalogueWorker{Worker: oldSDK}
			require.NoError(t, old.Start())
			startEntered, finishStart := make(chan struct{}), make(chan struct{})
			var finish sync.Once
			t.Cleanup(func() { finish.Do(func() { close(finishStart) }) })
			replacement := &catalogueTestWorker{
				start: func() error {
					events = append(events, "start:next")
					close(startEntered)
					<-finishStart
					if fail {
						return errors.New("startup failed during shutdown")
					}
					return nil
				},
				stop: func() { events = append(events, "stop:next") },
			}
			newWorker = func(client.Client, string, worker.Options) worker.Worker { return replacement }
			newWorkflow = func(w worker.Worker, _ *model.Workflow, _ map[string]any, _ *cloudevents.Events,
				_ *telemetry.Telemetry, _ *tasks.TaskOpts,
			) error {
				w.RegisterWorkflowWithOptions(func() {}, workflow.RegisterOptions{Name: testCatalogueWorkflowName})
				return nil
			}
			path := writeTempWorkflow(t, t.TempDir(), "test", testCatalogueWorkflowName)
			root, err := os.OpenRoot(filepath.Dir(path))
			require.NoError(t, err)
			defer func() { require.NoError(t, root.Close()) }()
			data, err := root.ReadFile(filepath.Base(path))
			require.NoError(t, err)
			opts := &runOptions{Files: []string{path}, DirectoryGlob: testDirectoryGlob, WatchDebounce: time.Millisecond}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			watchFinished := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				finish.Do(func() { close(finishStart) })
				select {
				case <-watchFinished:
				case <-time.After(10 * time.Second):
					t.Error("watch goroutine did not exit")
				}
			})
			go func() {
				defer close(watchFinished)
				done <- runWatchMode(ctx, opts.Files, nil, opts, nil, &workerGeneration{workers: []worker.Worker{old}})
			}()
			require.Eventually(t, func() bool {
				select {
				case <-startEntered:
					return true
				default:
				}
				// Retry the write until the real fsnotify watcher has subscribed.
				require.NoError(t, root.WriteFile(filepath.Base(path), data, 0o600))
				return false
			}, 10*time.Second, 20*time.Millisecond)
			cancel()
			finish.Do(func() { close(finishStart) })
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("watch shutdown did not finish")
			}
			expected := []string{"start:next", "stop:old", "stop:next"}
			if fail {
				expected = []string{"start:next", "stop:next", "stop:old"}
			}
			require.Equal(t, expected, events)
			require.PanicsWithValue(t, "Zigflow workflow entered after its worker retired", func() { replacement.functions[0].(func())() })
		})
	}
}
