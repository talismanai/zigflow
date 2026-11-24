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
	"errors"
	"fmt"
	"testing"

	"github.com/serverlessworkflow/sdk-go/v3/model"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/cloudevents"
	"github.com/zigflow/zigflow/pkg/telemetry"
	"github.com/zigflow/zigflow/pkg/zigflow/tasks"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type reloadTestWorker struct {
	worker.Worker
	name     string
	events   *[]string
	startErr error
}

func (w *reloadTestWorker) RegisterActivity(any) {}

func (w *reloadTestWorker) Start() error {
	*w.events = append(*w.events, "start:"+w.name)
	return w.startErr
}

func (w *reloadTestWorker) Stop() {
	*w.events = append(*w.events, "stop:"+w.name)
}

func TestReplaceWorkersLifecycle(t *testing.T) {
	const (
		purgeEvent     = "purge"
		stopOldA       = "stop:old-a"
		stopOldB       = "stop:old-b"
		startNextA     = "start:next-a"
		startNextB     = "start:next-b"
		stopNextA      = "stop:next-a"
		startRestoredA = "start:restored-a"
		startRestoredB = "start:restored-b"
	)
	for _, scenario := range []struct {
		name         string
		buildFails   bool
		failedStarts map[string]bool
		wantEvents   []string
		wantError    bool
		wantFatal    bool
	}{
		{
			name:       "stop every queue before purge and replacement start",
			wantEvents: []string{stopOldA, stopOldB, purgeEvent, startNextA, startNextB},
		},
		{name: "build failure preserves running workers", buildFails: true, wantError: true},
		{
			name: "partial startup failure restores previous catalogue", failedStarts: map[string]bool{"next-b": true},
			wantError: true, wantEvents: []string{
				stopOldA, stopOldB, purgeEvent, startNextA, startNextB,
				"stop:next-b", stopNextA, purgeEvent, startRestoredA, startRestoredB,
			},
		},
		{
			name: "rollback failure is fatal", failedStarts: map[string]bool{"next-b": true, "restored-b": true},
			wantError: true, wantFatal: true, wantEvents: []string{
				stopOldA, stopOldB, purgeEvent, startNextA,
				startNextB, "stop:next-b", stopNextA, purgeEvent, startRestoredA, startRestoredB,
				"stop:restored-b", "stop:restored-a", purgeEvent,
			},
		},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			originalWorker, originalWorkflow, originalPurge := newWorker, newWorkflow, purgeStickyWorkflowCache
			t.Cleanup(func() {
				newWorker, newWorkflow, purgeStickyWorkflowCache = originalWorker, originalWorkflow, originalPurge
			})
			var events []string
			created := 0
			newWorker = func(_ client.Client, queue string, _ worker.Options) worker.Worker {
				prefix := "next-"
				if created >= 2 {
					prefix = "restored-"
				}
				created++
				name := prefix + queue
				w := &reloadTestWorker{name: name, events: &events}
				if scenario.failedStarts[name] {
					w.startErr = errors.New("injected startup failure")
				}
				return w
			}
			newWorkflow = func(worker.Worker, *model.Workflow, map[string]any, *cloudevents.Events,
				*telemetry.Telemetry, *tasks.TaskOpts,
			) error {
				if scenario.buildFails {
					return errors.New("injected build failure")
				}
				return nil
			}
			purgeStickyWorkflowCache = func() { events = append(events, purgeEvent) }
			old := []*workflowRegistration{{TaskQueue: "a"}, {TaskQueue: "b"}}
			replacement := []*workflowRegistration{{TaskQueue: "a"}, {TaskQueue: "b"}}
			current := &workerGeneration{registrations: old}
			for _, queue := range []string{"a", "b"} {
				current.workers = append(current.workers, &reloadTestWorker{name: "old-" + queue, events: &events})
			}
			next, err := replaceWorkers(nil, &runOptions{}, nil, current, replacement)
			require.Equal(t, scenario.wantEvents, events)
			if scenario.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if scenario.wantFatal {
				require.Nil(t, next)
				require.ErrorContains(t, err, "restoring previous workers failed")
			} else {
				require.NotNil(t, next)
				require.Len(t, next.workers, 2)
				expected := replacement
				if scenario.wantError {
					expected = old
				}
				require.Same(t, expected[0], next.registrations[0], fmt.Sprint(events))
				if scenario.buildFails {
					require.Same(t, current, next)
				}
			}
		})
	}
}
