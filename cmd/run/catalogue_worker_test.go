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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const testCatalogueWorkflowName = "hold"

type catalogueTestWorker struct {
	worker.Worker
	functions []any
	options   []workflow.RegisterOptions
	start     func() error
	stop      func()
}

func (w *catalogueTestWorker) RegisterActivity(any) {}

func (w *catalogueTestWorker) RegisterWorkflowWithOptions(fn any, opts workflow.RegisterOptions) {
	w.functions = append(w.functions, fn)
	w.options = append(w.options, opts)
}
func (w *catalogueTestWorker) Start() error { return w.start() }
func (w *catalogueTestWorker) Stop()        { w.stop() }

func TestCatalogueWorkerForwardsSignatureResultsAndPanics(t *testing.T) {
	type childResult struct {
		Output  any
		Context map[string]any
	}
	sdk := &catalogueTestWorker{}
	w := &catalogueWorker{Worker: sdk}
	expectedErr := errors.New("workflow error")
	opts := workflow.RegisterOptions{
		Name: "child", DisableAlreadyRegisteredCheck: true,
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	}
	w.RegisterWorkflowWithOptions(func(_ workflow.Context, input any, state *int) (childResult, error) {
		require.Nil(t, state)
		return childResult{Output: input}, expectedErr
	}, opts)
	w.RegisterWorkflowWithOptions(func() { panic("workflow panic") }, workflow.RegisterOptions{Name: "panic"})
	require.Equal(t, opts, sdk.options[0])
	fn := sdk.functions[0].(func(workflow.Context, any, *int) (childResult, error))
	result, err := fn(nil, "payload", nil)
	require.Equal(t, childResult{Output: "payload"}, result)
	require.ErrorIs(t, err, expectedErr)
	require.PanicsWithValue(t, "workflow panic", func() { sdk.functions[1].(func())() })
	w.Stop() // An unstarted worker releases registrations without SDK Stop.
	require.PanicsWithValue(t, "Zigflow workflow entered after its worker retired", func() { _, _ = fn(nil, nil, nil) })
	require.ErrorContains(t, w.Start(), "retired")
	require.Panics(t, func() { w.RegisterWorkflowWithOptions(func() {}, opts) })
}

func TestCatalogueWorkerRejectsUnsupportedRegistration(t *testing.T) {
	w := &catalogueWorker{Worker: &catalogueTestWorker{start: func() error { return nil }}}
	require.Panics(t, func() { w.RegisterWorkflow(func() {}) })
	for _, fn := range []any{nil, 42, (func())(nil)} {
		require.Panics(t, func() { w.RegisterWorkflowWithOptions(fn, workflow.RegisterOptions{Name: "invalid"}) })
	}
	require.NoError(t, w.Start())
	require.Panics(t, func() { w.RegisterWorkflowWithOptions(func() {}, workflow.RegisterOptions{Name: "late"}) })
}

type retainedDefinition struct{ payload [1024]byte }

//go:noinline
func registerBlockingDefinition(w *catalogueWorker, entered chan<- struct{}, resume <-chan struct{}) weak.Pointer[retainedDefinition] {
	definition := &retainedDefinition{}
	definition.payload[0] = 42
	w.RegisterWorkflowWithOptions(func() byte {
		close(entered)
		<-resume
		return definition.payload[0]
	}, workflow.RegisterOptions{Name: testCatalogueWorkflowName})
	return weak.Make(definition)
}

//go:noinline
func definitionAlive(ref weak.Pointer[retainedDefinition]) bool { return ref.Value() != nil }

func TestCatalogueWorkerSuspendedCallSurvivesConcurrentStop(t *testing.T) {
	var stops atomic.Int32
	sdk := &catalogueTestWorker{start: func() error { return nil }, stop: func() { stops.Add(1) }}
	w := &catalogueWorker{Worker: sdk}
	entered, resume := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(resume) }) })
	ref := registerBlockingDefinition(w, entered, resume)
	require.NoError(t, w.Start())
	done := make(chan byte, 1)
	go func() { done <- sdk.functions[0].(func() byte)() }()
	require.Eventually(t, func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	},
		time.Second, time.Millisecond)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(w.Stop)
	}
	wg.Wait()
	require.EqualValues(t, 1, stops.Load(), "concurrent Stop calls must not race inside the SDK")
	runtime.GC()
	require.True(t, definitionAlive(ref), "the suspended invocation must still own its function")
	release.Do(func() { close(resume) })
	select {
	case result := <-done:
		require.EqualValues(t, 42, result)
	case <-time.After(time.Second):
		t.Fatal("suspended invocation did not finish")
	}
	require.Eventually(t, func() bool { runtime.GC(); return !definitionAlive(ref) },
		5*time.Second, 10*time.Millisecond, "registry must not retain the function after the invocation finishes")
	runtime.KeepAlive(sdk) // The registry itself is deliberately still reachable.
}

func TestCatalogueWorkerSerialisesStartAndShutdown(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "successful start", true: "failed start after polling"}[fail], func(t *testing.T) {
			entered, finishStart := make(chan struct{}), make(chan struct{})
			var finish sync.Once
			t.Cleanup(func() { finish.Do(func() { close(finishStart) }) })
			var startReturned, stopped atomic.Bool
			sdk := &catalogueTestWorker{}
			sdk.start = func() error {
				close(entered)
				<-finishStart
				startReturned.Store(true)
				if fail {
					return errors.New("late startup failure")
				}
				return nil
			}
			sdk.stop = func() {
				if !startReturned.Load() {
					t.Error("SDK Stop raced with Start")
				}
				// Targets stay callable until the underlying SDK's Stop has returned.
				if sdk.functions[0].(func() int)() != 7 {
					t.Error("registration changed during SDK shutdown")
				}
				stopped.Store(true)
			}
			w := &catalogueWorker{Worker: sdk}
			w.RegisterWorkflowWithOptions(func() int { return 7 }, workflow.RegisterOptions{Name: testCatalogueWorkflowName})
			started := make(chan error, 1)
			go func() { started <- w.Start() }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("Start did not enter")
			}
			stopRequested, stopDone := make(chan struct{}), make(chan struct{})
			go func() { close(stopRequested); w.Stop(); close(stopDone) }()
			<-stopRequested
			require.False(t, stopped.Load())
			finish.Do(func() { close(finishStart) })
			select {
			case err := <-started:
				if fail {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(time.Second):
				t.Fatal("Start did not finish")
			}
			select {
			case <-stopDone:
			case <-time.After(time.Second):
				t.Fatal("Stop did not finish")
			}
			require.True(t, stopped.Load())
			require.PanicsWithValue(t, "Zigflow workflow entered after its worker retired", func() { sdk.functions[0].(func() int)() })
			require.Error(t, w.Start(), "shutdown must prevent restart")
		})
	}
}

func TestCatalogueWorkerStopBeforeStart(t *testing.T) {
	sdk := &catalogueTestWorker{
		start: func() error { t.Error("retired worker started"); return nil },
		stop:  func() { t.Error("unstarted SDK worker stopped") },
	}
	w := &catalogueWorker{Worker: sdk}
	w.Stop()
	w.Stop()
	require.Error(t, w.Start())
}
