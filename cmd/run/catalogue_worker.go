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
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// catalogueWorker is used only by the CLI's named DSL registrations. The SDK's
// sticky cache may retain a stopped worker's registry. Register a small forwarding
// function there instead of letting that registry own every compiled definition.
// This is not a general-purpose worker adapter: callers use Start/Stop and invoke
// workflows by their explicit names, never by Go function aliases.
type catalogueWorker struct {
	worker.Worker
	mu             sync.Mutex
	startAttempted bool
	stopped        bool
	slots          []*workflowSlot
}

type compiledWorkflow struct{ value reflect.Value }

type workflowSlot struct {
	target atomic.Pointer[compiledWorkflow]
}

func (s *workflowSlot) call(args []reflect.Value) []reflect.Value {
	target := s.target.Load()
	if target == nil {
		// A task entering after retirement must retry on a live worker, not report
		// a successful result or fail the durable execution with an ordinary error.
		// CLI workers use the SDK's default BlockWorkflow panic policy.
		panic("Zigflow workflow entered after its worker retired")
	}
	result := target.value.Call(args)
	// A suspended invocation owns ONLY its compiled workflow, even after Stop
	// clears the registry slot. Do not hold a lifecycle lock across this call.
	runtime.KeepAlive(target)
	return result
}

func (w *catalogueWorker) RegisterWorkflow(fn any) {
	w.RegisterWorkflowWithOptions(fn, workflow.RegisterOptions{})
}

func (w *catalogueWorker) RegisterWorkflowWithOptions(fn any, opts workflow.RegisterOptions) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.startAttempted || w.stopped {
		panic("cannot register a workflow after worker startup or retirement")
	}
	if opts.Name == "" {
		panic("Zigflow catalogue workflows require an explicit registration name")
	}
	value := reflect.ValueOf(fn)
	if value.Kind() != reflect.Func || value.IsNil() {
		panic("Zigflow catalogue workflow must be a non-nil function")
	}
	slot := &workflowSlot{}
	slot.target.Store(&compiledWorkflow{value: value})
	// MakeFunc preserves both DSL signatures, including the private for-child
	// result type. The forwarder captures only its slot, not w or the catalogue.
	forwarding := reflect.MakeFunc(value.Type(), slot.call).Interface()
	w.Worker.RegisterWorkflowWithOptions(forwarding, opts)
	w.slots = append(w.slots, slot)
}

func (w *catalogueWorker) Start() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return errors.New("cannot start a retired Zigflow worker")
	}
	// Set before Start: an error can occur after SDK pollers have started.
	w.startAttempted = true
	return w.Worker.Start()
}

func (w *catalogueWorker) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	w.stopped = true
	// Serialise Start/Stop, and keep every target until SDK shutdown finishes.
	// Never call SDK Stop for a worker discarded during catalogue construction.
	if w.startAttempted {
		w.Worker.Stop()
	}
	for _, slot := range w.slots {
		slot.target.Store(nil)
	}
	w.slots = nil
}
