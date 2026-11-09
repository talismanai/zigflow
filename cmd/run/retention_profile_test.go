//go:build profile

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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

// TestWorkerRetentionProfile runs only when explicitly selected with -tags=profile.
// It uses synthetic definitions and a loopback-only Temporal server. It records
// post-GC heap samples and profiles, rather than asserting a platform-dependent
// byte threshold. Compare the same test on v0.14.2 and the patched branch.
func TestWorkerRetentionProfile(t *testing.T) {
	output := os.Getenv("ZIGFLOW_PROFILE_DIR")
	address := os.Getenv("ZIGFLOW_PROFILE_ADDRESS")
	if output == "" || address == "" {
		t.Skip("set ZIGFLOW_PROFILE_DIR and loopback ZIGFLOW_PROFILE_ADDRESS")
	}
	host, _, err := net.SplitHostPort(address)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", host, "profiling must use the isolated local server")
	outputRoot, err := os.OpenRoot(output)
	require.NoError(t, err, "create the profile output directory before running the experiment")
	defer func() { require.NoError(t, outputRoot.Close()) }()
	directory := t.TempDir()
	for i := range 955 {
		definition := fmt.Sprintf(`document:
  dsl: 1.0.0
  taskQueue: retention-profile
  workflowType: retention-%04d
  version: 1.0.0
do:
  - request:
      call: http
      with:
        method: get
        endpoint: https://example.invalid/
`, i)
		require.NoError(t, os.WriteFile(filepath.Join(directory, fmt.Sprintf("%04d.yaml", i)), []byte(definition), 0o600))
	}
	c, err := client.Dial(client.Options{HostPort: address, Namespace: "default"})
	require.NoError(t, err)
	defer c.Close()
	opts := &runOptions{DirectoryPath: directory, DirectoryGlob: "*.yaml"}
	current, err := launchWorkers(c, opts, nil)
	require.NoError(t, err)
	defer func() { stopWorkerList(current) }()
	samples, err := outputRoot.Create("heap.csv")
	require.NoError(t, err)
	defer func() { require.NoError(t, samples.Close()) }()
	_, err = fmt.Fprintln(samples, "reload,heap_alloc_bytes,heap_objects,num_gc")
	require.NoError(t, err)
	snapshot := func(cycle int) {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		_, err := fmt.Fprintf(samples, "%d,%d,%d,%d\n", cycle, m.HeapAlloc, m.HeapObjects, m.NumGC)
		require.NoError(t, err)
		if cycle == 0 || cycle == 10 {
			f, err := outputRoot.Create(fmt.Sprintf("reload-%02d.heap.pb.gz", cycle))
			require.NoError(t, err)
			require.NoError(t, pprof.WriteHeapProfile(f))
			require.NoError(t, f.Close())
		}
	}
	snapshot(0)
	for cycle := 1; cycle <= 10; cycle++ {
		next, err := launchWorkers(c, opts, nil)
		require.NoError(t, err)
		stopWorkerList(current)
		current = next
		snapshot(cycle)
	}
}
