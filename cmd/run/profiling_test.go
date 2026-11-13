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
	"compress/gzip"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/telemetry"
)

func TestProfilingDisabled(t *testing.T) {
	server, err := startProfiling(t.Context(), "")
	require.NoError(t, err)
	require.Nil(t, server)
	stopProfiling(server)
}

func TestProfilingRejectsNonLoopback(t *testing.T) {
	for _, address := range []string{"localhost:6060", ":6060", "0.0.0.0:6060", "[::]:6060", "192.0.2.1:6060", "invalid"} {
		t.Run(address, func(t *testing.T) {
			server, err := startProfiling(t.Context(), address)
			require.Error(t, err)
			require.Nil(t, server)
		})
	}
}

func TestProfilingBindFailure(t *testing.T) {
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	server, err := startProfiling(t.Context(), listener.Addr().String())
	require.ErrorContains(t, err, "listen for pprof")
	require.Nil(t, server)
}

func TestProfilingProfilesAndShutdown(t *testing.T) {
	server, err := startProfiling(t.Context(), "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { stopProfiling(server) })
	client := &http.Client{Timeout: 10 * time.Second}
	for _, name := range []string{"heap?gc=1", "allocs", "goroutine"} {
		t.Run(name, func(t *testing.T) {
			response := profilingRequest(t, client, server, http.MethodGet, name)
			defer response.Body.Close()
			require.Equal(t, http.StatusOK, response.StatusCode)
			reader, err := gzip.NewReader(response.Body)
			require.NoError(t, err)
			defer reader.Close()
			data, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.NotEmpty(t, data, "profile must contain gzipped protobuf data")
		})
	}
	for _, name := range []string{"", "profile", "trace", "cmdline", "mutex", "block"} {
		response := profilingRequest(t, client, server, http.MethodGet, name)
		require.Equal(t, http.StatusNotFound, response.StatusCode)
		require.NoError(t, response.Body.Close())
	}
	response := profilingRequest(t, client, server, http.MethodPost, "heap")
	require.Equal(t, http.StatusMethodNotAllowed, response.StatusCode)
	require.NoError(t, response.Body.Close())
	stopProfiling(server)
	connection, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", server.Addr)
	if connection != nil {
		require.NoError(t, connection.Close())
	}
	require.Error(t, err, "listener must be closed on shutdown")
}

func profilingRequest(t *testing.T, client *http.Client, server *http.Server, method, name string) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), method, "http://"+server.Addr+"/debug/pprof/"+name, http.NoBody)
	require.NoError(t, err)
	response, err := client.Do(request)
	require.NoError(t, err)
	return response
}

func TestProfilingFlagAndStartupValidation(t *testing.T) {
	cmd := New(func() *telemetry.Telemetry { return nil })
	flag := cmd.Flags().Lookup("pprof-listen-address")
	require.NotNil(t, flag)
	require.Empty(t, flag.DefValue)
	require.NoError(t, cmd.Flags().Set("pprof-listen-address", "0.0.0.0:6060"))
	// No workflows or Temporal connection: profiling validation must happen first.
	err := cmd.RunE(cmd, nil)
	require.ErrorContains(t, err, "literal loopback IP")
}

func TestProfilingClosesAfterRegistrationFailure(t *testing.T) {
	listener, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	err = runRunCmd(context.Background(), &runOptions{
		PprofListenAddress: address,
		Files:              []string{t.TempDir() + "/missing.yaml"},
	})
	require.Error(t, err)
	require.False(t, strings.Contains(err.Error(), "listen for pprof"), "profiling must have started")
	listener, err = new(net.ListenConfig).Listen(t.Context(), "tcp", address)
	require.NoError(t, err, "registration failure must release the profiling listener")
	require.NoError(t, listener.Close())
}
