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
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	"github.com/rs/zerolog/log"
)

// startProfiling binds before starting workers, so invalid configuration fails startup.
// Linking the heap handler also keeps Go's allocation sampling reachable at startup.
func startProfiling(ctx context.Context, address string) (*http.Server, error) {
	if address == "" {
		return nil, nil
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid pprof listen address: %w", err)
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		return nil, fmt.Errorf("pprof listen address must use a literal loopback IP: %q", address)
	}
	if runtime.MemProfileRate <= 0 {
		return nil, errors.New("pprof requires memory sampling; remove GODEBUG=memprofilerate=0")
	}

	// Never use DefaultServeMux: net/http/pprof registers additional routes there.
	mux := http.NewServeMux()
	for _, name := range []string{"heap", "allocs", "goroutine"} {
		mux.Handle("GET /debug/pprof/"+name, pprof.Handler(name))
	}
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen for pprof: %w", err)
	}
	server := &http.Server{
		Addr:              listener.Addr().String(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("Profiling server failed")
		}
	}()
	log.Info().Str("address", server.Addr).Int("memorySampleRate", runtime.MemProfileRate).
		Msg("Profiling server started")
	return server, nil
}

func stopProfiling(server *http.Server) {
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Warn().Err(err).Msg("Profiling server shutdown timed out")
		if err := server.Close(); err != nil {
			log.Warn().Err(err).Msg("Closing profiling server failed")
		}
	}
}
