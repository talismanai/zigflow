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
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/serverlessworkflow/sdk-go/v3/model"
	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const (
	unrelatedScheduleID = "unrelated"
	sharedScheduleID    = "shared"
	deleteFailureStage  = "delete"
	listFailureStage    = "list"
	createFailureStage  = "create"
	replacedScheduleID  = "zigflow_replaced"
	pageFailureStage    = "second_page"
)

// Exercise the real SDK iterator and RPC serialisation over an in-memory gRPC
// transport. No cluster, credentials or external network is used.
type scheduleRPCServer struct {
	workflowservice.UnimplementedWorkflowServiceServer
	mu            sync.Mutex
	ids           map[string]bool
	pageSize      int
	lists         int
	deletes       []string
	creates       []*workflowservice.CreateScheduleRequest
	listErrorPage int
	deleteError   bool
	createError   bool
}

func (s *scheduleRPCServer) GetSystemInfo(
	context.Context,
	*workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (s *scheduleRPCServer) ListSchedules(
	_ context.Context,
	req *workflowservice.ListSchedulesRequest,
) (*workflowservice.ListSchedulesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lists++
	if s.listErrorPage > 0 && s.lists >= s.listErrorPage {
		return nil, serviceerror.NewInvalidArgument("list rejected")
	}
	ids := make([]string, 0, len(s.ids))
	for id := range s.ids {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	offset := 0
	if len(req.NextPageToken) > 0 {
		var err error
		offset, err = strconv.Atoi(string(req.NextPageToken))
		if err != nil {
			return nil, serviceerror.NewInvalidArgument("invalid page token")
		}
	}
	end := len(ids)
	if s.pageSize > 0 && offset+s.pageSize < end {
		end = offset + s.pageSize
	}
	result := &workflowservice.ListSchedulesResponse{}
	for _, id := range ids[offset:end] {
		result.Schedules = append(result.Schedules, &schedulepb.ScheduleListEntry{ScheduleId: id})
	}
	if end < len(ids) {
		result.NextPageToken = []byte(strconv.Itoa(end))
	}
	return result, nil
}

func (s *scheduleRPCServer) DeleteSchedule(
	_ context.Context,
	req *workflowservice.DeleteScheduleRequest,
) (*workflowservice.DeleteScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes = append(s.deletes, req.ScheduleId)
	if s.deleteError {
		return nil, serviceerror.NewInvalidArgument("delete rejected")
	}
	if !s.ids[req.ScheduleId] {
		return nil, serviceerror.NewNotFound("missing schedule")
	}
	delete(s.ids, req.ScheduleId)
	return &workflowservice.DeleteScheduleResponse{}, nil
}

func (s *scheduleRPCServer) CreateSchedule(
	_ context.Context,
	req *workflowservice.CreateScheduleRequest,
) (*workflowservice.CreateScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates = append(s.creates, req)
	if s.createError {
		return nil, serviceerror.NewInvalidArgument("create rejected")
	}
	if s.ids[req.ScheduleId] {
		return nil, serviceerror.NewInvalidArgument("duplicate schedule")
	}
	s.ids[req.ScheduleId] = true
	return &workflowservice.CreateScheduleResponse{}, nil
}

func scheduleTestClient(t *testing.T, server *scheduleRPCServer) client.Client {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	rpc := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(rpc, server)
	go func() { _ = rpc.Serve(listener) }()
	t.Cleanup(rpc.Stop)
	c, err := client.NewLazyClient(client.Options{
		HostPort: "passthrough:///schedule-test", Namespace: "local-test",
		ConnectionOptions: client.ConnectionOptions{DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		}},
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func scheduleRegistration(name string, scheduled bool) *workflowRegistration {
	w := &model.Workflow{Document: model.Document{
		Name: name, Namespace: "local-queue", Metadata: map[string]any{"scheduleWorkflowName": name},
	}}
	if scheduled {
		w.Schedule = &model.Schedule{Cron: "0 * * * *"}
	}
	return &workflowRegistration{WorkflowType: name, TaskQueue: "local-queue", SourceFile: name + ".yaml", Definition: w}
}

func TestScheduleListingRPCCount(t *testing.T) {
	for _, n := range []int{0, 1, 955, 1090} {
		t.Run(fmt.Sprintf("definitions_%d", n), func(t *testing.T) {
			s := &scheduleRPCServer{ids: map[string]bool{}}
			c := scheduleTestClient(t, s)
			registrations := make([]*workflowRegistration, n)
			for i := range registrations {
				registrations[i] = scheduleRegistration(fmt.Sprintf("workflow-%d", i), false)
			}
			require.NoError(t, runScheduleUpdates(context.Background(), c, registrations, nil))
			s.mu.Lock()
			defer s.mu.Unlock()
			expected := 1
			if n == 0 {
				expected = 0
			}
			t.Logf("definitions=%d ListSchedules=%d DeleteSchedule=%d CreateSchedule=%d", n, s.lists, len(s.deletes), len(s.creates))
			require.Equal(t, expected, s.lists)
			require.Empty(t, s.deletes)
			require.Empty(t, s.creates)
		})
	}
}

func TestScheduleUpdatesPreserveLifecycleAndPagination(t *testing.T) {
	s := &scheduleRPCServer{ids: map[string]bool{"zigflow_removed": true, replacedScheduleID: true, unrelatedScheduleID: true}, pageSize: 1}
	c := scheduleTestClient(t, s)
	require.NoError(t, runScheduleUpdates(context.Background(), c, []*workflowRegistration{
		scheduleRegistration("removed", false), scheduleRegistration("replaced", true), scheduleRegistration("new", true),
	}, nil))
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, 3, s.lists, "all pages fetched once for the entire pass")
	require.Equal(t, []string{"zigflow_removed", replacedScheduleID}, s.deletes)
	require.Equal(t, map[string]bool{unrelatedScheduleID: true, replacedScheduleID: true, "zigflow_new": true}, s.ids)
	require.Len(t, s.creates, 2)
	action := s.creates[0].Schedule.Action.GetStartWorkflow()
	require.Equal(t, "replaced", action.WorkflowType.Name)
	require.Equal(t, "local-queue", action.TaskQueue.Name)
	require.Equal(t, []string{"0 * * * *"}, s.creates[0].Schedule.Spec.CronString)
}

func TestScheduleUpdatesRefreshOnEveryPass(t *testing.T) {
	s := &scheduleRPCServer{ids: map[string]bool{}}
	c := scheduleTestClient(t, s)
	regs := []*workflowRegistration{scheduleRegistration("external", false)}
	require.NoError(t, runScheduleUpdates(context.Background(), c, regs, nil))
	s.mu.Lock()
	s.ids["zigflow_external"] = true
	s.mu.Unlock()
	require.NoError(t, runScheduleUpdates(context.Background(), c, regs, nil))
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Equal(t, 2, s.lists)
	require.Equal(t, []string{"zigflow_external"}, s.deletes)
}

func TestScheduleUpdatesTrackSharedIDWithinPass(t *testing.T) {
	for _, scheduled := range []bool{false, true} {
		t.Run(fmt.Sprintf("second_scheduled_%t", scheduled), func(t *testing.T) {
			s := &scheduleRPCServer{ids: map[string]bool{}}
			c := scheduleTestClient(t, s)
			first, second := scheduleRegistration("first", true), scheduleRegistration("second", scheduled)
			first.Definition.Document.Metadata["scheduleId"] = sharedScheduleID
			second.Definition.Document.Metadata["scheduleId"] = sharedScheduleID
			require.NoError(t, runScheduleUpdates(context.Background(), c, []*workflowRegistration{first, second}, nil))
			s.mu.Lock()
			defer s.mu.Unlock()
			require.Equal(t, 1, s.lists)
			require.Equal(t, []string{sharedScheduleID}, s.deletes)
			require.Equal(t, scheduled, s.ids[sharedScheduleID])
		})
	}
}

func TestScheduleUpdatesPropagateErrors(t *testing.T) {
	for _, stage := range []string{listFailureStage, pageFailureStage, deleteFailureStage, createFailureStage} {
		t.Run(stage, func(t *testing.T) {
			s := &scheduleRPCServer{ids: map[string]bool{"zigflow_test": true, unrelatedScheduleID: true}, pageSize: 1}
			switch stage {
			case listFailureStage:
				s.listErrorPage = 1
			case pageFailureStage:
				s.listErrorPage = 2
			case deleteFailureStage:
				s.deleteError = true
			case createFailureStage:
				s.createError = true
			}
			c := scheduleTestClient(t, s)
			require.Error(t, runScheduleUpdates(context.Background(), c, []*workflowRegistration{scheduleRegistration("test", true)}, nil))
			s.mu.Lock()
			defer s.mu.Unlock()
			if stage == listFailureStage || stage == pageFailureStage {
				require.Empty(t, s.deletes)
				require.Empty(t, s.creates)
			}
			if stage == deleteFailureStage {
				require.Empty(t, s.creates)
			}
		})
	}
}
