#!/usr/bin/env bash
# Copyright 2025 - 2026 Zigflow authors <https://github.com/zigflow/zigflow/graphs/contributors>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail
root=$(git rev-parse --show-toplevel)
output=${1:?Usage: reproduce.sh OUTPUT_DIRECTORY}
mkdir -p "$output"
output=$(cd "$output" && pwd)
baseline=$(mktemp -d)
# Preserve the baseline checkout alongside the logs for inspection.
printf '%s\n' "$baseline" > "$output/baseline-directory.txt"
git -C "$root" archive v0.14.2 | tar -x -C "$baseline"
cp "$root/cmd/run/schedule_updates_test.go" "$baseline/cmd/run/"
set +e
(cd "$baseline" && go test ./cmd/run -run '^TestScheduleListingRPCCount$' -count=1 -v) > "$output/before.log" 2>&1
before_status=$?
set -e
if [ "$before_status" -eq 0 ]; then
  echo 'Expected the baseline to fail the one-listing assertion' >&2
  exit 1
fi
grep 'definitions=1090 ListSchedules=1090' "$output/before.log"
(cd "$root" && go test ./cmd/run -run '^TestSchedule' -count=1 -v) > "$output/after.log" 2>&1
grep 'definitions=1090 ListSchedules=1 ' "$output/after.log"
echo "Full RPC counts and regression results: $output"
