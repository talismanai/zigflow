#!/usr/bin/env python3
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

"""Exercise a locally built image on an isolated Docker network. No cluster access."""

import argparse
import gzip
import json
from pathlib import Path
import subprocess
import tempfile
import time
import uuid


def command(*args, **kwargs):
    return subprocess.run(args, check=True, stdout=subprocess.PIPE, **kwargs).stdout


def wait_for(check, description):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(1)
    raise RuntimeError("Timed out: " + description)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="talisman-zigflow:profiling-local")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    project = "zigflow-profile-" + uuid.uuid4().hex[:10]
    with tempfile.TemporaryDirectory(prefix="zigflow-profile-") as directory:
        root = Path(directory)
        workflows = root / "workflows"
        workflows.mkdir()
        definition = workflows / "smoke.yaml"
        definition.write_text("""document:
  dsl: 1.0.0
  taskQueue: profiling-smoke
  workflowType: profiling-smoke
  version: 1.0.0
do:
  - result:
      set:
        status: profiling-ok
""")
        compose = {
            "services": {
                "temporal": {
                    "image": "temporalio/temporal",
                    "command": "server start-dev --ip 0.0.0.0",
                    "healthcheck": {
                        "test": ["CMD", "temporal", "operator", "cluster", "health"],
                        "interval": "2s", "timeout": "5s", "retries": 30,
                    },
                },
                "worker": {
                    "image": args.image, "platform": "linux/amd64",
                    "command": ["run", "--dir=/workflows", "--watch=true",
                                "--disable-telemetry=true", "--temporal-address=temporal:7233",
                                "--temporal-namespace=default", "--pprof-listen-address=127.0.0.1:6060"],
                    "environment": {"WORKFLOW_FILE": ""},
                    "volumes": [str(workflows) + ":/workflows:ro"],
                    "depends_on": {"temporal": {"condition": "service_healthy"}},
                },
            },
            "networks": {"default": {"internal": True}},
        }
        config = root / "compose.json"
        config.write_text(json.dumps(compose))
        base = ["docker", "compose", "-p", project, "-f", str(config)]

        def compose_command(*parts):
            return command(*base, *parts)

        def logs():
            return compose_command("logs", "--no-color", "worker").decode()

        def request(port, path, expected=200):
            code = """import sys, urllib.request, urllib.error
try:
    response = urllib.request.urlopen(sys.argv[1], timeout=30)
except urllib.error.HTTPError as error:
    response = error
assert response.status == int(sys.argv[2]), response.status
sys.stdout.buffer.write(response.read())
"""
            return compose_command("exec", "-T", "worker", "python3", "-c", code,
                                   f"http://127.0.0.1:{port}{path}", str(expected))

        def capture(prefix):
            for name in ("heap", "allocs", "goroutine"):
                suffix = "?gc=1" if name == "heap" else ""
                data = request(6060, "/debug/pprof/" + name + suffix)
                assert len(gzip.decompress(data)) > 0
                (args.output / f"{prefix}.{name}.pb.gz").write_bytes(data)

        try:
            compose_command("up", "-d", "--wait", "--wait-timeout", "90", "--pull", "never")
            wait_for(lambda: "Watch mode enabled" in logs(), "worker startup")
            assert '"memorySampleRate":524288' in logs(), logs()
            for port in (3000, 9090):
                request(port, "/debug/pprof/heap", 404)
            for path in ("profile", "trace", "cmdline", ""):
                request(6060, "/debug/pprof/" + path, 404)
            capture("before")
            result = json.loads(compose_command(
                "exec", "-T", "temporal", "temporal", "workflow", "execute",
                "--namespace", "default", "--workflow-id", "profiling-smoke",
                "--type", "profiling-smoke", "--task-queue", "profiling-smoke",
                "--execution-timeout", "60s", "-o", "json"))
            assert result["status"] == "COMPLETED", result
            assert result["result"] == {"status": "profiling-ok"}, result
            (args.output / "workflow-result.json").write_text(json.dumps(result, indent=2))
            definition.write_text(definition.read_text() + "\n# trigger one watch reload\n")
            wait_for(lambda: "Watch: workers reloaded successfully" in logs(), "watch reload")
            capture("after")
            (args.output / "worker.log").write_text(logs())
            # The remote network peer must not reach the loopback-only listener.
            compose_command("run", "--rm", "--no-deps", "--entrypoint", "python3", "worker", "-c",
                            "import socket; s=socket.socket(); s.settimeout(3); "
                            "assert s.connect_ex(('worker',6060)) != 0; s.close()")
            # Parse the profiles with the same Go version used by Dockerfile.talisman.
            for name in ("heap", "allocs", "goroutine"):
                data = command("docker", "run", "--rm", "--network", "none",
                               "-v", str(args.output.resolve()) + ":/profiles:ro",
                               "golang:1.26.4", "go", "tool", "pprof", "-top",
                               "/profiles/after." + name + ".pb.gz")
                (args.output / (name + "-top.txt")).write_bytes(data)
            print("PASS: workflow, reload, three readable profiles, sampling rate, private listener and port isolation")
        finally:
            compose_command("down")

    failure = subprocess.run(
        ["docker", "run", "--rm", "--network", "none", "--platform", "linux/amd64",
         "-e", "GODEBUG=memprofilerate=0", args.image, "run",
         "--disable-telemetry=true", "--pprof-listen-address=127.0.0.1:6060"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=False)
    assert failure.returncode != 0 and b"pprof requires memory sampling" in failure.stdout, failure.stdout
    (args.output / "sampling-disabled.log").write_bytes(failure.stdout)
    print("PASS: disabled allocation sampling rejected at startup")


if __name__ == "__main__":
    main()
