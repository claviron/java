#!/usr/bin/env python3
"""Run two JVM applications against Claviron's real SHM HTTP and event transports."""

import argparse
import datetime
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true", help="reuse a completed scripts/test.py build")
    args = parser.parse_args()
    if not args.skip_build:
        subprocess.run([sys.executable, "scripts/test.py"], cwd=ROOT, check=True)
    jdk = Path(os.environ["JAVA_HOME"])
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    classes = ROOT / "java/build/classes"
    examples = ROOT / "java/build/examples"
    examples.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(jdk / "bin/javac"), "--release", "25", "-Xlint:all", "-Werror", "-cp", str(classes),
                    "-d", str(examples), str(ROOT / "examples/plain/PlainApplication.java")], check=True)
    fleet = "j" + secrets.token_hex(3)
    edge_fleet = fleet + "e"
    if any(Path("/dev/shm").glob("orbit-" + fleet + "-*")) or any(Path("/dev/shm").glob("orbit-" + edge_fleet + "-*")):
        raise RuntimeError("Generated acceptance namespace already exists")
    run = ROOT / ".local" / ("application-" + fleet)
    run.mkdir(parents=True)
    children = []
    streams = []

    def start(label, command):
        stderr = (run / (label + ".stderr")).open("w")
        streams.append(stderr)
        process = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=stderr, text=True, bufsize=1)
        children.append((label, process))
        (run / "children.json").write_text(json.dumps({name: child.pid for name, child in children}, indent=2) + "\n")
        return process

    def expect(process, line):
        actual = process.stdout.readline().strip()
        if actual != line:
            raise RuntimeError(f"Expected {line!r}, received {actual!r}; inspect {run}")
        print(actual, flush=True)

    def send(process, line):
        process.stdin.write(line + "\n")
        process.stdin.flush()

    def stopped(process):
        status = process.wait()
        if status != 0:
            raise RuntimeError(f"Owned acceptance process exited with {status}; inspect {run}")

    passed = False
    try:
        peer = start("peer", [str(target / "debug/claviron-java-application-check"), fleet, edge_fleet, "plain"])
        expect(peer, "PEER_READY")
        applications = []
        for tag in ("one", "two"):
            # Test-fixture budgets, not production defaults. No durations or load test.
            process = start(tag, [str(jdk / "bin/java"), "--enable-native-access=ALL-UNNAMED", "--illegal-native-access=deny",
                                 "-cp", os.pathsep.join(map(str, [classes, examples])), "example.PlainApplication",
                                 str(target / "debug/libclaviron_java.so"), fleet, "plain", tag, "1", "64", "4194304", "2", "8", "16", "16"])
            expect(process, "READY " + tag)
            applications.append(process)
        send(peer, "run")
        expect(peer, "EDGE_CHECKS_PASSED")
        send(applications[0], "publish-local")
        expect(applications[0], "PUBLISHED one")
        send(peer, "published")
        expect(peer, "FIRST_CHECKS_PASSED")
        send(applications[0], "unsubscribe-edge")
        expect(applications[0], "UNSUBSCRIBED one")
        send(peer, "unsubscribed")
        expect(peer, "APPLICATION_CHECKS_PASSED")
        for tag, process in zip(("one", "two"), applications):
            send(process, "stop")
            expect(process, "DRAINED " + tag)
            stopped(process)
        send(peer, "stop")
        expect(peer, "PEER_DRAINED")
        stopped(peer)
        passed = True
    finally:
        # Assertion/process failure, not an invented deadline, cancels only this harness's children.
        for _, process in children:
            if process.poll() is None:
                process.terminate()
        for _, process in children:
            process.wait()
        for stream in streams:
            stream.close()
        # Both fleets are unique to this invocation; every mapping owner has exited.
        for name in (fleet, edge_fleet):
            subprocess.run(["orbit", "clear", name, "--all", "--yes"], check=True,
                           stdout=(run / (name + ".cleanup")).open("w"))
    record = {"passed": passed, "platform": "Linux x86-64", "jvms": 2,
              "application_fleet": fleet, "edge_event_fleet": edge_fleet,
              "checks": ["plain HTTP on both lanes", "raw URI/query and repeated/byte headers", "handler error 500", "idle HTTP cancellation",
                         "sibling ring sharing", "Edge bridge delivery", "once per subscriber", "unsubscribe",
                         "graceful drain and native/JVM exit"],
              "completed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    (run / "result.json").write_text(json.dumps(record, indent=2) + "\n")
    print("APPLICATION_ACCEPTANCE_PASSED " + str(run), flush=True)


if __name__ == "__main__":
    main()
