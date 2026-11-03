#!/usr/bin/env python3
"""Validate full-duplex HTTP, native backpressure, cancellation and drain in a separate JVM."""
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
    checks = ROOT / "java/build/streaming"
    checks.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(jdk / "bin/javac"), "--release", "25", "-Xlint:all", "-Werror", "-cp", str(classes),
                    "-d", str(checks), str(ROOT / "harness/application/java/example/StreamingApplication.java")], check=True)
    fleet = "s" + secrets.token_hex(3)
    if any(Path("/dev/shm").glob("orbit-" + fleet + "-*")):
        raise RuntimeError("Generated acceptance namespace already exists")
    record = ROOT / ".local" / ("streaming-" + fleet)
    record.mkdir(parents=True)
    children = []
    logs = []
    def start(label, command):
        stderr = (record / (label + ".stderr")).open("w")
        logs.append(stderr)
        child = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=stderr, text=True, bufsize=1)
        children.append(child)
        (record / "children.json").write_text(json.dumps([child.pid for child in children]) + "\n")
        return child
    def line(child):
        value = child.stdout.readline().strip()
        if not value:
            raise RuntimeError(f"Acceptance child closed output; inspect {record}")
        print(value, flush=True)
        return value
    def expect(child, expected):
        value = line(child)
        if value != expected:
            raise RuntimeError(f"Expected {expected}, got {value}; inspect {record}")
    def send(child, value):
        child.stdin.write(value + "\n"); child.stdin.flush()
    try:
        peer = start("peer", [str(target / "debug/streaming"), fleet])
        ready = line(peer).split()
        if ready[0] != "PEER_READY":
            raise RuntimeError("Missing peer geometry")
        app = start("java", [str(jdk / "bin/java"), "--enable-native-access=ALL-UNNAMED", "--illegal-native-access=deny",
                             "-cp", os.pathsep.join(map(str, [classes, checks])), "example.StreamingApplication",
                             str(target / "debug/liborbit_java.so"), fleet, ready[1]])
        expect(app, "STREAM_READY")
        send(peer, "run")
        expect(peer, "WRITE_HEAD"); expect(app, "WRITE_BLOCKED"); send(peer, "read")
        expect(peer, "WRITE_CANCEL_HEAD"); expect(app, "WRITE_CANCEL_PENDING"); send(peer, "cancel-write")
        expect(app, "WRITE_CANCELLED"); send(peer, "write-cancelled")
        expect(peer, "READ_HEAD"); expect(app, "READ_PENDING"); send(peer, "cancel-read")
        expect(app, "READ_CANCELLED"); send(peer, "read-cancelled")
        expect(app, "READ_ABORTED"); send(peer, "read-aborted")
        expect(peer, "HTTP_CHECKS_PASSED")
        expect(peer, "DRAIN_HEAD"); expect(app, "DRAIN_PENDING")
        send(app, "withdraw"); expect(app, "WITHDRAWN"); send(peer, "drain")
        expect(peer, "STREAMING_CHECKS_PASSED"); expect(app, "APPLICATION_CLOSED")
        send(app, "stop"); expect(app, "STREAM_DRAINED")
        if app.wait() != 0:
            raise RuntimeError("Java drain failed")
        send(peer, "stop"); expect(peer, "PEER_DRAINED")
        if peer.wait() != 0:
            raise RuntimeError("Peer drain failed")
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            child.wait()
        for log in logs:
            log.close()
        with (record / "cleanup.txt").open("w") as cleanup:
            subprocess.run(["orbit", "clear", fleet, "--all", "--yes"], check=True, stdout=cleanup)
    (record / "result.json").write_text(json.dumps({"passed": True, "transfer_bytes": int(ready[1]),
        "checks": ["response backpressure", "single writer/read admission", "native byte ownership", "body read cancellation",
                   "backpressured write cancellation", "explicit read cancellation", "producer error resets output",
                   "binary full-duplex echo and EOF", "accepted stream survives withdrawal", "clean JVM/native drain"],
        "completed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}, indent=2) + "\n")
    print("STREAMING_ACCEPTANCE_PASSED " + str(record), flush=True)

if __name__ == "__main__":
    main()
