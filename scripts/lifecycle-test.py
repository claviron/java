#!/usr/bin/env python3
"""Validate application generations across sibling JVMs without restarting any process."""
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
    checks = ROOT / "java/build/lifecycle"
    checks.mkdir(parents=True, exist_ok=True)
    subprocess.run([str(jdk / "bin/javac"), "--release", "25", "-Xlint:all", "-Werror", "-cp", str(classes),
                    "-d", str(checks), str(ROOT / "harness/application/java/example/LifecycleApplication.java")], check=True)
    fleet = "l" + secrets.token_hex(3)
    if any(Path("/dev/shm").glob("orbit-" + fleet + "-*")):
        raise RuntimeError("Generated acceptance namespace already exists")
    record = ROOT / ".local" / ("lifecycle-" + fleet)
    record.mkdir(parents=True)
    children, logs = [], []
    def start(label, command):
        stderr = (record / (label + ".stderr")).open("w"); logs.append(stderr)
        child = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=stderr, text=True, bufsize=1)
        children.append(child)
        (record / "children.json").write_text(json.dumps([child.pid for child in children]) + "\n")
        return child
    def line(child):
        value = child.stdout.readline().strip()
        if not value:
            raise RuntimeError(f"Acceptance child closed output; inspect {record}")
        print(value, flush=True); return value
    def expect(child, expected):
        value = line(child)
        if value != expected:
            raise RuntimeError(f"Expected {expected}, got {value}; inspect {record}")
    def send(child, value):
        child.stdin.write(value + "\n"); child.stdin.flush()
    def actions(apps, action, generation, running):
        for tag, app in apps.items():
            expect(app, f"CONTROL {tag} {action} {generation} {str(running).lower()}")
    try:
        peer = start("peer", [str(target / "debug/lifecycle"), fleet])
        ready = line(peer).split()
        if ready[0] != "PEER_READY": raise RuntimeError("Missing peer geometry")
        apps = {}
        all_apps = {}
        for tag, name in (("one", "life"), ("two", "life"), ("other", "other")):
            app = start(tag, [str(jdk / "bin/java"), "--enable-native-access=ALL-UNNAMED", "--illegal-native-access=deny",
                             "-cp", os.pathsep.join(map(str, [classes, checks])), "example.LifecycleApplication",
                             str(target / "debug/libclaviron_java.so"), fleet, name, tag, ready[1]])
            expect(app, f"LIFE_READY {tag} {app.pid}")
            all_apps[tag] = app
            if tag != "other": apps[tag] = app
        send(apps["one"], "block-events")
        observed = [line(apps["one"]), line(apps["one"])]
        if set(observed) != {"EVENT_BLOCKED one", "EVENTS_PUBLISHED one"}:
            raise RuntimeError("Missing blocked application-event path")
        expect(apps["two"], "EVENT_BLOCKED two")
        send(peer, "run"); expect(peer, "CONTROL_FILTERS_SENT")
        for tag, app in apps.items(): expect(app, "PROBE " + tag); expect(app, "SHUTDOWN " + tag)
        send(peer, "filters-checked"); expect(peer, "HOLDS_SENT")
        for tag, app in apps.items(): expect(app, "HELD " + tag + " 1")
        send(peer, "stop"); expect(peer, "STOPPED_CHECKS_PASSED"); actions(apps, "STOP", 1, False)
        for tag, app in apps.items(): send(app, "check-stopped"); expect(app, "STOPPED_CHECKED " + tag)
        send(peer, "start"); expect(peer, "STARTED_CHECKS_PASSED"); actions(apps, "START", 2, True)
        for tag, app in apps.items():
            send(app, "release-events"); expect(app, "EVENTS_RELEASED " + tag)
            send(app, "complete-late"); expect(app, "LATE_DROPPED " + tag)
        send(peer, "idempotent-start"); expect(peer, "IDEMPOTENT_START_SENT"); actions(apps, "START", 2, True)
        send(peer, "blocked"); expect(peer, "BLOCKED_SENT")
        for tag, app in apps.items():
            expected = {"WRITE " + tag + " 2", "READ " + tag + " 2"}
            observed = [line(app), line(app)]
            if set(observed) != expected: raise RuntimeError(f"Missing blocked operations: {observed}")
        send(peer, "restart"); expect(peer, "RESTART_CHECKS_PASSED"); actions(apps, "RESTART", 3, True)
        send(apps["one"], "restart"); actions(apps, "RESTART", 4, True)
        send(peer, "local-done"); expect(peer, "FINAL_HOLDS_SENT")
        for tag, app in apps.items(): expect(app, "HELD " + tag + " 4")
        send(peer, "final-stop"); expect(peer, "LIFECYCLE_CHECKS_PASSED"); actions(apps, "STOP", 4, False)
        send(all_apps["other"], "check-unrelated"); expect(all_apps["other"], "UNRELATED_CHECKED other")
        for tag, app in all_apps.items():
            send(app, "stop"); expect(app, "LIFE_DRAINED " + tag)
            if app.wait() != 0: raise RuntimeError("JVM drain failed")
        send(peer, "stop"); expect(peer, "PEER_DRAINED")
        if peer.wait() != 0: raise RuntimeError("Peer drain failed")
    finally:
        for child in children:
            if child.poll() is None: child.terminate()
        for child in children: child.wait()
        for log in logs: log.close()
        with (record / "cleanup.txt").open("w") as cleanup:
            subprocess.run(["orbit", "clear", fleet, "--all", "--yes"], stdout=cleanup, check=True)
    (record / "result.json").write_text(json.dumps({"passed": True, "jvms": 3,
        "checks": ["same PID/lane/incarnation through lifecycle", "Java runtime/name targeting", "503 without handler while stopped",
                   "old generation cancellation", "fresh generation before callbacks", "idempotent start", "sibling propagation once",
                   "lifecycle progresses through full application-event queues", "blocked read/write retirement", "late response ignored", "local restart fanout", "unrelated application isolation",
                   "binary command payload and edge shutdown notice", "close with ignored cancelled handler futures", "authoritative status generations"],
        "completed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}, indent=2) + "\n")
    print("LIFECYCLE_ACCEPTANCE_PASSED " + str(record), flush=True)

if __name__ == "__main__": main()
