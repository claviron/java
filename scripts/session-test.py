#!/usr/bin/env python3
"""Validate WebSocket and WebTransport message sessions over a real Link in a separate JVM."""
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    jdk = Path(os.environ["JAVA_HOME"])
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    subprocess.run(["cargo", "build", "--workspace", "--all-features", "--all-targets"], cwd=ROOT, check=True)
    build = ROOT / "java/build/session"
    if build.exists():
        shutil.rmtree(build)
    build.mkdir(parents=True)
    sources = sorted((ROOT / "java/src/main/java").rglob("*.java"))
    subprocess.run([str(jdk / "bin/javac"), "--release", "25", "-Xlint:all", "-Werror", "-d", str(build),
                    *map(str, sources), str(ROOT / "harness/application/java/example/SessionApplication.java")], check=True)
    library = target / ("debug/libclaviron_java" + (".dylib" if sys.platform == "darwin" else ".so"))
    fleet = "w" + secrets.token_hex(3)
    children = []

    def start(command):
        child = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)
        children.append(child)
        return child

    def expect(child, expected):
        value = child.stdout.readline().strip()
        print(value, flush=True)
        if value != expected:
            raise RuntimeError(f"Expected {expected}, got {value!r}")

    def send(child):
        child.stdin.write("next\n")
        child.stdin.flush()

    try:
        app = start([str(jdk / "bin/java"), "--enable-native-access=ALL-UNNAMED", "--illegal-native-access=deny",
                     "-cp", str(build), "example.SessionApplication", str(library), fleet])
        expect(app, "SESSION_READY")
        peer = start([str(target / "debug/session"), fleet])
        expect(peer, "PEER_READY")
        send(peer)
        for _ in range(2):
            expect(peer, "CLOSE_ANSWERED")
            expect(app, "SESSION_CLOSE 4001 done")
            send(peer)
        expect(peer, "GONE_OBSERVED")
        expect(app, "SESSION_CLOSE 1006")
        send(peer)
        expect(peer, "END_SENT")
        expect(app, "SESSION_END")
        send(peer)
        expect(peer, "TOO_LARGE_SENT")
        expect(app, "SESSION_TOO_LARGE")
        send(peer)
        expect(peer, "SESSION_CHECKS_PASSED")
        if peer.wait() != 0:
            raise RuntimeError("Peer failed")
        app.stdin.write("stop\n")
        app.stdin.flush()
        expect(app, "SESSION_STOPPED")
        if app.wait() != 0:
            raise RuntimeError("Java application failed")
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        for child in children:
            child.wait()
        subprocess.run(["orbit", "clear", fleet, "--all", "--yes"], check=True, stdout=subprocess.DEVNULL)
    print("SESSION_ACCEPTANCE_PASSED", flush=True)


if __name__ == "__main__":
    main()
