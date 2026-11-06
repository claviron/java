#!/usr/bin/env python3
"""Validate the FFM foundation without a framework, server or load test."""

import argparse
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def run(*command):
    subprocess.run(command, cwd=ROOT, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-rust-checks", action="store_true",
                        help="reuse successful Rust format/lint/tests; still build native artifacts")
    args = parser.parse_args()
    jdk = Path(os.environ["JAVA_HOME"])
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise SystemExit("This foundation harness currently validates Linux x86-64 only")
    if not args.skip_rust_checks:
        run("cargo", "fmt", "--all", "--", "--check")
        run("cargo", "clippy", "--workspace", "--all-features", "--all-targets", "--", "-D", "warnings")
        run("cargo", "test", "--workspace", "--all-features")
    run("cargo", "build", "--workspace", "--all-features", "--all-targets")
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    if not target.is_absolute():
        target = ROOT / target
    output = ROOT / "java/build"
    classes = output / "classes"
    tests = output / "tests"
    for directory in (classes, tests):
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True)
    sources = sorted((ROOT / "java/src/main/java").rglob("*.java"))
    checks = sorted((ROOT / "java/src/test/java").rglob("*.java"))
    run(str(jdk / "bin/javac"), "--release", "25", "-Xlint:all", "-Werror", "-d", str(classes), *map(str, sources))
    run(str(jdk / "bin/jar"), "--create", "--file", str(output / "orbit.jar"), "-C", str(classes), ".")
    run(str(jdk / "bin/javac"), "--release", "25", "-Xlint:all", "-Werror", "-cp", str(classes), "-d", str(tests), *map(str, checks))
    run(str(jdk / "bin/java"), "--illegal-native-access=deny",
        "-cp", os.pathsep.join(map(str, [classes, tests])), "dev.claviron.orbit.NativeAccessCheck",
        str(target / "debug/libclaviron_java.so"))
    run(str(jdk / "bin/java"), "--enable-native-access=ALL-UNNAMED", "--illegal-native-access=deny",
        "-cp", os.pathsep.join(map(str, [classes, tests])), "dev.claviron.orbit.BindingChecks",
        str(target / "debug/libclaviron_java.so"), str(target / "debug/libclaviron_java_fixture.so"))
    probe = output / "c-layout"
    run(*shlex.split(os.environ.get("CC", "cc")), "-std=c11", "-Wall", "-Wextra", "-Werror",
        "-I", str(ROOT / "native/include"), str(ROOT / "harness/layout.c"), "-o", str(probe))
    run(str(probe))
    print("C, Rust and Java ABI/FFM foundation checks passed")


if __name__ == "__main__":
    main()
