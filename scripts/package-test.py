#!/usr/bin/env python3
"""Test release packaging and native JAR loading without publishing or load tests."""
import argparse
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--native", type=Path, help="existing current-host native library; enables real packaged FFM acceptance")
args = parser.parse_args()
env = os.environ.copy()
if args.native:
    env["ORBIT_JAVA_TEST_NATIVE"] = str(args.native.resolve(strict=True))
subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests/scripts", "-v"], cwd=ROOT, env=env, check=True)
