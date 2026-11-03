#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ $# -eq 0 ]]; then
  exec python3 scripts/release.py --help
fi
for argument in "$@"; do
  if [[ "$argument" == --prepare ]]; then
    source scripts/lib/cargo.sh
    prepare_cargo_target
    break
  fi
done
exec python3 scripts/release.py "$@"
