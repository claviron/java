#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ $# -eq 0 ]]; then
  exec python3 scripts/remote-build.py --sync .release-sync.local.json
fi
case "$1" in
  --help|-h)
    echo 'Usage: scripts/sync.sh [--config FILE]'
    echo 'Collect verified prebuilds only; never starts a build or publication.'
    ;;
  --config)
    if [[ $# -ne 2 ]]; then echo 'Usage: scripts/sync.sh [--config FILE]' >&2; exit 2; fi
    exec python3 scripts/remote-build.py --sync "$2"
    ;;
  *) echo 'Usage: scripts/sync.sh [--config FILE]' >&2; exit 2 ;;
esac
