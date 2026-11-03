#!/usr/bin/env bash
# Keep build output off a shared development mount when a local helper exists.
prepare_cargo_target() {
  case "$(uname -s)" in Linux|FreeBSD) ;; *) return 0 ;; esac
  local filesystem
  filesystem="$(df -PT . 2>/dev/null | awk 'NR == 2 { print $2 }')" || return 0
  case "$filesystem" in 9p|9pfs|p9fs|virtfs|virtiofs) ;; *) return 0 ;; esac
  if ! command -v set_cargo_target >/dev/null 2>&1 &&
    [[ -r "$HOME/.config/ai/set-cargo-target.sh" ]]; then
    source "$HOME/.config/ai/set-cargo-target.sh"
  fi
  if command -v set_cargo_target >/dev/null 2>&1; then
    set_cargo_target
  fi
}
