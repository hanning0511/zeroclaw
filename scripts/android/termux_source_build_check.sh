#!/usr/bin/env bash
set -euo pipefail

TARGET="aarch64-linux-android"
RUN_CARGO_CHECK=0

usage() {
  cat <<'EOF'
Usage:
  scripts/android/termux_source_build_check.sh [--target <triple>] [--run-cargo-check]

Options:
  --target <triple>    Android Rust target (default: aarch64-linux-android)
                       Supported: aarch64-linux-android, armv7-linux-androideabi
  --run-cargo-check    Run cargo check --locked --target <triple> --no-default-features
  -h, --help           Show this help

Purpose:
  Validate Android source-build environment for ZeroClaw, with focus on:
  - Termux native builds using plain clang
  - NDK cross-build overrides (CARGO_TARGET_*_LINKER and CC_*)
  - Common cc-rs linker mismatch failures
EOF
}

log() {
  printf '[android-selfcheck] %s\n' "$*"
}

warn() {
  printf '[android-selfcheck] warning: %s\n' "$*" >&2
}

die() {
  printf '[android-selfcheck] error: %s\n' "$*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      [[ $# -ge 2 ]] || die "--target requires a value"
      TARGET="$2"
      shift 2
      ;;
    --run-cargo-check)
      RUN_CARGO_CHECK=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "unknown argument: $1 (use --help)"
      ;;
  esac
done

case "$TARGET" in
  aarch64-linux-android|armv7-linux-androideabi) ;;
  *)
    die "unsupported target '$TARGET' (expected aarch64-linux-android or armv7-linux-androideabi)"
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" >/dev/null 2>&1 && pwd || pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." >/dev/null 2>&1 && pwd || pwd)"
CONFIG_FILE="$REPO_ROOT/.cargo/config.toml"
cd "$REPO_ROOT"

TARGET_UPPER="$(printf '%s' "$TARGET" | tr '[:lower:]-' '[:upper:]_')"
TARGET_UNDERSCORE="${TARGET//-/_}"
CARGO_LINKER_VAR="CARGO_TARGET_${TARGET_UPPER}_LINKER"
CC_LINKER_VAR="CC_${TARGET_UNDERSCORE}"

is_termux=0
if [[ -n "${TERMUX_VERSION:-}" ]] || [[ "${PREFIX:-}" == *"/com.termux/files/usr"* ]]; then
  is_termux=1
fi

extract_linker_from_config() {
  [[ -f "$CONFIG_FILE" ]] || return 0
  awk -v target="$TARGET" '
    $0 ~ "^\\[target\\." target "\\]$" { in_section=1; next }
    in_section && $0 ~ "^\\[" { in_section=0 }
    in_section && $1 == "linker" {
      gsub(/"/, "", $3);
      print $3;
      exit
    }
  ' "$CONFIG_FILE"
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

is_executable_tool() {
  local tool="$1"
  if [[ "$tool" == */* ]]; then
    [[ -x "$tool" ]]
  else
    command_exists "$tool"
  fi
}

log "repo: $REPO_ROOT"
log "target: $TARGET"
if [[ "$is_termux" -eq 1 ]]; then
  log "environment: Termux/native"
else
  log "environment: non-Termux (likely desktop/CI)"
fi

command_exists rustup || die "rustup is not installed"
command_exists cargo || die "cargo is not installed"

if ! rustup target list --installed | grep -Fx "$TARGET" >/dev/null 2>&1; then
  die "Rust target '$TARGET' is not installed. Run: rustup target add $TARGET"
fi

config_linker="$(extract_linker_from_config || true)"
cargo_linker_override="${!CARGO_LINKER_VAR:-}"
cc_linker_override="${!CC_LINKER_VAR:-}"

if [[ -n "$config_linker" ]]; then
  log "config linker ($TARGET): $config_linker"
else
  warn "no linker configured for $TARGET in .cargo/config.toml"
fi

if [[ -n "$cargo_linker_override" ]]; then
  log "env override $CARGO_LINKER_VAR=$cargo_linker_override"
fi
if [[ -n "$cc_linker_override" ]]; then
  log "env override $CC_LINKER_VAR=$cc_linker_override"
fi

effective_linker="${cargo_linker_override:-${config_linker:-clang}}"
log "effective linker: $effective_linker"

if [[ "$is_termux" -eq 1 ]]; then
  command_exists clang || die "clang is required in Termux. Run: pkg install -y clang pkg-config"

  if [[ "${config_linker:-}" != "clang" ]]; then
    warn "Termux native build should use linker = \"clang\" for $TARGET"
  fi

  if [[ -n "$cargo_linker_override" && "$cargo_linker_override" != "clang" ]]; then
    warn "Termux native build usually should unset $CARGO_LINKER_VAR (currently '$cargo_linker_override')"
  fi
  if [[ -n "$cc_linker_override" && "$cc_linker_override" != "clang" ]]; then
    warn "Termux native build usually should unset $CC_LINKER_VAR (currently '$cc_linker_override')"
  fi
else
  if [[ -n "$cargo_linker_override" && -z "$cc_linker_override" ]]; then
    warn "cross-build may still fail in cc-rs crates; consider setting $CC_LINKER_VAR=$cargo_linker_override"
  fi
fi

if ! is_executable_tool "$effective_linker"; then
  if [[ "$is_termux" -eq 1 ]]; then
    die "effective linker '$effective_linker' is not executable in PATH"
  fi
  warn "effective linker '$effective_linker' not found (expected for some desktop hosts without NDK toolchain)"
fi

if [[ "$RUN_CARGO_CHECK" -eq 1 ]]; then
  log "running cargo check --locked --target $TARGET --no-default-features"
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/zeroclaw-android-selfcheck-target}" \
    cargo check --locked --target "$TARGET" --no-default-features
  log "cargo check completed successfully"
else
  log "skip cargo check (use --run-cargo-check to enable)"
fi

log "self-check completed"
