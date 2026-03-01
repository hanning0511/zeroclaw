#!/usr/bin/env bash
set -euo pipefail

# Remove corrupted toolchain installs that can break rustc startup on long-lived runners.
# Usage: ./scripts/ci/self_heal_rust_toolchain.sh [toolchain]

TOOLCHAIN="${1:-1.92.0}"

if ! command -v rustup >/dev/null 2>&1; then
  echo "rustup not installed yet; skipping rust toolchain self-heal."
  exit 0
fi

if rustc "+${TOOLCHAIN}" --version >/dev/null 2>&1; then
  echo "Rust toolchain ${TOOLCHAIN} is healthy."
  exit 0
fi

echo "Rust toolchain ${TOOLCHAIN} appears unhealthy; removing cached installs."
for candidate in \
  "${TOOLCHAIN}" \
  "${TOOLCHAIN}-x86_64-apple-darwin" \
  "${TOOLCHAIN}-aarch64-apple-darwin" \
  "${TOOLCHAIN}-x86_64-unknown-linux-gnu" \
  "${TOOLCHAIN}-aarch64-unknown-linux-gnu"
do
  rustup toolchain uninstall "${candidate}" >/dev/null 2>&1 || true
done

