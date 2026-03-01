#!/usr/bin/env bash
set -euo pipefail

toolchain="${1:-1.92.0}"

echo "Ensuring cargo component is available for toolchain: ${toolchain}"

if ! rustup run "${toolchain}" cargo --version >/dev/null 2>&1; then
    echo "cargo is missing for ${toolchain}; installing component..."
    rustup component add cargo --toolchain "${toolchain}"
fi

rustup run "${toolchain}" rustc --version

# Some self-hosted runners occasionally surface transient "Text file busy"
# while cargo is being refreshed. Retry a few times to stabilize the job.
for attempt in 1 2 3; do
    if rustup run "${toolchain}" cargo --version; then
        exit 0
    fi
    if [ "${attempt}" -eq 3 ]; then
        echo "cargo is still unavailable after ${attempt} attempts" >&2
        exit 1
    fi
    echo "cargo probe failed on attempt ${attempt}; retrying in 2s..."
    sleep 2
done
