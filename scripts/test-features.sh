#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
cargo check --locked --workspace --no-default-features
cargo check --locked --package tinybus --no-default-features --features modules
cargo metadata --locked --format-version 1 --manifest-path examples/Cargo.toml >/dev/null
cargo test --locked --workspace --no-default-features --features macros
cargo test --locked --workspace --all-features
