#!/usr/bin/env bash
# Usage: ./compile_deploy.sh <function-app-name>
set -euo pipefail
cd "$(dirname "$0")"
cargo build --release --target x86_64-unknown-linux-musl
func azure functionapp publish app-ciso
