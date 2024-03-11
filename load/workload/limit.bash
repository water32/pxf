#!/usr/bin/env bash

set -eo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

source "${SCRIPT_DIR}/common.bash"

# run limit workload only
run_pgbench "limit_query"
