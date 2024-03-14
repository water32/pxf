#!/usr/bin/env bash

set -eo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

source "${SCRIPT_DIR}/common.bash"

run_pgbench "limit_query" 10 1000

run_pgbench "limit_query" 20 1000

run_pgbench "limit_query" 30 1000

run_pgbench "limit_query" 40 1000

run_pgbench "limit_query" 50 1000

run_pgbench "limit_query" 60 1000

run_pgbench "limit_query" 100 1500
