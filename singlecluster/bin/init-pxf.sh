#!/usr/bin/env bash

# Load settings
root=$(cd "$(dirname "$0")/.." && pwd)
bin=${root}/bin

# shellcheck source=/dev/null
. "${bin}/gphd-env.sh"

# Initialize PXF instances
for ((i=0; i < WORKERS; i++)); do
    echo initializing PXF instance "${i}"
    if ! "${bin}/pxf-service.sh" init "${i}"; then
        echo
        echo "tcServer instance #${i} initialization failed"
        echo "check console output"
        exit 1
    fi
done
