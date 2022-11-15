#!/usr/bin/env bash

# Load settings
root=$(cd "$(dirname "$0")/.." && pwd)
bin=${root}/bin

# shellcheck source=/dev/null
. "${bin}/gphd-env.sh"

if ! cluster_initialized && [ ! "${PXFDEMO}" ]; then
    echo "cluster not initialized"
    echo "please run ${bin}/init-gphd.sh"
    exit 1
fi

# Start PXF
pushd "${GPHD_ROOT}" &>/dev/null || exit 1
for ((i = 0; i < WORKERS; i++)); do
    "${bin}/pxf-service.sh" start "${i}" | sed "s/^/node $i: /"
done
popd &>/dev/null || exit 1
