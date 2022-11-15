#!/usr/bin/env bash

# Load settings
root=$(cd "$(dirname "$0")/.." && pwd)
bin=${root}/bin

# shellcheck source=/dev/null
. "${bin}/gphd-env.sh"

for ((i = 0; i < WORKERS; i++)); do
    "${bin}/hadoop-datanode.sh" stop "${i}" | sed "s/^/node $i: /"
done

# Stop NameNode
"${HADOOP_SBIN}/hadoop-daemon.sh" --config "${HADOOP_CONF}" stop namenode | sed "s/^/master: /"
