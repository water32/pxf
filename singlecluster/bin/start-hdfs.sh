#!/usr/bin/env bash

# Load settings
root=$(cd "$(dirname "$0")/.." && pwd)
bin=${root}/bin

# shellcheck source=/dev/null
. "${bin}/gphd-env.sh"

if ! cluster_initialized; then
    echo "cluster not initialized"
    echo "please run ${bin}/init-gphd.sh"
    exit 1
fi

# Hadoop
# Start NameNode
echo Starting HDFS...
"${HADOOP_SBIN}/hadoop-daemon.sh" --config "${HADOOP_CONF}" start namenode | sed "s/^/master: /"

# Start DataNodes
for ((i = 0; i < WORKERS; i++)); do
    "${bin}/hadoop-datanode.sh" start "${i}" | sed "s/^/node $i: /"
done

# Wait for Namenode to leave safemode
"${HADOOP_BIN}/hdfs" dfsadmin -safemode wait || sleep 5
