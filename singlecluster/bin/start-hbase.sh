#!/usr/bin/env bash

# Load settings
root=$(cd "$(dirname "$0")/.." && pwd)
bin=${root}/bin

# shellcheck source=/dev/null
. "${bin}/gphd-env.sh"

# Check to see HDFS is up
if ! hdfs_running; then
    echo "HDFS is not ready, HBase cannot start"
    echo "Please see HDFS is up and out of safemode"
    exit 1
fi

# Check to see Zookeeper is up
if ! zookeeper_running; then
    echo "Zookeeper is not running, HBase cannot start"
    echo "Have you run start-zookeeper.sh?"
    exit 1
fi

echo Starting HBase...
# Start HBase master
"${HBASE_BIN}/hbase-daemon.sh" --config "${HBASE_CONF}" start master

# Start regions
for ((i = 0; i < WORKERS; i++)); do
    "${bin}/hbase-regionserver.sh" start "${i}" | sed "s/^/node $i: /"
done

# Start Stargate
if [ "$START_STARGATE" == "true" ]; then
    echo Starting Stargate...
    "${HBASE_BIN}/hbase-daemon.sh" --config "${HBASE_CONF}" start rest -p "${STARGATE_PORT}"
fi
