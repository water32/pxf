#!/usr/bin/env bash

# Load settings
root=$(cd "$(dirname "$0")/.." && pwd)
bin=${root}/bin

# shellcheck source=/dev/null
. "${bin}/gphd-env.sh"

# Check to see HDFS is up
if ! hdfs_running; then
    echo "HDFS is not running, YARN cannot start"
    echo "Please see HDFS is up and out of safemode"
    exit 1
fi

# Hadoop
# Start ResourceManager
echo "Starting YARN"
"${HADOOP_SBIN}/yarn-daemon.sh" --config "${HADOOP_CONF}" start resourcemanager | sed "s/^/master: /"

# Start NodeManager(s)
for ((i = 0; i < WORKERS; i++)); do
    "${bin}/yarn-nodemanager.sh" start "${i}" | sed "s/^/node $i: /"
done

# HistoryServer
if [ "$START_YARN_HISTORY_SERVER" != "true" ]; then
    echo "Mapreduce History Server wont be started"
    exit 0
fi

"${HADOOP_SBIN}/mr-jobhistory-daemon.sh" --config "${HADOOP_CONF}" start historyserver | sed "s/^/master: /"
