#!/usr/bin/env bash

usage="Usage: $(basename "$0") <start|stop> <node_id>"

if [ $# -ne 2 ]; then
    echo "${usage}"
    exit 1
fi

command=$1
nodeid=$2

# Load settings
root=$(cd "$(dirname "$0")/.." && pwd)
bin=${root}/bin

# shellcheck source=/dev/null
. "${bin}/gphd-env.sh"

datanode_root=${HADOOP_STORAGE_ROOT}/datanode${nodeid}
datanode_conf=${datanode_root}/etc/hadoop

export HADOOP_DATANODE_OPTS="-Dhadoop.tmp.dir=$datanode_root/data"
export HADOOP_CONF_DIR=${datanode_conf}
export HADOOP_IDENT_STRING=${USER}-node${nodeid}

# remove the conf directory
# this allows a refresh
# FIXME: duplicate code in hbase-regionserver.sh
function clear_conf_directory() {
    if [ -d "${datanode_conf}" ]; then
        rm -rf "${datanode_conf}"
    fi
    mkdir -p "${datanode_conf}"
}

# copy all files from original hadoop conf directory
# FIXME: duplicate code in hbase-regionserver.sh
function copy_conf_files() {
    while IFS= read -r -d '' file; do
        cp "${file}" "${datanode_conf}/$(basename "${file}")"
    done < <(find "${HADOOP_CONF}" -type f -not -iname "*~" -print0)
}

# add single-cluster properties
function patch_hdfs_site() {
    sed -e "/^<configuration>$/ a\\
    <property>\\
    <name>dfs.datanode.address</name>\\
    <value>0.0.0.0:5001$nodeid</value>\\
    </property>\\
    <property>\\
    <name>dfs.datanode.http.address</name>\\
    <value>0.0.0.0:5008$nodeid</value>\\
    </property>\\
    <property>\\
    <name>dfs.datanode.ipc.address</name>\\
    <value>0.0.0.0:5002$nodeid</value>\\
    </property>" \
        "${HADOOP_CONF}/hdfs-site.xml" >"${datanode_conf}/hdfs-site.xml"
}

function dostart() {
    clear_conf_directory
    copy_conf_files
    patch_hdfs_site
}

case "$command" in
    "start") dostart ;;
    "stop") ;;
    *)
        echo "unknown command ${command}"
        echo "${usage}"
        exit 1
        ;;
esac

"${HADOOP_SBIN}/hadoop-daemon.sh" --config "${datanode_conf}" "${command}" datanode
