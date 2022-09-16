#!/usr/bin/env bash

PXF_SBINDIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
PXF_HOME="$(cd -- "$(dirname -- "${PXF_SBINDIR}")" &>/dev/null && pwd)"

if [[ "${EUID}" != 0 ]]; then
    echo "Please run as root (or via sudo)"
    exit 1
fi

echo "Stopping and disabling user@$(id -ru).service"
systemctl start "user@$(id -u gpadmin).service"
systemctl disable "user@$(id -u gpadmin).service"
if cmp --silent "${PXF_HOME}/conf/user@.service" /usr/lib/systemd/system/user@.service; then
    echo "Removing /usr/lib/systemd/system/user@.service"
    rm -rf /usr/lib/systemd/system/user@.service
else
    echo "The file /usr/lib/systemd/system/user@.service is not the same as ${PXF_HOME}/conf/user@.service; leaving the file in place"
fi
