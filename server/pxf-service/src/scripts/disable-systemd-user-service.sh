#!/usr/bin/env bash

# TODO: move to PXF repo
if [[ "${EUID}" != 0 ]]; then
    echo "Please run as root (or via sudo)"
    exit 1
fi

systemctl start "user@$(id -u gpadmin).service"
systemctl disable "user@$(id -u gpadmin).service"
rm -rd /usr/lib/systemd/system/user@.service
