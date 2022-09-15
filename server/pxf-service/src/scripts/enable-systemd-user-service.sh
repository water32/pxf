#!/usr/bin/env bash

# TODO: move to PXF repo
if [[ "${EUID}" != 0 ]]; then
    echo "Please run as root (or via sudo)"
    exit 1
fi

# TODO: determine PXF_HOME instead of reading from env
install -m 0644 "${PXF_HOME}/conf/user@.service" /usr/lib/systemd/system/
systemctl enable "user@$(id -u gpadmin).service"
systemctl start "user@$(id -u gpadmin).service"
