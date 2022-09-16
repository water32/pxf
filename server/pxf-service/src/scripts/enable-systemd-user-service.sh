#!/usr/bin/env bash

PXF_SBINDIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
PXF_HOME="$(cd -- "$(dirname -- "${PXF_SBINDIR}")" &>/dev/null && pwd)"

if [[ "${EUID}" != 0 ]]; then
    echo "Please run as root (or via sudo)"
    exit 1
fi

if [[ ! -e /usr/lib/systemd/system/user@.service ]]; then
    echo "Installing ${PXF_HOME}/conf/user@.service into /usr/lib/systemd/system/"
    install -m 0644 "${PXF_HOME}/conf/user@.service" /usr/lib/systemd/system/
else
    echo "There is already a user@service file in /usr/lib/systemd/system/; skipping installation..."
fi

echo "Enabling and starting user@$(id -ru gpadmin).service ..."
systemctl enable "user@$(id -ru gpadmin).service"
systemctl start "user@$(id -ru gpadmin).service"
