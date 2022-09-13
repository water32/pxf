#!/usr/bin/env bash

dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
[[ -e ${dir}/common.sh ]] || exit 1
# shellcheck source=./common.sh
source "${dir}/common.sh"

before_all() {
    pxf cluster stop

    PXF_BASE="$(mktemp -d)"
    export PXF_BASE
    echo "Updating PXF_BASE to ${PXF_BASE}"
    # PXF_BASE_DIR is already exported, don't need to re-export
    PXF_BASE_DIR="${PXF_BASE}"

    pxf cluster prepare
}
before_all

# ************************************************************************************************************
# ***** TEST Suite starts with PXF not running on any nodes **************************************************
# ************************************************************************************************************

# === Test "pxf cluster start (without enabling systemd user services)" =====================================
expected_output=\
"Starting PXF on master host and 2 segment hosts...\n\
ERROR: PXF failed to start on 3 out of 3 hosts\n\
mdw ==> ${red}ERROR: The systemd user service for user 'gpadmin' (id=1025) is not active.${reset}\n\n\
sdw1 ==> ${red}ERROR: The systemd user service for user 'gpadmin' (id=1025) is not active.${reset}\n\n\
sdw2 ==> ${red}ERROR: The systemd user service for user 'gpadmin' (id=1025) is not active.${reset}\n"
test_enabling_systemd_without_user_services() {
    # given:
    #      : PXF is not running
    pxf cluster stop
    # when : a DBA configures PXF to use systemd
    sed -i.bak -e '/PXF_USE_SYSTEMD/c\export PXF_USE_SYSTEMD=true' "${PXF_BASE_DIR}/conf/pxf-env.sh"
    #      : AND runs "pxf cluster sync"
    pxf cluster sync
    #      : AND runs "pxf cluster start"
    local output
    local result
    output="$(pxf cluster start 2>&1)"
    result="$?"
    assert_equals "1" "${result}" "pxf cluster start should not succeed"
    # then : it prints an error message
    assert_equals "$(echo -e ${expected_output})" "${output}" "pxf cluster start should not succeed"
}

run_test test_enabling_systemd_without_user_services "pxf cluster start should not succeed"
# ===========================================================================================================

# === Test "pxf cluster start (with enabling systemd user services)" =====================================
enable_systemd_user_service() {
    local host="${1}"
    ssh centos@"${host}" "sudo install -m 0644 $PXF_HOME/conf/user@.service /usr/lib/systemd/system/ && sudo systemctl enable user@\$(id -u gpadmin).service && sudo systemctl start user@\$(id -u gpadmin).service"
}

disable_systemd_user_service() {
    local host="${1}"
    ssh centos@"${host}" "sudo systemctl stop user@\$(id -u gpadmin).service && sudo systemctl disable user@\$(id -u gpadmin).service && sudo rm /usr/lib/systemd/system/user@.service"
}

expected_output=\
"Starting PXF on master host and 2 segment hosts...\n\
PXF started successfully on 3 out of 3 hosts"
test_enabling_systemd_with_user_services() {
    # given:
    #      : PXF is not running
    pxf cluster stop
    for host in "${all_cluster_hosts[@]}"; do
        enable_systemd_user_service "${host}"
    done
    # when : a DBA configures PXF to use systemd
    sed -i.bak -e '/PXF_USE_SYSTEMD/c\export PXF_USE_SYSTEMD=true' "${PXF_BASE_DIR}/conf/pxf-env.sh"
    #      : AND runs "pxf cluster sync"
    pxf cluster sync
    #      : AND runs "pxf cluster start"
    local output
    local result
    output="$(pxf cluster start 2>&1)"
    result="$?"
    assert_equals "0" "${result}" "pxf cluster start should succeed"
    # then : it prints an error message
    assert_equals "$(echo -e ${expected_output})" "${output}" "pxf cluster start should succeed"
    for host in "${all_cluster_hosts[@]}"; do
        disable_systemd_user_service "${host}"
    done
}

run_test test_enabling_systemd_with_user_services "pxf cluster start should succeed"
# ===========================================================================================================

after_all() {
    pxf cluster stop
    rm -rf "${PXF_BASE}"
    unset PXF_BASE
}
after_all

exit_with_err "${BASH_SOURCE[0]}"
