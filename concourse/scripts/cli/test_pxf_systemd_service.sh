#!/usr/bin/env bash

dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
[[ -e ${dir}/common.sh ]] || exit 1
# shellcheck source=./common.sh
source "${dir}/common.sh"

# ************************************************************************************************************
# ***** TEST Suite starts with PXF not running on any nodes **************************************************
# ************************************************************************************************************

# === Test "pxf cluster start (without enabling systemd user services)" =====================================
expected_output=\
"The systemd user service for user '$(id -un)' (id=$(id -u)) is not active"
test_enabling_systemd_without_user_services() {
    # given:
    #      : PXF is not running
    pxf cluster stop
    # when : a DBA configures PXF to use systemd
    sed -e '/PXF_USE_SYSTEMD/c\export PXF_USE_SYSTEMD=true' "${PXF_BASE_DIR}/conf/pxf-env.sh"
    #      : AND runs "pxf cluster sync"
    pxf cluster sync
    #      : AND runs "pxf cluster start"
    local result
    result="$(pxf cluster start)"
    # then : it prints an error message
    assert_equals "${expected_output}" "${result}" "pxf cluster start should not succeed"
}

run_test test_enabling_systemd_without_user_services "pxf cluster start should not succeed"
# ===========================================================================================================

exit_with_err "${BASH_SOURCE[0]}"
