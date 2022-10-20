#!/bin/bash

function log() {
    echo "=====> $(date) $* <======" >> "${PXF_LOGDIR}/pxf-oom.log"
}

function isRunning() {
    ps -o pid,command -p "$1" | grep 'pxf-app' &>/dev/null
}

function _main() {
    local PID="$1"
    if [[ -z $PID ]]; then
        log 'No PID provided for JVM; exiting...'
        exit 0
    fi

    sleep 1
    log 'Stopping PXF'
    kill "${PID}" &>/dev/null
    for i in $(seq 30); do
        if ! isRunning "${PID}"; then
            log "PXF stopped [${PID}]"
            exit 0
        elif ((i == STOP_WAIT_TIME / 2)); then
            kill "$PID" &>/dev/null
        fi
        sleep 1
    done

    kill -9 "$PID" &>/dev/null
    for i in $(seq 30); do
        if ! isRunning "${PID}"; then
            log "PXF stopped [${PID}]"
            exit 0
        elif ((i == STOP_WAIT_TIME / 2)); then
            kill -9 "$PID" &>/dev/null
        fi
        sleep 1
    done
}

log 'PXF Out of memory detected'
_main "$1" &
log 'PXF shutdown scheduled'
