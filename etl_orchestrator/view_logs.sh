#!/bin/bash
# View Airflow logs in real-time

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

case "$1" in
    "webserver"|"w")
        tail -f logs/webserver.log
        ;;
    "scheduler"|"s")
        tail -f logs/scheduler.log
        ;;
    "triggerer"|"t")
        tail -f logs/triggerer.log
        ;;
    "all"|"")
        tail -f logs/*.log
        ;;
    *)
        echo "Usage: $0 [webserver|scheduler|triggerer|all]"
        echo "Shortcuts: w=webserver, s=scheduler, t=triggerer"
        exit 1
        ;;
esac
