#!/bin/bash
# Stop Airflow services

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${YELLOW}Stopping Airflow services...${NC}"

# Function to stop a process
stop_process() {
    local name=$1
    local pidfile=$2
    
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile")
        if ps -p $pid > /dev/null 2>&1; then
            echo -e "Stopping $name (PID: $pid)..."
            kill $pid
            sleep 2
            # Force kill if still running
            if ps -p $pid > /dev/null 2>&1; then
                echo -e "${RED}Process still running, force killing...${NC}"
                kill -9 $pid
            fi
        else
            echo -e "$name was not running"
        fi
        rm -f "$pidfile"
    else
        echo -e "${YELLOW}$name PID file not found${NC}"
    fi
}

stop_process "Webserver" "webserver.pid"
stop_process "Scheduler" "scheduler.pid"
stop_process "Triggerer" "triggerer.pid"

# Also try to kill any remaining airflow processes
pkill -f "airflow webserver" || true
pkill -f "airflow scheduler" || true
pkill -f "airflow triggerer" || true

echo -e "\n${GREEN}Airflow services stopped${NC}"
