#!/bin/bash
# Start Airflow webserver and scheduler

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${GREEN}Starting Airflow services...${NC}"

# Activate virtual environment
if [ ! -d ".venv" ]; then
    echo -e "${YELLOW}Virtual environment not found. Please run ./setup_airflow.sh first.${NC}"
    exit 1
fi

source .venv/bin/activate

# Set AIRFLOW_HOME
export AIRFLOW_HOME="$SCRIPT_DIR"

# Load environment variables
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
fi

# Start webserver in background
echo -e "${YELLOW}Starting webserver on port 8080...${NC}"
airflow webserver -p 8080 > logs/webserver.log 2>&1 &
WEBSERVER_PID=$!
echo $WEBSERVER_PID > webserver.pid

# Start scheduler in background
echo -e "${YELLOW}Starting scheduler...${NC}"
airflow scheduler > logs/scheduler.log 2>&1 &
SCHEDULER_PID=$!
echo $SCHEDULER_PID > scheduler.pid

# Start triggerer (for deferrable operators)
echo -e "${YELLOW}Starting triggerer...${NC}"
airflow triggerer > logs/triggerer.log 2>&1 &
TRIGGERER_PID=$!
echo $TRIGGERER_PID > triggerer.pid

echo -e "\n${GREEN}Airflow is starting up...${NC}"
echo -e "Webserver PID: $WEBSERVER_PID"
echo -e "Scheduler PID: $SCHEDULER_PID"
echo -e "Triggerer PID: $TRIGGERER_PID"
echo -e "\nAccess the UI at: ${GREEN}http://localhost:8080${NC}"
echo -e "Username: ${GREEN}admin${NC}"
echo -e "Password: ${GREEN}admin${NC}"
echo -e "\nTo view logs:"
echo -e "  ${YELLOW}tail -f logs/webserver.log${NC}"
echo -e "  ${YELLOW}tail -f logs/scheduler.log${NC}"
echo -e "\nTo stop Airflow, run:"
echo -e "  ${YELLOW}./stop_airflow.sh${NC}"
