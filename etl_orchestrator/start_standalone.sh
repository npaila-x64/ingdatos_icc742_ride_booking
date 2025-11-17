#!/bin/bash
# Start Airflow in standalone mode (simpler for development)
# This starts webserver + scheduler + triggerer in one process

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "${GREEN}Starting Airflow in standalone mode...${NC}"

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

echo -e "${YELLOW}Starting Airflow standalone...${NC}"
echo -e "This will start webserver, scheduler, and triggerer."
echo -e "Press Ctrl+C to stop.\n"

# Run airflow standalone - it will handle everything
airflow standalone
