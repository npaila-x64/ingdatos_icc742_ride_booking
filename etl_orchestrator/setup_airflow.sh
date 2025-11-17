#!/bin/bash
# Airflow Local Development Setup Script
# Based on Apache Airflow 3.1.3 installation guidelines

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Airflow 3.1.3 Development Setup${NC}"
echo -e "${GREEN}========================================${NC}"

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo -e "\n${YELLOW}Step 1: Creating Python virtual environment...${NC}"
if [ -d ".venv" ]; then
    echo "Virtual environment already exists. Removing and recreating..."
    rm -rf .venv
fi

python3 -m venv .venv
source .venv/bin/activate

echo -e "\n${YELLOW}Step 2: Upgrading pip...${NC}"
pip install --upgrade pip

echo -e "\n${YELLOW}Step 3: Installing Apache Airflow 3.1.3...${NC}"
# Use official constraints file for stable installation
AIRFLOW_VERSION=3.1.3
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Using constraints: $CONSTRAINT_URL"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

echo -e "\n${YELLOW}Step 4: Installing project dependencies...${NC}"
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "No requirements.txt found, skipping..."
fi

echo -e "\n${YELLOW}Step 5: Setting up Airflow environment...${NC}"
export AIRFLOW_HOME="$SCRIPT_DIR"

# Load environment variables from .env
if [ -f ".env" ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment variables from .env"
fi

echo -e "\n${YELLOW}Step 6: Initializing Airflow database...${NC}"
airflow db migrate

echo -e "\n${YELLOW}Step 7: Creating admin user (will be done on first standalone run)...${NC}"
echo "Note: In Airflow 3.x, user creation is handled differently."
echo "The standalone command will create a default admin user automatically."

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\nTo start Airflow, run:"
echo -e "  ${YELLOW}cd $SCRIPT_DIR${NC}"
echo -e "  ${YELLOW}./start_airflow.sh${NC}"
echo -e "\nOr manually:"
echo -e "  ${YELLOW}source .venv/bin/activate${NC}"
echo -e "  ${YELLOW}export AIRFLOW_HOME=$SCRIPT_DIR${NC}"
echo -e "  ${YELLOW}airflow webserver -p 8080 &${NC}"
echo -e "  ${YELLOW}airflow scheduler &${NC}"
echo -e "\nAccess the UI at: ${GREEN}http://localhost:8080${NC}"
echo -e "Username: ${GREEN}admin${NC}"
echo -e "Password: ${GREEN}admin${NC}"
