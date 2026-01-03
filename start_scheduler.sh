#!/bin/bash
# =============================================================================
# Follow The Goat - WSL Scheduler Startup Script
# =============================================================================
# 
# Usage (from WSL):
#   cd /mnt/c/0000websites/00phpsites/follow_the_goat
#   ./start_scheduler.sh
#
# Or from Windows PowerShell:
#   wsl -e bash /mnt/c/0000websites/00phpsites/follow_the_goat/start_scheduler.sh
#
# =============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Follow The Goat - Scheduler Startup  ${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Project path
PROJECT_DIR="/mnt/c/0000websites/00phpsites/follow_the_goat"
VENV_DIR="$HOME/follow_the_goat_venv"
DATA_DIR="$HOME/follow_the_goat_data"

# Ensure WSL data directory exists (for DuckDB files)
mkdir -p "$DATA_DIR"
echo -e "DuckDB data directory: ${GREEN}$DATA_DIR${NC}"

# Check if virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${RED}ERROR: Virtual environment not found at $VENV_DIR${NC}"
    echo "Create it with: python3 -m venv $VENV_DIR"
    exit 1
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source "$VENV_DIR/bin/activate"

# Navigate to project
cd "$PROJECT_DIR" || {
    echo -e "${RED}ERROR: Cannot access project directory $PROJECT_DIR${NC}"
    exit 1
}

# Get Windows host IP dynamically (WSL2 can reach Windows via this IP)
WINDOWS_IP=$(cat /etc/resolv.conf | grep nameserver | awk '{print $2}')
echo -e "Windows Host IP: ${GREEN}$WINDOWS_IP${NC}"

# Get WSL IP (for Windows/PHP to reach the Flask API)
WSL_IP=$(hostname -I | awk '{print $1}')
echo -e "WSL IP: ${GREEN}$WSL_IP${NC}"

# Set environment variables
export DB_HOST="$WINDOWS_IP"
export WSL_HOST_IP="$WSL_IP"
echo -e "DB_HOST set to: ${GREEN}$DB_HOST${NC}"
echo -e "WSL_HOST_IP set to: ${GREEN}$WSL_HOST_IP${NC}"

# Update the PHP config file with current WSL IP
CONFIG_FILE="$PROJECT_DIR/000website/includes/config.php"
if [ -f "$CONFIG_FILE" ]; then
    # Update the WSL_HOST_IP in the config file
    sed -i "s/define('WSL_HOST_IP', getenv('WSL_HOST_IP') ?: '[^']*');/define('WSL_HOST_IP', getenv('WSL_HOST_IP') ?: '$WSL_IP');/" "$CONFIG_FILE"
    echo -e "${GREEN}✓ Updated PHP config with WSL IP${NC}"
fi

# Test MySQL connectivity via Windows host IP
echo -e "${YELLOW}Testing MySQL connectivity...${NC}"
if timeout 3 bash -c "</dev/tcp/$WINDOWS_IP/3306" 2>/dev/null; then
    echo -e "${GREEN}✓ MySQL ($WINDOWS_IP) is reachable${NC}"
else
    echo -e "${RED}✗ MySQL ($WINDOWS_IP) is NOT reachable - scheduler may have issues${NC}"
fi

# Test Webhook connectivity
echo -e "${YELLOW}Testing Webhook connectivity...${NC}"
if curl -s --max-time 3 http://195.201.84.5/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Webhook is reachable${NC}"
else
    echo -e "${RED}✗ Webhook is NOT reachable - scheduler may have issues${NC}"
fi

echo ""
echo -e "${GREEN}Starting scheduler...${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo ""

# Start the scheduler
python scheduler/master.py

