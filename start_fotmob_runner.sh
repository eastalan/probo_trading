#!/bin/bash

# FotMob Runner Startup Script
# This script activates the venv_sw_test environment and starts the runner

echo "Starting FotMob Runner with venv_sw_test environment..."

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Change to the project directory
cd "$SCRIPT_DIR"

# Activate the virtual environment
source venv_sw_test/bin/activate

# Check if the virtual environment is activated
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "✅ Virtual environment activated: $VIRTUAL_ENV"
else
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

# Check if required dependencies are available
python -c "import selenium, webdriver_manager, requests" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "✅ All dependencies are available"
else
    echo "❌ Missing dependencies"
    exit 1
fi

# Start the runner
echo "🚀 Starting FotMob Runner..."
python fotmob_runner.py
