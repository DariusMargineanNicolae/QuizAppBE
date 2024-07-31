#!/bin/bash

# Path to the virtual environment's activation script
VENV_PATH="/opt/venv/bin/activate"

# Path to your pylintrc file
PYLINTRC_PATH="./pylintrc"

# Default Python path
PYTHONPATH=${PWD}

# Check if the virtual environment exists
if [ ! -f "$VENV_PATH" ]; then
    echo "Virtual environment not found at /opt/venv."
    exit 1
fi

# Activate the virtual environment
. "$VENV_PATH"

# Check if pylint is available in the virtual environment
if ! command -v pylint &> /dev/null; then
    echo "pylint not found in the virtual environment."
    exit 1
fi

# Optionally, set PYTHONPATH if your project requires it
export PYTHONPATH="${PWD}/google_ocr:$PYTHONPATH"

# Run pylint with specified configurations and the pylintrc file
pylint_output=$(pylint --rcfile="$PYLINTRC_PATH" "$@" 2>&1)
exit_code=$?

# Exit code handling to determine the outcome
if [ $exit_code -ne 0 ]; then
    echo "Pylint found issues:"
    echo "$pylint_output"
    exit $exit_code  # Exit with pylint's exit code if it's non-zero
else
    echo "No Pylint issues found."
    exit 0
fi