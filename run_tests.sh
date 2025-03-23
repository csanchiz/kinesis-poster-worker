#!/bin/bash
# Script to run all unit tests for the kinesis-poster-worker application

echo "Running unit tests for kinesis-poster-worker..."
python3 -m unittest discover -s tests

# Check if tests passed
if [ $? -eq 0 ]; then
    echo "All tests passed successfully!"
    exit 0
else
    echo "Some tests failed. Please check the output above for details."
    exit 1
fi
