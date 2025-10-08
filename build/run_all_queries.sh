#!/bin/bash

# This script runs the query_processor for all query plans in a specified directory.

# --- Configuration ---
# Set the path to the directory containing your query plan .json files.
PLANS_DIRECTORY="../plans"

# Set the path to your data directory.
DATA_DIRECTORY="../data/"

# Set the path to your executable.
EXECUTABLE="./query_processor"

# --- Script Body ---
# Check if the plans directory exists
if [ ! -d "$PLANS_DIRECTORY" ]; then
  echo "Error: Plans directory not found at '$PLANS_DIRECTORY'"
  exit 1
fi

# Check if the executable exists and is executable
if [ ! -x "$EXECUTABLE" ]; then
  echo "Error: Executable not found or not executable at '$EXECUTABLE'"
  exit 1
fi

# Loop through all files ending with .json in the plans directory
for plan_file in "$PLANS_DIRECTORY"/*.json; do
  # Check if the file exists to avoid errors if no .json files are found
  if [ -f "$plan_file" ]; then
    echo "----------------------------------------------------"
    echo "Executing with plan: $plan_file"
    echo "----------------------------------------------------"
    
    # Run the query processor with the current plan file and the data directory
    "$EXECUTABLE" "$plan_file" "$DATA_DIRECTORY"
    
    echo "" # Add a newline for better separation of output
  fi
done

echo "----------------------------------------------------"
echo "All query plans processed."
echo "----------------------------------------------------"
