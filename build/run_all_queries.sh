#!/bin/bash

# This script runs the query_processor for all query plans in a specified directory,
# times the execution of each run, and prints a summary at the end.

# --- Configuration ---
PLANS_DIRECTORY="../plans"
DATA_DIRECTORY="../data/"
EXECUTABLE="./query_processor"

# --- Script Body ---
# Check for dependencies
if [ ! -d "$PLANS_DIRECTORY" ]; then
  echo "Error: Plans directory not found at '$PLANS_DIRECTORY'"
  exit 1
fi
if [ ! -x "$EXECUTABLE" ]; then
  echo "Error: Executable not found or not executable at '$EXECUTABLE'"
  exit 1
fi
if ! command -v /usr/bin/time &> /dev/null; then
    echo "Error: /usr/bin/time command not found. Please install it to measure execution time."
    exit 1
fi

# Array to store the results
declare -a results

# Temporary file to capture timing output
timing_file=$(mktemp)

# Ensure the temporary file is removed on exit
trap 'rm -f "$timing_file"' EXIT

# Loop through all files ending with .json in the plans directory
for plan_file in "$PLANS_DIRECTORY"/*.json; do
  if [ -f "$plan_file" ]; then
    plan_name=$(basename "$plan_file")
    echo "----------------------------------------------------"
    echo "Executing with plan: $plan_name"
    echo "----------------------------------------------------"
    
    # The time command's output goes to stderr. We use a subshell (...) and redirect
    # its stderr (2>) to capture the timing information in our temporary file.
    # The -p flag produces a standardized, portable output format that works on both Linux and macOS/BSD.
    ( /usr/bin/time -p "$EXECUTABLE" "$plan_file" "$DATA_DIRECTORY" ) 2> "$timing_file"
    
    # Parse the temp file to extract the 'real' (elapsed) time, which is the first line of the output.
    execution_time=$(grep 'real' "$timing_file" | awk '{print $2}')
    
    # Store the result in the format "plan_name: time"
    results+=("$plan_name: $execution_time")
    
    echo "" # Add a newline for better separation
  fi
done

# --- Print Final Summary ---
echo "===================================================="
echo "          Benchmark Execution Summary"
echo "===================================================="
printf "%-40s %s\n" "Query Plan" "Elapsed Time (real)"
echo "----------------------------------------------------"

for result in "${results[@]}"; do
  # Split the string "plan_name: time" into two parts
  plan_name_part="${result%:*}"
  time_part="${result#*: }"
  printf "%-40s %s\n" "$plan_name_part" "$time_part"
done

echo "===================================================="

