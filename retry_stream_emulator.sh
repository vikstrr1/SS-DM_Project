#!/bin/bash

# Retry loop for starting the stream emulator
until python /opt/flink/jobs/stream_emulator.py; do
  echo "Stream emulator crashed with exit code $?. Restarting..." >&2
  sleep 5  # Wait before restarting
done
