#!/usr/bin/env bash
# Use this script to wait for a service to become available
# Usage: ./wait-for-it.sh <host:port>

hostport="$1"
shift

until nc -z ${hostport%%:*} ${hostport##*:}; do
  echo "Waiting for $hostport..."
  sleep 2
done

exec "$@"
