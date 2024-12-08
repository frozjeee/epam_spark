#!/bin/sh

# Load environment variables from .env file if it exists
if [ -f "/app/.env" ]; then
  echo "Loading environment variables from .env file"
  export $(grep -v '^#' /app/.env | xargs)
fi

# Execute the command passed to the container
exec "$@"