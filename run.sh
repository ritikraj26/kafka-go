#!/bin/sh
set -e

cd "$(dirname "$0")"

# Seed pre-populated topic data
echo "Seeding topic data..."
go run cmd/seed/main.go

# Build and run the broker
echo "Starting broker..."
go run app/main.go "$@"
