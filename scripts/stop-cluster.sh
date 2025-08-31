#!/bin/bash

echo "Stopping Twitter Analytics Big Data Cluster..."

# Stop all services
docker compose down

echo "Cluster stopped successfully!"