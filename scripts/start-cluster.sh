#!/bin/bash

echo "Starting Twitter Analytics Big Data Cluster..."

# Start all services
docker compose up -d

echo "Waiting for services to start..."
sleep 30

# Check service status
echo "Checking service status..."
docker compose ps

echo ""
echo "=== Cluster URLs ==="
echo "Hadoop NameNode Web UI: http://localhost:9870"
echo "Hadoop ResourceManager Web UI: http://localhost:8088"
echo "Hadoop HistoryServer Web UI: http://localhost:8188"
echo "Spark Master Web UI: http://localhost:8080"
echo "Spark Worker 1 Web UI: http://localhost:8081"
echo "Spark Worker 2 Web UI: http://localhost:8082"
echo "Hive Server: localhost:10000"
echo "Kafka: localhost:9092"
echo ""
echo "Cluster started successfully!"