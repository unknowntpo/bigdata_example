#!/bin/bash

echo "=== Twitter Analytics Big Data Setup ==="
echo ""

# Make scripts executable
chmod +x scripts/*.sh

# Start the cluster
echo "Step 1: Starting cluster..."
./scripts/start-cluster.sh

echo ""
echo "Step 2: Waiting for cluster to be fully ready..."
sleep 60

# Initialize HDFS
echo ""
echo "Step 3: Initializing HDFS directories..."
./scripts/init-hdfs.sh

# Create Kafka topics
echo ""
echo "Step 4: Creating Kafka topics..."
./scripts/create-kafka-topics.sh

echo ""
echo "=== Setup Complete! ==="
echo ""
echo "Your Twitter Analytics cluster is ready!"
echo ""
echo "Next steps:"
echo "1. Update your Java application dependencies"
echo "2. Create data models (Tweet, Comment, Retweet POJOs)"
echo "3. Implement data generator service"
echo "4. Build Spark analytics jobs"
echo ""
echo "Access the web UIs:"
echo "  - Hadoop NameNode: http://localhost:9870"
echo "  - Spark Master: http://localhost:8080"
echo "  - ResourceManager: http://localhost:8088"