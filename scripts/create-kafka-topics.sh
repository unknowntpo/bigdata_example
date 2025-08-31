#!/bin/bash

echo "Creating Kafka topics for Twitter Analytics..."

# Create topics
docker exec kafka kafka-topics --create --topic tweets --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic comments --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic retweets --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --create --topic celebrity-activity --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo "Kafka topics created successfully!"
echo ""
echo "Created topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092