#!/bin/bash

echo "Initializing HDFS directories for Twitter Analytics..."

# Create necessary directories in HDFS
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -mkdir -p /tweets
docker exec namenode hdfs dfs -mkdir -p /tweets/raw
docker exec namenode hdfs dfs -mkdir -p /tweets/processed
docker exec namenode hdfs dfs -mkdir -p /spark-logs
docker exec namenode hdfs dfs -mkdir -p /user/spark

# Set permissions
docker exec namenode hdfs dfs -chmod 777 /user/hive/warehouse
docker exec namenode hdfs dfs -chmod 777 /tweets
docker exec namenode hdfs dfs -chmod 777 /tweets/raw
docker exec namenode hdfs dfs -chmod 777 /tweets/processed
docker exec namenode hdfs dfs -chmod 777 /spark-logs
docker exec namenode hdfs dfs -chmod 777 /user/spark

echo "HDFS directories initialized successfully!"
echo ""
echo "Created directories:"
echo "  /user/hive/warehouse - Hive data warehouse"
echo "  /tweets/raw - Raw tweet data"
echo "  /tweets/processed - Processed tweet data"
echo "  /spark-logs - Spark application logs"
echo "  /user/spark - Spark user directory"