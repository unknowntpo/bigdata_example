#!/bin/bash

echo "🚀 Starting Hadoop services for testing..."

# Start only Hadoop namenode and datanode (fast startup)
docker compose up namenode datanode -d

echo "⏳ Waiting for Hadoop services to be ready..."

# Wait for namenode to be accessible
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
    if curl -s http://localhost:9870 > /dev/null; then
        echo "✅ Hadoop namenode is ready!"
        break
    fi
    attempt=$((attempt + 1))
    echo "Waiting... ($attempt/$max_attempts)"
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ Timeout waiting for Hadoop to start"
    exit 1
fi

echo "🎯 Hadoop cluster is ready for testing!"
echo "   - Namenode UI: http://localhost:9870"
echo "   - HDFS endpoint: hdfs://localhost:9000"
echo ""
echo "Run tests with:"
echo "   ./gradlew test --tests DockerHadoopTest -Dhadoop.test=true"
echo "   ./gradlew test --tests DockerHadoopDataGeneratorTest -Dhadoop.test=true"
echo ""
echo "To stop Hadoop services:"
echo "   docker compose down"