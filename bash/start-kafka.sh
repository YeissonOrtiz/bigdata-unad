#!/bin/bash
echo "Starting Kafka server..."
cd ~/kafka/current
export KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
bin/kafka-server-start.sh config/server.properties 