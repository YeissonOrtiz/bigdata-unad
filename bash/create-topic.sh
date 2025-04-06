#!/bin/bash
echo "Creating Kafka topic 'election-tweets'..."
cd ~/kafka/current
bin/kafka-topics.sh --create --topic election-tweets --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 