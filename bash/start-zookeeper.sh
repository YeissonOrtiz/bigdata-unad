#!/bin/bash
echo "Starting ZooKeeper service..."
cd ~/kafka/current
export KAFKA_HEAP_OPTS="-Xmx512M -Xms512M"
bin/zookeeper-server-start.sh config/zookeeper.properties 