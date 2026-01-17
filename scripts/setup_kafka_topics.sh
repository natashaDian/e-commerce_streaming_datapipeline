#!/bin/bash
# setup_kafka_topics.sh
# Creates all required Kafka topics with proper configuration

set -e

echo "========================================"
echo "Setting up Kafka Topics"
echo "========================================"

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
BOOTSTRAP_SERVER="localhost:29092"
REPLICATION_FACTOR=1
PARTITIONS=3
RETENTION_MS=604800000  # 7 days
COMPRESSION_TYPE="gzip"

# Wait for Kafka to be ready
echo "${YELLOW}Waiting for Kafka to be ready...${NC}"
sleep 10

# Function to create topic
create_topic() {
    local topic_name=$1
    
    echo "${YELLOW}Creating topic: ${topic_name}${NC}"
    
    docker exec kafka kafka-topics --create \
        --bootstrap-server ${BOOTSTRAP_SERVER} \
        --replication-factor ${REPLICATION_FACTOR} \
        --partitions ${PARTITIONS} \
        --topic ${topic_name} \
        --config retention.ms=${RETENTION_MS} \
        --config compression.type=${COMPRESSION_TYPE} \
        --config cleanup.policy=delete \
        --if-not-exists
    
    if [ $? -eq 0 ]; then
        echo "${GREEN}✓ Topic ${topic_name} created successfully${NC}"
    else
        echo "Failed to create topic: ${topic_name}"
        return 1
    fi
}

# Create topics
create_topic "olist.orders"
create_topic "olist.order_items"
create_topic "olist.payments"

# List all topics
echo ""
echo "${YELLOW}Current Kafka topics:${NC}"
docker exec kafka kafka-topics --list --bootstrap-server ${BOOTSTRAP_SERVER}

# Describe topics
echo ""
echo "${YELLOW}Topic configurations:${NC}"
docker exec kafka kafka-topics --describe --bootstrap-server ${BOOTSTRAP_SERVER}

echo ""
echo "${GREEN}========================================"
echo "Kafka topics setup complete!"
echo "========================================${NC}"