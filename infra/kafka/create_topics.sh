#!/usr/bin/env bash

set -e

KAFKA_CONTAINER_NAME="kafka"
BOOTSTRAP_SERVER="kafka:9092"

echo "Waiting for Kafka to be ready..."
sleep 10

create_topic () {
  local TOPIC_NAME=$1
  local PARTITIONS=$2
  local RETENTION_MS=$3

  echo "Creating topic: ${TOPIC_NAME}"

  docker exec ${KAFKA_CONTAINER_NAME} kafka-topics \
    --bootstrap-server ${BOOTSTRAP_SERVER} \
    --create \
    --if-not-exists \
    --topic ${TOPIC_NAME} \
    --partitions ${PARTITIONS} \
    --replication-factor 1 \
    --config retention.ms=${RETENTION_MS}
}

create_topic "user_events_raw" 3 604800000
create_topic "user_events_invalid" 1 1209600000
create_topic "user_sessions" 3 259200000
create_topic "user_metrics_minute" 1 259200000

echo "Kafka topics created successfully."
