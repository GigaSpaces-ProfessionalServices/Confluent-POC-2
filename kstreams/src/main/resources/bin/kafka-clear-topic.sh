#!/usr/bin/env bash

if [ -z "$KAFKA_HOME" ]; then
    echo "Please set KAFKA_HOME environment variable."
    exit 1
fi

$KAFKA_HOME/bin/kafka-delete-records.sh --bootstrap-server localhost:9092 --offset-json-file offsetfile.json
