#!/usr/bin/env bash

if [ -z "$KAFKA_HOME" ]; then
    echo "Please set KAFKA_HOME environment variable."
    exit 1
fi

# this command expects input to be entered from the command line. Use ctrl-d to exit.
# example data for a session:
# key1:kafka message number 1
# key2:1 2 3 3 4 5
# key3:hello world
# key1:repeat again

$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092 \
--topic source-topic \
--property "parse.key=true" \
--property "key.separator=:"