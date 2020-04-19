#!/bin/bash

kafka-topics.sh kafka-topics --zookeeper localhost:2181 --create --topic source-topic --if-not-exists  --partitions 1 --replication-factor 1

kafka-topics.sh  --zookeeper localhost:2181 --create --topic target-topic --if-not-exists  --partitions 1 --replication-factor 1

kafka-topics.sh --bootstrap-server localhost:9092 --list
