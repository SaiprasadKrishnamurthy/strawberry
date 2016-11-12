#!/usr/bin/env bash
./kafka-topics.sh --create --topic vehicle-camera-sensor-stream --replication-factor 1 --partitions 3 --zookeeper localhost:2181