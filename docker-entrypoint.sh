#!/bin/bash

dask-scheduler &
service zookeeper start
sudo ./Kafka/kafka_2.11-0.11.0.2/bin/kafka-server-start.sh ./Kafka/kafka_2.11-0.11.0.2/config/server.properties &
sleep infinity
