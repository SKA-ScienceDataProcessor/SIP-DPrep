#!/usr/bin/env python

"""
A code to consume Queues data within a QA aggregator container.
"""
import os
import time as t

from confluent_kafka import Producer, Consumer, KafkaError
import pickle

__author__ = "Jamie Farnes"
__email__ = "jamie.farnes@oerc.ox.ac.uk"

# Start the zookeeper service:
os.system('service zookeeper start')

# Define settings for Confluent Kafka consumer:
settings = {
     'bootstrap.servers': 'scheduler:9092',
     'group.id': 'mygroup',
     'client.id': 'client-1',
     'enable.auto.commit': True,
     'session.timeout.ms': 6000,
     'fetch.message.max.bytes': 100000000,
     'receive.message.max.bytes': 1000000000,
     'default.topic.config': {'auto.offset.reset': 'smallest'}
}

# Consume QA data from queue (available in docker logs):
c = Consumer(settings)
c.subscribe(['qa'])
running = True
while running:
    ingest = c.poll(10.0)
    if ingest is None:
        print("No QA messages in queue to aggregate.")
        t.sleep(10)
    elif not ingest.error():
        print(pickle.loads(ingest.value()))
    elif ingest.error().code() != KafkaError._PARTITION_EOF:
        print(ingest.error())

c.close()
