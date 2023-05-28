#! /bin/bash

# check if the topic exist
kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -q 'rocket-launch'
if [$? -ne O]; then
    # create topic rocket-launch if it doesn't exist
    kafka-topics.sh --boostrap-server kafka:9092 --create --topic rocket-launch --partitions 1 --replication-factor 1
fi