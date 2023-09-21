#!/bin/bash
DIR=$(dirname "${BASH_SOURCE[0]}") 
cd $DIR
source ./common.sh

waitPort 2181
waitPort 9092
cd target
cd kafka_2.11-2.3.1
echo "Deleting my_test_topic if it exists"
bin/kafka-topics.sh --zookeeper localhost:2181 --topic my_test_topic --delete
bin/kafka-server-stop.sh
sleep 2
bin/zookeeper-server-stop.sh

echo "Stopped Kafka and Zookeeper servers"