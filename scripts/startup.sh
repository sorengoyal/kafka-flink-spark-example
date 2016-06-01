#!/bin/bash

#Zookeeper
export ROOT=/Users/sogoyal/projects
export KAFKA_HOME=${ROOT}/external/kafka_2.10-0.10.0.0
${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties

#Kafka Server
export ROOT=/Users/sogoyal/projects
export KAFKA_HOME=${ROOT}/external/kafka_2.10-0.10.0.0
${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties

#Stop Zookeeper and Kafka
export ROOT=/Users/sogoyal/projects
export KAFKA_HOME=${ROOT}/external/kafka_2.10-0.10.0.0
${KAFKA_HOME}/bin/kafka-server-stop.sh
${KAFKA_HOME}/bin/zookeeper-server-stop.sh


#Kafka Topics
export ROOT=/Users/sogoyal/projects
export KAFKA_HOME=${ROOT}/external/kafka_2.10-0.10.0.0
${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic impressions
${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic clicks

#Consumer
export ROOT=/Users/sogoyal/projects
export KAFKA_HOME=${ROOT}/external/kafka_2.10-0.10.0.0
${KAFKA_HOME}/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic impressions --from-beginning

#Producer
export ROOT=/Users/sogoyal/projects
export KAFKA_HOME=${ROOT}/external/kafka_2.10-0.10.0.0
${KAFKA_HOME}/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic impressions
