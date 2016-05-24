#!/bin/bash

export ROOT=/Users/sogoyal/projects
export KAFKA_HOME=${ROOT}/external/kafka_2.10-0.10.0.0

bash ${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties &
bash ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties &



