#!/bin/bash
mkdir -p ${KAFKA_HOME}/console_logs/
nohup ${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_HOME}/config/zookeeper.properties > ${KAFKA_HOME}/console_logs/zookeeper.log 2>&1 &
sleep 5s
nohup ${KAFKA_HOME}/bin/kafka-server-start.sh ${KAFKA_HOME}/config/server.properties > ${KAFKA_HOME}/console_logs/server.log 2>&1 &