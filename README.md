# Flink-sample

This repository has some sample flink codes
1. FlinkSocketStream
    - Code to consume stream of sample company json messages from socket and parse and emit/print data for individual employee details
2. SocketStreamWindowWordCount
    - Code to consume messages from socket and count the words, standard word count program

# How to Run
- clone the repository 
- mvn clean install
- run locally on any IDE or run on standalone flink cluster using docker image https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/