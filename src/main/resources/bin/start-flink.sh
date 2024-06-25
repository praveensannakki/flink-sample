echo "flink run-application -t yarn-application -DMainClass=com.flink.sample.application.FlinkSocketStream <path of jar>"
flink run --detached flink-streaming-0.0.1.jar

#https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/cli/
#to run with savepoint
echo "flink run-application -s ${RECOVER_PATH} -restoreMode CLAIM -t yarn-application -DMainClass=com.flink.sample.application.FlinkSocketStream <path of jar>"
flink run-application -s "${RECOVER_PATH}" -restoreMode CLAIM -t yarn-application -DMainClass=com.flink.sample.application.FlinkSocketStream /home/flink-streaming-0.0.1.jar