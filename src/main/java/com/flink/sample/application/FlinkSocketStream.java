package com.flink.sample.application;

import com.flink.sample.converter.EmployeeParser;
import com.flink.sample.converter.EmployeeSalaryFilter;
import com.flink.sample.events.Employee;
import com.flink.sample.sink.EmployeeSerializer;
import com.flink.sample.sink.MockLogSink;
import com.flink.sample.util.ConfigReader;
import com.flink.sample.util.Constants;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
//import lombok.extern.slf4j.Slf4j;

//@slf4j
public class FlinkSocketStream {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSocketStream.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Initializing socket stream");

        ParameterTool parameterTool = ParameterTool.fromMap(ConfigReader.readYaml(Constants.CONFIG_FILE_LOCATION));
        LOG.info("Initialization complete with parameters: {}", parameterTool);
        System.out.println(parameterTool.toMap());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.enableCheckpointing(parameterTool.getLong("flink.checkpoint.interval"));

        DataStream<String> jsonStream = env.socketTextStream(parameterTool.get("flink.socket.server"),
                                            parameterTool.getInt("flink.socket.port"));

        DataStream<Employee> employeeStream = jsonStream.flatMap(new EmployeeParser()).name("employee mapping")
                .filter(employee -> employee != null).name("employee null filter"); //can use uid which will help in savepoints

        employeeStream.print(); // prints on the console

        employeeStream.map(Employee::toString).name("employee to string")
                .addSink(new MockLogSink()).name("mock employee log sink");

        //send employees who's earning is > 20000
        DataStream<Employee> employeeSalaryStream = employeeStream.map(new EmployeeSalaryFilter()).name("salary filter")
                        .filter(Objects::nonNull).name("employee null filter after salary");

        employeeSalaryStream.sinkTo(buildKafkaSink(parameterTool));

        env.execute("Socket Streaming");

    }

    private static Sink<Employee> buildKafkaSink(ParameterTool parameterTool) throws IOException {
        Properties properties = ParameterTool.fromPropertiesFile(Constants.KAFKA_PRODUCER_CONFIG_FILE_LOCATION).getProperties();

        LOG.info("Kafka producer config: {}", properties);

        return KafkaSink.<Employee>builder()
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(new EmployeeSerializer(parameterTool))
                .build();
    }
}
