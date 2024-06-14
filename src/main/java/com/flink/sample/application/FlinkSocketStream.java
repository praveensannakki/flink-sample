package com.flink.sample.application;

import com.flink.sample.converter.EmployeeParser;
import com.flink.sample.events.Employee;
import com.flink.sample.util.ConfigReader;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import lombok.extern.slf4j.Slf4j;

//@slf4j
public class FlinkSocketStream {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSocketStream.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Initializing socket stream");

        ParameterTool parameterTool = ParameterTool.fromMap(ConfigReader.readYaml(args[0]));
        LOG.info("Initialization complete with parameters: {}", parameterTool);
        System.out.println(parameterTool.toMap());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.enableCheckpointing(parameterTool.getLong("flink.checkpoint.interval"));

        DataStream<String> jsonStream = env.socketTextStream(parameterTool.get("flink.socket.server"),
                                            parameterTool.getInt("flink.socket.port"));

        DataStream<Employee> employeeStream = jsonStream.flatMap(new EmployeeParser()).name("employee mapping")
                .filter(employee -> employee != null).name("employee filter");

        employeeStream.print();

        env.execute("Socket Streaming");

    }
}
