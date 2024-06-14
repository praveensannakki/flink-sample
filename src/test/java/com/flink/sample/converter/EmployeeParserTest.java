package com.flink.sample.converter;

import com.flink.sample.events.Employee;
import com.flink.sample.util.TestUtil;
import mockit.Injectable;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class EmployeeParserTest {
    @Injectable
    RuntimeContext runtimeContext;
    @Injectable ParameterTool parameterTool;

    @BeforeMethod
    public void setUp() {
        //parameterTool = ParameterTool.fromMap(ConfigReader.readYaml("src/test/resources/flink-application-test.yaml"));
        Map<String, String> map = Stream.of(new String[][]{
                {"flink.checkpoint.interval", "60000"},
                {"flink.socket.server", "localhost"},
                {"flink.socket.port", "8080"}
        }).collect(Collectors.toMap(data -> data[0], data -> data[1]));
        parameterTool = ParameterTool.fromMap(map);
    }

    @Test
    public void testEmployeeParserFlatMap() throws Exception {
        EmployeeParser employeeParser = new EmployeeParser();
        employeeParser.setRuntimeContext(runtimeContext);
        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setGlobalJobParameters(parameterTool);

        //OneInputStream Operator takes the input and output types as type parameters
        MockEnvironment environment = MockEnvironment.builder().setExecutionConfig(executionConfig).build();

        try(OneInputStreamOperatorTestHarness<String, Employee> testHarness = new OneInputStreamOperatorTestHarness<>(
                new StreamFlatMap<>(employeeParser),environment)) {
            testHarness.open();
            String inputJson = "{\"Company\":{\"Employees\":[{\"id\":1001,\"firstName\":\"Romin\",\"lastName\":\"Irani\",\"phone\":\"408-1234567\",\"email\":\"romin.k.irani@gmail.com\",\"salary\":10000},{\"id\":1002,\"firstName\":\"Neil\",\"lastName\":\"Irani\",\"phone\":\"408-1111111\",\"email\":\"neilrirani@gmail.com\",\"salary\":20000}]}}";
            testHarness.processElement(inputJson,10);
            Assert.assertNotNull(testHarness.extractOutputStreamRecords());
            Assert.assertEquals(testHarness.extractOutputStreamRecords().size(), 2);
            Assert.assertEquals(testHarness.extractOutputStreamRecords().get(0).getValue(), TestUtil.buildEmployee());

            //To test null or blank messages, parser won't emit record so size will be same
            testHarness.processElement(null,20);
            Assert.assertEquals(testHarness.extractOutputStreamRecords().size(), 2);

        }
    }

}
