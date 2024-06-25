package com.flink.sample.sink;

import com.flink.sample.events.Employee;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class EmployeeSerializer implements KafkaRecordSerializationSchema<Employee> {

    private static final Logger LOG = LoggerFactory.getLogger(EmployeeSerializer.class);
    private final String topic;

    public EmployeeSerializer(ParameterTool parameterTool) {
        this.topic = parameterTool.get("kafka.salary.topic");
        LOG.info("Inside EmployeeSerializer constructor with topic {}", topic);
    }

    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Employee employee, KafkaSinkContext kafkaSinkContext, Long contextTsMillis) {
        long startTime = System.currentTimeMillis();
        LOG.debug("Serializing employee {}, contextTsMillis {}", employee, contextTsMillis);
        try {
            byte [] serializedByteArray = employee.toString().getBytes();
            return new ProducerRecord<>(topic, null, serializedByteArray);
        } catch (Exception e) {
            LOG.error("Error serializing employee {}", employee, e);
            throw new RuntimeException(e);
        } finally {
            LOG.debug("Serializing employee {} took {} ms", employee, System.currentTimeMillis() - startTime);
        }
    }
}
