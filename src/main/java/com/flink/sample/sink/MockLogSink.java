package com.flink.sample.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock sink to print the logs
 */
public class MockLogSink extends RichSinkFunction<String> {
    private static final Logger LOG = LoggerFactory.getLogger(MockLogSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("Inside open method of MockLogSink...");
    }
    @Override
    public void invoke(String employee, Context context) throws Exception {
        LOG.info(employee);
    }
}
