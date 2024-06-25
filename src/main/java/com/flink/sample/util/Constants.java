package com.flink.sample.util;

import java.io.File;

public class Constants {
    public static final String CONFIG_FILE_LOCATION = System.getProperty("user.dir") + File.separator + "flink-application.yaml";
    public static final String KAFKA_PRODUCER_CONFIG_FILE_LOCATION = System.getProperty("user.dir") + File.separator + "kafka-producer.properties";
}
