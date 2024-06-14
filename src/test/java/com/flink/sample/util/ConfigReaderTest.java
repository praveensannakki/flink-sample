package com.flink.sample.util;

import org.junit.Assert;
import org.junit.Test;
import java.util.Map;

public class ConfigReaderTest {
    @Test
    public void readYamlFileTest() {
        Map<String, String> map = ConfigReader.readYaml("src/test/resources/flink-application-test.yaml");
        Assert.assertNotNull(map);
        Assert.assertEquals(3, map.size());
        Assert.assertEquals("localhost", map.get("flink.socket.server"));
        Assert.assertEquals("9999", map.get("flink.socket.port"));
        Assert.assertEquals("60000", map.get("flink.checkpoint.interval"));
    }

    @Test
    public void readYamlFileWhenEmptyFile() {
        Map<String, String> map = ConfigReader.readYaml("src/test/resources/flink-application-empty.yaml");
        Assert.assertNotNull(map);
        Assert.assertEquals(0, map.size());
    }

    @Test(expected = RuntimeException.class)
    public void readYamlFileWhenFileDoesNotExist() {
        ConfigReader.readYaml("src/test/resources/flink-application.yaml");
    }

    @Test(expected = RuntimeException.class)
    public void readYamlFileWhileExceptionIsThrown() {
        ConfigReader.readYaml("src/test/resources/flink-application-invalid.yaml");
    }
}
