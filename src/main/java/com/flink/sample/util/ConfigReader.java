package com.flink.sample.util;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.LoaderOptions;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigReader {
    public static Map<String, String> readYaml(String path) {
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        Map<String, Object> map;
        try(InputStream inputStream = new FileInputStream(path)) {
            map = yaml.load(inputStream);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("File not found: " + path, e);
        } catch (IOException e) {
            throw new RuntimeException("Error reading file: " + path, e);
        }
        if(map == null) {
            return Collections.emptyMap();
        }
        return map.entrySet().stream().collect(
                Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())));
    }
}
