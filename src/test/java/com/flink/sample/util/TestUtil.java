package com.flink.sample.util;

import com.flink.sample.events.Employee;

public class TestUtil {
    public static Employee buildEmployee() {
        return Employee.builder()
                .id(1001)
                .firstName("Romin")
                .lastName("Irani")
                .email("romin.k.irani@gmail.com")
                .phone("408-1234567")
                .salary(10000.0)
                .build();
    }
}
