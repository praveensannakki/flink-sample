package com.flink.sample.converter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flink.sample.events.Company;
import com.flink.sample.events.Employee;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EmployeeParser extends RichFlatMapFunction<String, Employee> {

    private static final Logger LOG = LoggerFactory.getLogger(EmployeeParser.class);
    private ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Open Employee Parser method with parameters: {}", parameters);
        //super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        /**
         * Initialize here like objects/api's/metrics
         */
        LOG.info("ParameterTool: {}", parameterTool);
        objectMapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String json, Collector<Employee> collector) {
        try {
            if (StringUtils.isNotBlank(json)) {
                CompanyWrapper companyWrapper = objectMapper.readValue(json, CompanyWrapper.class);
                Company company = companyWrapper.getCompany();
                if (company != null) {
                    //List<Employee> employees = company.getEmployees();
                    //employees.forEach(employee -> collector.collect(employee));
                    //employees.forEach(collector::collect);
                    company.getEmployees().forEach(collector::collect);
                } else {
                    System.out.println("No company found in JSON.");
                }
            }
        } catch (JsonProcessingException e) {
            System.out.println("Error parsing json: " + json + "\n" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // Helper class to wrap Company in JSON
    private static class CompanyWrapper {
        private Company Company;

        @JsonProperty("Company")
        public Company getCompany() {
            return Company;
        }

        @JsonProperty("Company")
        public void setCompany(Company company) {
            Company = company;
        }
    }
}
