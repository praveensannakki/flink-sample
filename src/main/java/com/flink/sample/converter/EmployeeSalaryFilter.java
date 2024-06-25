package com.flink.sample.converter;

import com.flink.sample.events.Employee;
import org.apache.flink.api.common.functions.RichMapFunction;

public class EmployeeSalaryFilter extends RichMapFunction<Employee, Employee> {
    @Override
    public Employee map(Employee employee) {
        //Filter the employees who is earning more than 20000
        if(employee != null && employee.getSalary() > 20000) {
            return employee;
        }

        return null;
    }
}
