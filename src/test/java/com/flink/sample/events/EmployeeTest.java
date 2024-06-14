package com.flink.sample.events;

import org.junit.Assert;
import org.junit.Test;

public class EmployeeTest {

    Employee employee = new Employee.EmployeeBuilder().build();
    @Test
    public void testEmployee() {
        Assert.assertNotNull(employee);
    }

    @Test
    public void testToString() {
        Employee employee = new Employee.EmployeeBuilder()
                .id(12345).firstName("john").lastName("doe").email("john@doe.com")
                .phone("123-456-7890").salary(2570)
                .build();
        Assert.assertNotNull(employee.toString());
        Assert.assertEquals(employee.toString(), "Employee(id=12345, firstName=john, " +
                "lastName=doe, email=john@doe.com, phone=123-456-7890, salary=2570.0)");

    }
}
