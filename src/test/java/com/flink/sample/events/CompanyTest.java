package com.flink.sample.events;

import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class CompanyTest {

    @Injectable
    Employee employee;
    @Test
    public void testConstructor() {
        Company company = Company.builder().employees(Collections.emptyList()).build();
        Assert.assertNotNull(company);
        Assert.assertEquals(0, company.getEmployees().size());
    }

    @Test
    public void testEmployees() {
        Company company = Company.builder().employees(Collections.singletonList(employee)).build();
        Assert.assertNotNull(company);
        Assert.assertEquals(1, company.getEmployees().size());
    }
}
