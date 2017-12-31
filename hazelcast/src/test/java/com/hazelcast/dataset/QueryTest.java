package com.hazelcast.dataset;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class QueryTest extends HazelcastTestSupport {

    @Test
    public void compileQuery() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("foo");
        dataSet.set(1L, new Employee(20, 100, 200));
        dataSet.set(1L, new Employee(21, 101, 200));
        dataSet.set(1L, new Employee(22, 103, 200));
        dataSet.set(1L, new Employee(23, 100, 201));
        dataSet.set(1L, new Employee(24, 100, 202));
        dataSet.set(1L, new Employee(20, 100, 204));

        CompiledPredicate<Employee> compiledPredicate = dataSet.compile(new SqlPredicate("age==$age"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 20);
        assertEquals(2, compiledPredicate.execute(bindings).size());
    }

    @Test
    public void compileQueryAll() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("foo");
        dataSet.set(1L, new Employee(20, 100, 200));
        dataSet.set(1L, new Employee(21, 101, 200));
        dataSet.set(1L, new Employee(22, 103, 200));
        dataSet.set(1L, new Employee(23, 100, 201));
        dataSet.set(1L, new Employee(24, 100, 202));
        dataSet.set(1L, new Employee(20, 100, 204));

        CompiledPredicate<Employee> compiledPredicate = dataSet.compile(new SqlPredicate("true"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        assertEquals(6, compiledPredicate.execute(bindings).size());
    }
}
