package com.hazelcast.dataset;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.LongSumAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.aggregation.impl.CountAggregation;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DataSetTest {

    @Test
    public void test() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("foo");
        for (int k = 0; k < 5; k++) {
            dataSet.set((long) k, new Employee(k, k, k));
        }
    }


    @Test
    public void compileQuery() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz1.getDataSet("foo");
        for (int k = 0; k < 1000; k++) {
            dataSet.set((long) k, new Employee(k, k, k));
        }

        CompiledPredicate compiledPredicate = dataSet.compile(new SqlPredicate("age==$age and iq==$iq and height>10"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 100);
        bindings.put("iq", 100l);
        compiledPredicate.execute(bindings);
    }


    @Test
    public void compileProjection() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz1.getDataSet("foo");
        for (int k = 0; k < 1000; k++) {
            dataSet.set((long) k, new Employee(k, k, k));
        }

        CompiledProjection<Employee> compiledPredicate = dataSet.compile(new ProjectionRecipe<Employee>(Employee.class, true, new SqlPredicate("age==$age and iq==$iq and height>10")));
//        Map<String, Object> bindings = new HashMap<String, Object>();
//        bindings.put("age", 100);
//        bindings.put("iq", 100l);
//        compiledPredicate.execute(bindings);
    }


    @Test
    public void compileProjectionAgeSalary() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz1.getDataSet("foo");
        for (int k = 0; k < 1000; k++) {
            dataSet.set((long) k, new Employee(k, k, k));
        }

        CompiledProjection<AgeSalary> compiledPredicate = dataSet.compile(new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("age==$age and iq==$iq and height>10")));
//        Map<String, Object> bindings = new HashMap<String, Object>();
//        bindings.put("age", 100);
//        bindings.put("iq", 100l);
//        compiledPredicate.execute(bindings);
    }


    @Test
    public void compileAggregation() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz1.getDataSet("foo");
        for (int k = 0; k < 1000; k++) {
            dataSet.set((long) k, new Employee(k, k, k));
        }

        Aggregator sumAggregator = new MaxAggregator();

        CompiledAggregation compiledPredicate = dataSet.compile(
                new AggregationRecipe<Object, AgeSalary>(Age.class, sumAggregator, new SqlPredicate("age==$age and iq==$iq and height>10")));
//        Map<String, Object> bindings = new HashMap<String, Object>();
//        bindings.put("age", 100);
//        bindings.put("iq", 100l);
//        compiledPredicate.execute(bindings);
    }

}
