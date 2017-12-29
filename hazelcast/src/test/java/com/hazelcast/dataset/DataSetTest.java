package com.hazelcast.dataset;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
import com.hazelcast.aggregation.impl.LongSumAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.aggregation.impl.CountAggregation;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

        CompiledProjection<AgeSalary> compiledProjection = dataSet.compile(new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("age==$age and iq==$iq and height>10")));
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
        long maxAge = Long.MAX_VALUE;
        Random random = new Random();
        for (int k = 0; k < 1000; k++) {
            int age = random.nextInt(100000);
            maxAge = Math.min(maxAge, age);
            dataSet.set((long) k, new Employee(age, k, k));
        }


        Aggregator aggregator = new MinAggregator();

        CompiledAggregation compiledAggregation = dataSet.compile(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
       // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        System.out.println("max inserted age:"+maxAge);
        System.out.println(compiledAggregation.execute(bindings));
    }

    @Test
    public void compileAggregationAverage() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz1.getDataSet("foo");
        double totalAge = 0;
        Random random = new Random();
        int count = 1000;
        for (int k = 0; k < count; k++) {
            int age = random.nextInt(100000);
            totalAge+=age;
            dataSet.set((long) k, new Employee(age, k, k));
        }


        Aggregator aggregator = new LongAverageAggregator();

        CompiledAggregation compiledAggregation = dataSet.compile(
                new AggregationRecipe<Long, Age>(Age.class, aggregator, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        // bindings.put("age", 200);
        //bindings.put("iq", 100l);
        System.out.println("average age:"+(totalAge/count));
        System.out.println(compiledAggregation.execute(bindings));
    }

}
