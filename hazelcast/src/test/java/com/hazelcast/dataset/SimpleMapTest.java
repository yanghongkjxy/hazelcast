package com.hazelcast.dataset;

import com.hazelcast.config.Config;
import com.hazelcast.config.SimpleMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataset.AgeSalary;
import com.hazelcast.dataset.CompiledPredicate;
import com.hazelcast.dataset.CompiledProjection;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SimpleMapTest {

    @Test
    public void test() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addSimpleMapConfig(new SimpleMapConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        SimpleMap<Long, Employee> simpleMap = hz.getSimpleMap("foo");
        for (int k = 0; k < 5; k++) {
            simpleMap.set((long) k, new Employee(k, k, k));
        }
    }


    @Test
    public void compileQuery() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addSimpleMapConfig(new SimpleMapConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        SimpleMap<Long, Employee> simpleMap = hz1.getSimpleMap("foo");
        for (int k = 0; k < 1000; k++) {
            simpleMap.set((long) k, new Employee(k, k, k));
        }

        CompiledPredicate compiledPredicate = simpleMap.compile(new SqlPredicate("age==$age and iq==$iq and height>10"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 100);
        bindings.put("iq", 100l);
        compiledPredicate.execute(bindings);
    }


    @Test
    public void compileProjection() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addSimpleMapConfig(new SimpleMapConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        SimpleMap<Long, Employee> simpleMap = hz1.getSimpleMap("foo");
        for (int k = 0; k < 1000; k++) {
            simpleMap.set((long) k, new Employee(k, k, k));
        }

        CompiledProjection<Employee> compiledPredicate = simpleMap.compile(new ProjectionRecipe<Employee>(Employee.class, true, new SqlPredicate("age==$age and iq==$iq and height>10")));
//        Map<String, Object> bindings = new HashMap<String, Object>();
//        bindings.put("age", 100);
//        bindings.put("iq", 100l);
//        compiledPredicate.execute(bindings);
    }


    @Test
    public void compileProjectionAgeSalary() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addSimpleMapConfig(new SimpleMapConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        SimpleMap<Long, Employee> simpleMap = hz1.getSimpleMap("foo");
        for (int k = 0; k < 1000; k++) {
            simpleMap.set((long) k, new Employee(k, k, k));
        }

        CompiledProjection<AgeSalary> compiledPredicate = simpleMap.compile(new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("age==$age and iq==$iq and height>10")));
//        Map<String, Object> bindings = new HashMap<String, Object>();
//        bindings.put("age", 100);
//        bindings.put("iq", 100l);
//        compiledPredicate.execute(bindings);
    }

}
