package com.hazelcast.simplemap;

import com.hazelcast.config.Config;
import com.hazelcast.config.SimpleMapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import org.junit.Test;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SimpleMapTest2 {

    @Test
    public void test() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addSimpleMapConfig(new SimpleMapConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        SimpleMap<Long, Employee> simpleMap = hz.getSimpleMap("foo");
        for (int k = 0; k < 5; k++) {
            simpleMap.set((long) k, new Employee(k, "Employee",1));
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
            simpleMap.set((long) k, new Employee(k, "employee" + k,1));
        }

        CompiledPredicate compiledPredicate = simpleMap.compile(new SqlPredicate("age==$age and iq==$iq and height>10"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 100);
        bindings.put("iq", 100l);
        compiledPredicate.execute(bindings);
    }

    public static class Employee implements Serializable {
        @Nullable
        @StringLength(length = 20)
        private final String name;
        public Integer age;
        public int salary;

        public Employee(Integer age, String name, int salary) {
            this.age = age;
            this.name = name;
            this.salary = salary;
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "age=" + age +
                    ", name=" + name +
                    '}';
        }
    }
}
