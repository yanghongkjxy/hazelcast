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

public class SimpleMapTest {

    @Test
    public void test() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addSimpleMapConfig(new SimpleMapConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        SimpleMap<Long, Employee> simpleMap = hz.getSimpleMap("foo");
        for (int k = 0; k < 5; k++) {
            simpleMap.insert((long) k, new Employee(k, k, k));
        }
    }


    @Test
    public void compileQuery() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addSimpleMapConfig(new SimpleMapConfig("foo").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        SimpleMap<Long, Employee> simpleMap = hz.getSimpleMap("foo");
        for (int k = 0; k < 1000; k++) {
            simpleMap.insert((long) k, new Employee(k, k, k));
        }

        CompiledPredicate compiledPredicate = simpleMap.compile(new SqlPredicate("age==$age and iq==$iq and height>10"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 100);
        bindings.put("iq", 100l);
        compiledPredicate.execute(bindings);
    }

    public static class Employee implements Serializable {
        public int age;
        public long iq;
        public int height;
        public int money = 100;
        public int money2 = 200;

        public Employee(int age, int iq, int height) {
            this.age = age;
            this.iq = iq;
            this.height = height;
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "age=" + age +
                    ", iq=" + iq +
                    ", height=" + height +
                    ", money=" + money +
                    ", money2=" + money2 +
                    '}';
        }
    }
}
