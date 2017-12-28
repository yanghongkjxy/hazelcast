package com.hazelcast.simplemap;

import java.io.Serializable;

public class Employee implements Serializable {
    public int age;
    public long iq;
    public int height;
    public int money = 100;
    public int money2 = 200;

    public Employee(){}

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
