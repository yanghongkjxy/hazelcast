package com.hazelcast.config;

public class SimpleMapConfig {
    private String name;
    private Class keyClass;
    private Class valueClass;
    private long sizeBytesPerPartition = 256 * 1024 * 1024;

    public SimpleMapConfig() {
    }

    public SimpleMapConfig(String name) {
        this.name = name;
    }

    public SimpleMapConfig(SimpleMapConfig defConfig) {
        this.name = defConfig.name;
        this.keyClass = defConfig.keyClass;
        this.valueClass = defConfig.valueClass;
        this.sizeBytesPerPartition = defConfig.sizeBytesPerPartition;
    }

    public long getSizeBytesPerPartition() {
        return sizeBytesPerPartition;
    }

    public SimpleMapConfig setSizeBytesPerPartition(long sizeBytesPerPartition) {
        this.sizeBytesPerPartition = sizeBytesPerPartition;
        return this;
    }

    public Class getKeyClass() {
        return keyClass;
    }

    public SimpleMapConfig setKeyClass(Class keyClass) {
        this.keyClass = keyClass;
        return this;
    }

    public Class getValueClass() {
        return valueClass;
    }

    public SimpleMapConfig setValueClass(Class valueClass) {
        this.valueClass = valueClass;
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public SimpleMapConfig getAsReadOnly() {
        return this;
    }
}
