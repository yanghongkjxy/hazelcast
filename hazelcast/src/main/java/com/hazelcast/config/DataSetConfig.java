package com.hazelcast.config;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;

public class DataSetConfig {
    private String name;
    private Class keyClass;
    private Class valueClass;
    private long sizeBytesPerPartition = 32 * 1024 * 1024;

    public DataSetConfig() {
    }

    public DataSetConfig(String name) {
        this.name = name;
    }

    public DataSetConfig(DataSetConfig defConfig) {
        this.name = defConfig.name;
        this.keyClass = defConfig.keyClass;
        this.valueClass = defConfig.valueClass;
        this.sizeBytesPerPartition = defConfig.sizeBytesPerPartition;
    }

    public long getSizeBytesPerPartition() {
        return sizeBytesPerPartition;
    }

    public DataSetConfig setSizeBytesPerPartition(long sizeBytesPerPartition) {
        this.sizeBytesPerPartition = checkPositive(sizeBytesPerPartition, "sizeBytesPerPartition should be larger than 0");
        return this;
    }

    public Class getKeyClass() {
        return keyClass;
    }

    public DataSetConfig setKeyClass(Class keyClass) {
        this.keyClass = checkNotNull(keyClass, "keyClass");
        return this;
    }

    public Class getValueClass() {
        return valueClass;
    }

    public DataSetConfig setValueClass(Class valueClass) {
        this.valueClass = checkNotNull(valueClass, "valueClass");
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public DataSetConfig getAsReadOnly() {
        return this;
    }
}
