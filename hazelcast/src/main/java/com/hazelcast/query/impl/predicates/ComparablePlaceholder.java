package com.hazelcast.query.impl.predicates;

public class ComparablePlaceholder implements Comparable {
    @Override
    public int compareTo(Object o) {
        throw new RuntimeException("Unsupported operation exception");
    }
}
