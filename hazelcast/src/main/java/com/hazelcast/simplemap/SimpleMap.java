package com.hazelcast.simplemap;

public interface SimpleMap<K, V> {

    void insert(K key, V value);
}
