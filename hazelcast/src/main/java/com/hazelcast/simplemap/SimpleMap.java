package com.hazelcast.simplemap;


import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.query.Predicate;

public interface SimpleMap<K, V> {

    void insert(K key, V value);

    ICompletableFuture insertAsync(K key, V value);

    CompiledPredicate<V> compile(Predicate predicate);

    long size();
}
