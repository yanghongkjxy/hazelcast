package com.hazelcast.simplemap;


import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.query.Predicate;

public interface SimpleMap<K, V> {

    void set(K key, V value);

    ICompletableFuture setAsync(K key, V value);

    CompiledPredicate<V> compile(Predicate predicate);



    long size();
}
