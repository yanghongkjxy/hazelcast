package com.hazelcast.dataset;


import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.query.Predicate;

/**
 * Todo:
 * - string fields
 * - enum fields
 * - primary index
 * - secondary index
 * - projections
 * - aggregations
 * - offloading
 * - regular predicates using the domain object
 * - entry processors
 * - in predicate
 * - partition predicate
 * - primitive wrappers (so nullable fields)
 *      - there could be a set of bytes added to each record where a bit is allocated per nullable field. So 8 nullable fields,
 *      can share 1 byte. 9 nullable fields, require 2 bytes.
 *
 * in progress
 * - between predicate
 *
 * done:
 * - char fields
 *
 * <h1>String</h1>
 * a string can be fixed max length. So if it is defined as 20 chars, then 20 chars storage is allocated. If only 5 chars are used,
 * the remaining 15 chars are zero'd.
 *
 * variable length strings:
 * These are very difficult to deal with in case of a fixed length record.
 *
 * String should also have option to indicate nullability.
 *
 * <h1>More complex mappings.</h1>
 * - mapping of string; the actual byte content could be written directly to offheap. But it requires a fixed size mapping.
 * - mapping of enums
 * - mappings of nullable primitive wrappers
 *
 * So instead of doing an actual mem-copy; write the fields one by one. And each field
 * knows what it should do. E.g. in case of a primitive wrapper, either the null bit is set, or the null bit is unset and the
 * value is set.
 *
 * Perhaps a code generator for record insertion? It can optimize for the case where there is a simple memcopy.
 *
 *
 * @param <K>
 * @param <V>
 */
public interface DataSet<K, V> {

    void set(K key, V value);

    ICompletableFuture setAsync(K key, V value);

    CompiledPredicate<V> compile(Predicate predicate);

    <E> CompiledProjection<E> compile(ProjectionRecipe<E> projectionRecipe);

    <T,E> CompiledAggregation<E> compile(AggregationRecipe<T,E> aggregationRecipe);

    long size();

    long memoryConsumption();
}
