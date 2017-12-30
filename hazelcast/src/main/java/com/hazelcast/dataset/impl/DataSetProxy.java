package com.hazelcast.dataset.impl;

import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.CompiledAggregation;
import com.hazelcast.dataset.CompiledPredicate;
import com.hazelcast.dataset.CompiledProjection;
import com.hazelcast.dataset.DataSet;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.dataset.impl.aggregation.CompileAggregationOperationFactory;
import com.hazelcast.dataset.impl.operations.SetOperation;
import com.hazelcast.dataset.impl.operations.SizeOperationFactory;
import com.hazelcast.dataset.impl.operations.UsedMemoryOperationFactory;
import com.hazelcast.dataset.impl.projection.CompileProjectionOperationFactory;
import com.hazelcast.dataset.impl.query.CompilePredicateOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.UuidUtil;

import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DataSetProxy<K, V> extends AbstractDistributedObject<DataSetService> implements DataSet<K, V> {

    private final String name;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;

    public DataSetProxy(String name, NodeEngine nodeEngine, DataSetService dataSetService) {
        super(nodeEngine, dataSetService);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
    }

    @Override
    public void set(K key, V value) {
        setAsync(key, value).join();
    }

    @Override
    public InternalCompletableFuture<Object> setAsync(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = toData(key);
        Data valueData = toData(value);

        Operation op = new SetOperation(name, keyData, valueData)
                .setPartitionId(partitionService.getPartitionId(key));

        return operationService.invokeOnPartition(op);
    }

    @Override
    public CompiledPredicate<V> compile(Predicate query) {
        checkNotNull(query, "query can't be null");

        String compileId = UuidUtil.newUnsecureUuidString().replace("-", "");

        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new CompilePredicateOperationFactory(name, compileId, query));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledPredicate<V>(operationService, name, compileId);
    }

    @Override
    public <E> CompiledProjection<E> compile(ProjectionRecipe<E> projectionRecipe) {
        checkNotNull(projectionRecipe, "projectionRecipe can't be null");

        String compileId = UuidUtil.newUnsecureUuidString().replace("-", "");

        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new CompileProjectionOperationFactory(name, compileId, projectionRecipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledProjection<E>(operationService, name, compileId);
    }

    @Override
    public <T, E> CompiledAggregation<E> compile(AggregationRecipe<T, E> aggregationRecipe) {
        checkNotNull(aggregationRecipe, "aggregationRecipe can't be null");

        String compileId = UuidUtil.newUnsecureUuidString().replace("-", "");

        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new CompileAggregationOperationFactory(name, compileId, aggregationRecipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledAggregation<E>(operationService, name, compileId);
    }

    @Override
    public long size() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new SizeOperationFactory(name));

            long size = 0;
            for (Object value : result.values()) {
                size += ((Long) value);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long memoryConsumption() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new UsedMemoryOperationFactory(name));

            long size = 0;
            for (Object value : result.values()) {
                size += ((Long) value);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getServiceName() {
        return com.hazelcast.dataset.impl.DataSetService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
