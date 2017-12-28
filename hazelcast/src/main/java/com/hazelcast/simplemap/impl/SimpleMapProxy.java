package com.hazelcast.simplemap.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.simplemap.CompiledPredicate;
import com.hazelcast.simplemap.CompiledProjection;
import com.hazelcast.simplemap.ProjectionInfo;
import com.hazelcast.simplemap.SimpleMap;
import com.hazelcast.simplemap.impl.operations.CompilePredicateOperationFactory;
import com.hazelcast.simplemap.impl.operations.CompileProjectionOperationFactory;
import com.hazelcast.simplemap.impl.operations.SetOperation;
import com.hazelcast.simplemap.impl.operations.SizeOperationFactory;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.UuidUtil;

import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class SimpleMapProxy<K, V> extends AbstractDistributedObject<SimpleMapService> implements SimpleMap<K, V> {

    private final String name;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;

    public SimpleMapProxy(String name, NodeEngine nodeEngine, SimpleMapService simpleMapService) {
        super(nodeEngine, simpleMapService);
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

        String compiledQueryUuid = UuidUtil.newUnsecureUuidString().replace("-", "");

        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    SimpleMapService.SERVICE_NAME, new CompilePredicateOperationFactory(name, compiledQueryUuid, query));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledPredicate<V>(operationService, name, compiledQueryUuid);
    }

    @Override
    public <E> CompiledProjection<E> compile(ProjectionInfo<E> projectionInfo) {
        checkNotNull(projectionInfo, "projectionInfo can't be null");

        String compiledQueryUuid = UuidUtil.newUnsecureUuidString().replace("-", "");

        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    SimpleMapService.SERVICE_NAME, new CompileProjectionOperationFactory(name, compiledQueryUuid, projectionInfo));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledProjection<E>(operationService, name, compiledQueryUuid);
    }

    @Override
    public long size() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    SimpleMapService.SERVICE_NAME, new SizeOperationFactory(name));

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
        return SimpleMapService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
