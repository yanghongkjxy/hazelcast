package com.hazelcast.simplemap.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.simplemap.CompiledPredicate;
import com.hazelcast.simplemap.SimpleMap;
import com.hazelcast.simplemap.impl.operations.CompilePredicateOperationFactory;
import com.hazelcast.simplemap.impl.operations.InsertOperation;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;

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
    public void insert(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = toData(key);
        Data valueData = toData(value);

        Operation op = new InsertOperation(name, keyData, valueData)
                .setPartitionId(partitionService.getPartitionId(key));

        operationService.invokeOnPartition(op).join();
    }

    @Override
    public CompiledPredicate<V> compile(Predicate query) {
        checkNotNull(query, "query can't be null");

        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    SimpleMapService.SERVICE_NAME, new CompilePredicateOperationFactory(name, query));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledPredicate<V>();
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
