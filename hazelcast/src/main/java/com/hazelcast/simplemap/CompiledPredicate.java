package com.hazelcast.simplemap;

import com.hazelcast.simplemap.impl.SimpleMapService;
import com.hazelcast.simplemap.impl.operations.QueryOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Map;
import java.util.Set;

public class CompiledPredicate<V> {

    private final OperationService operationService;
    private final String compiledQueryUuid;
    private final String name;

    public CompiledPredicate(OperationService operationService, String name, String compiledQueryUuid) {
        this.operationService = operationService;
        this.name = name;
        this.compiledQueryUuid = compiledQueryUuid;
    }

    public Set<V> execute(Map<String, Object> bindings) {
        try {
             operationService.invokeOnAllPartitions(
                    SimpleMapService.SERVICE_NAME, new QueryOperationFactory(name, compiledQueryUuid, bindings));
             return null;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
