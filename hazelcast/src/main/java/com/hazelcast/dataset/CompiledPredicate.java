package com.hazelcast.dataset;

import com.hazelcast.dataset.impl.DataSetService;
import com.hazelcast.dataset.impl.query.QueryOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Map;
import java.util.Set;

public class CompiledPredicate<V> {

    private final OperationService operationService;
    private final String compileId;
    private final String name;

    public CompiledPredicate(OperationService operationService, String name, String compileId) {
        this.operationService = operationService;
        this.name = name;
        this.compileId = compileId;
    }

    public Set<V> execute(Map<String, Object> bindings) {
        try {
            operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new QueryOperationFactory(name, compileId, bindings));
            return null;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
