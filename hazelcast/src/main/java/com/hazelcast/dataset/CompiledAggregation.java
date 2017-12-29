package com.hazelcast.dataset;

import com.hazelcast.dataset.impl.operations.QueryOperationFactory;
import com.hazelcast.spi.OperationService;

import java.util.Map;

public class CompiledAggregation<E> {

    private final OperationService operationService;
    private final String compiledQueryUuid;
    private final String name;

    public CompiledAggregation(OperationService operationService, String name, String compiledQueryUuid) {
        this.operationService = operationService;
        this.name = name;
        this.compiledQueryUuid = compiledQueryUuid;
    }

    public E execute(Map<String, Object> bindings) {
        try {
           Map<Integer,E> result = (Map<Integer,E>)operationService.invokeOnAllPartitions(
                    com.hazelcast.dataset.impl.DataSetService.SERVICE_NAME, new QueryOperationFactory(name, compiledQueryUuid, bindings));
            return null;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
