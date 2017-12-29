package com.hazelcast.dataset;

import com.hazelcast.spi.OperationService;

import java.util.Map;
import java.util.Set;

public class CompiledProjection<E> {

    private final OperationService operationService;
    private final String compiledQueryUuid;
    private final String name;

    public CompiledProjection(OperationService operationService, String name, String compiledQueryUuid) {
        this.operationService = operationService;
        this.name = name;
        this.compiledQueryUuid = compiledQueryUuid;
    }

    public Set<E> execute(Map<String, Object> bindings) {
        try {
//            operationService.invokeOnAllPartitions(
//                    DataSetService.SERVICE_NAME, new (name, compiledQueryUuid, bindings));
            return null;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
