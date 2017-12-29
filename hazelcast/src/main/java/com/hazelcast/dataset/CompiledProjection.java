package com.hazelcast.dataset;

import com.hazelcast.spi.OperationService;

import java.util.Map;
import java.util.Set;

public class CompiledProjection<E> {

    private final OperationService operationService;
    private final String compileId;
    private final String name;

    public CompiledProjection(OperationService operationService, String name, String compileId) {
        this.operationService = operationService;
        this.name = name;
        this.compileId = compileId;
    }

    public Set<E> execute(Map<String, Object> bindings) {
        try {
//            operationService.invokeOnAllPartitions(
//                    DataSetService.SERVICE_NAME, new (name, compileId, bindings));
            return null;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }
}
