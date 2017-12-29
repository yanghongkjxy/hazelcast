package com.hazelcast.dataset;

import com.hazelcast.spi.OperationService;

import java.util.Map;

public class CompiledAggregation<E> {

    private final OperationService operationService;
    private final String compileId;
    private final String name;

    public CompiledAggregation(OperationService operationService, String name, String compileId) {
        this.operationService = operationService;
        this.name = name;
        this.compileId = compileId;
    }

    public E execute(Map<String, Object> bindings) {
        return null;
//        try {
//           Map<Integer,AggregationResult> result = (Map<Integer,E>)operationService.invokeOnAllPartitions(
//                    DataSetService.SERVICE_NAME, new QueryOperationFactory(name, compileId, bindings));
//            return null;
//
//
//            AggregationResult result = queryEngine.execute(query, Target.ALL_NODES);
//            return result.<R>getAggregator().aggregate();
//
//        } catch (Exception e) {
//            throw new RuntimeException();
//        }
    }
}
