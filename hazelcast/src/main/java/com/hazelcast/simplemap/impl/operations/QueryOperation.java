package com.hazelcast.simplemap.impl.operations;

import com.hazelcast.simplemap.impl.SimpleMapDataSerializerHook;

import java.util.Map;

public class QueryOperation extends SimpleMapOperation {

    private Map<String, Object> bindings;
    private String compiledQueryUuid;

    public QueryOperation() {
    }

    public QueryOperation(String name, String compiledQueryUuid, Map<String, Object> bindings) {
        super(name);
        this.compiledQueryUuid = compiledQueryUuid;
        this.bindings = bindings;
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.QUERY;
    }
}
