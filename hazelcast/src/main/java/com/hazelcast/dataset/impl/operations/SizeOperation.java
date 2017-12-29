package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.SimpleMapDataSerializerHook;
import com.hazelcast.dataset.impl.SimpleRecordStore;

public class SizeOperation extends SimpleMapOperation {

    private long response;

    public SizeOperation() {
    }

    public SizeOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        SimpleRecordStore recordStore = container.getRecordStore(getPartitionId());
        response = recordStore.size();
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.SIZE;
    }
}
