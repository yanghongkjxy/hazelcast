package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.DataSetStore;

public class UsedMemoryOperation extends DataSetOperation {

    private long response;

    public UsedMemoryOperation() {
    }

    public UsedMemoryOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        DataSetStore recordStore = container.getRecordStore(getPartitionId());
        response = recordStore.usedMemory();
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.USED_MEMORY_OPERATION;
    }
}