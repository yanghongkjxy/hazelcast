package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.DataSetStore;

public class SizeOperation extends DataStoreOperation {

    private long response;

    public SizeOperation() {
    }

    public SizeOperation(String name) {
        super(name);
    }

    @Override
    public void run() throws Exception {
        DataSetStore recordStore = container.getRecordStore(getPartitionId());
        response = recordStore.size();
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.SIZE;
    }
}
