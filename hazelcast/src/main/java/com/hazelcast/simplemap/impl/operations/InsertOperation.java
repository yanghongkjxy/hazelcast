package com.hazelcast.simplemap.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.simplemap.impl.SimpleMapDataSerializerHook;
import com.hazelcast.simplemap.impl.SimpleRecordStore;

import java.io.IOException;

public class InsertOperation extends SimpleMapOperation {

    private Data key;
    private Data value;

    public InsertOperation() {
    }

    public InsertOperation(String name, Data key, Data value) {
        super(name);
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        SimpleRecordStore recordStore = container.getRecordStore(getPartitionId());
        recordStore.insert(key, value);
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.INSERT;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(key);
        out.writeData(value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = in.readData();
        value = in.readData();
    }
}
