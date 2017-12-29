package com.hazelcast.dataset.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.dataset.impl.SimpleMapDataSerializerHook;
import com.hazelcast.dataset.impl.SimpleRecordStore;

import java.io.IOException;

public class SetOperation extends SimpleMapOperation {

    private Data key;
    private Data value;

    public SetOperation() {
    }

    public SetOperation(String name, Data key, Data value) {
        super(name);
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() throws Exception {
        SimpleRecordStore recordStore = container.getRecordStore(getPartitionId());
        recordStore.set(key, value);
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.SET;
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
