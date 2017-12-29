package com.hazelcast.dataset.impl.operations;

import com.hazelcast.dataset.impl.DataSetContainer;
import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.dataset.impl.DataSetService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class DataStoreOperation extends Operation
        implements IdentifiedDataSerializable, NamedOperation {

    private String name;
    protected DataSetService dataSetService;
    protected DataSetContainer container;

    public DataStoreOperation() {
    }

    public DataStoreOperation(String name) {
        this.name = name;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        dataSetService = getService();
        container = dataSetService.getDataSetContainer(name);
    }

    @Override
    public String getServiceName() {
        return DataSetService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }
}
