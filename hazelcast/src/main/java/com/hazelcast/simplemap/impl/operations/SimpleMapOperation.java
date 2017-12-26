package com.hazelcast.simplemap.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.simplemap.impl.SimpleMapContainer;
import com.hazelcast.simplemap.impl.SimpleMapDataSerializerHook;
import com.hazelcast.simplemap.impl.SimpleMapService;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public abstract class SimpleMapOperation extends Operation
        implements PartitionAwareOperation, IdentifiedDataSerializable, NamedOperation {

    private String name;
    protected SimpleMapService simpleMapservice;
    protected SimpleMapContainer container;

    public SimpleMapOperation() {
    }

    public SimpleMapOperation(String name) {
        this.name = name;
    }

    @Override
    public void beforeRun() throws Exception {
        super.beforeRun();
        simpleMapservice = getService();
        container = simpleMapservice.getSimpleMapContainer(name);
    }

    @Override
    public String getServiceName() {
        return SimpleMapService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getFactoryId() {
        return SimpleMapDataSerializerHook.F_ID;
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
