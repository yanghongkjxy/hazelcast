package com.hazelcast.simplemap.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.simplemap.ProjectionInfo;
import com.hazelcast.simplemap.impl.SimpleMapDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class CompileProjectionOperationFactory implements OperationFactory {

    private  String compiledQueryUuid;
    private String name;
    private ProjectionInfo projectionInfo;

    public CompileProjectionOperationFactory() {
    }

    public CompileProjectionOperationFactory(String name,String compiledQueryUuid, ProjectionInfo projectionInfo) {
        this.name = name;
        this.compiledQueryUuid = compiledQueryUuid;
        this.projectionInfo = projectionInfo;
    }

    @Override
    public Operation createOperation() {
        return new CompileProjectionOperation(name, compiledQueryUuid, projectionInfo);
    }

    @Override
    public int getFactoryId() {
        return SimpleMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.COMPILE_PROJECTION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(compiledQueryUuid);
        out.writeObject(projectionInfo);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        compiledQueryUuid = in.readUTF();
        projectionInfo = in.readObject();
    }
}