package com.hazelcast.dataset.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class CompileProjectionOperationFactory implements OperationFactory {

    private  String compiledQueryUuid;
    private String name;
    private ProjectionRecipe projectionRecipe;

    public CompileProjectionOperationFactory() {
    }

    public CompileProjectionOperationFactory(String name,String compiledQueryUuid, ProjectionRecipe projectionRecipe) {
        this.name = name;
        this.compiledQueryUuid = compiledQueryUuid;
        this.projectionRecipe = projectionRecipe;
    }

    @Override
    public Operation createOperation() {
        return new CompileProjectionOperation(name, compiledQueryUuid, projectionRecipe);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.COMPILE_PROJECTION_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(compiledQueryUuid);
        out.writeObject(projectionRecipe);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        compiledQueryUuid = in.readUTF();
        projectionRecipe = in.readObject();
    }
}