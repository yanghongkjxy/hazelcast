package com.hazelcast.dataset.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.dataset.impl.SimpleMapDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class CompilePredicateOperationFactory implements OperationFactory {

    private  String compiledQueryUuid;
    private String name;
    private Predicate predicate;

    public CompilePredicateOperationFactory() {
    }

    public CompilePredicateOperationFactory(String name,String compiledQueryUuid, Predicate predicate) {
        this.name = name;
        this.compiledQueryUuid = compiledQueryUuid;
        this.predicate = predicate;
    }

    @Override
    public Operation createOperation() {
        return new CompilePredicateOperation(name, compiledQueryUuid, predicate);
    }

    @Override
    public int getFactoryId() {
        return SimpleMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SimpleMapDataSerializerHook.COMPILE_PREDICATE_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(compiledQueryUuid);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        compiledQueryUuid = in.readUTF();
        predicate = in.readObject();
    }
}
