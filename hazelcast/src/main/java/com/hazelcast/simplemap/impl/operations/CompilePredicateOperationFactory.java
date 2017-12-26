package com.hazelcast.simplemap.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.simplemap.impl.SimpleMapDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class CompilePredicateOperationFactory implements OperationFactory {

    private String name;
    private Predicate predicate;

    public CompilePredicateOperationFactory() {
    }

    public CompilePredicateOperationFactory(String name, Predicate predicate) {
        this.name = name;
        this.predicate = predicate;
    }

    @Override
    public Operation createOperation() {
        return new CompilePredicateOperation(name, predicate);
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
        out.writeObject(predicate);
        out.writeUTF(name);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        predicate = in.readObject();
        name = in.readUTF();
    }
}
