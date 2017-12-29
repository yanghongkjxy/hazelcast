package com.hazelcast.dataset.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.dataset.impl.DataSetDataSerializerHook;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QueryOperationFactory implements OperationFactory{

    private  String name;
    private String compiledQueryUuid;
    private Map<String,Object> bindings;

    public QueryOperationFactory(){}

    public QueryOperationFactory(String name, String compiledQueryUuid, Map<String, Object> bindings) {
        this.name = name;
        this.compiledQueryUuid = compiledQueryUuid;
        this.bindings = bindings;
    }

    @Override
    public Operation createOperation() {
        return new QueryOperation(name, compiledQueryUuid, bindings);
    }

    @Override
    public int getFactoryId() {
        return DataSetDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return DataSetDataSerializerHook.QUERY_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(compiledQueryUuid);
        out.writeInt(bindings.size());
        for(Map.Entry<String,Object> entry: bindings.entrySet()){
            out.writeUTF(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        compiledQueryUuid = in.readUTF();
        int size = in.readInt();
        bindings = new HashMap<String, Object>();
        for(int k=0;k<size;k++){
            bindings.put(in.readUTF(), in.readObject());
        }
    }
}
