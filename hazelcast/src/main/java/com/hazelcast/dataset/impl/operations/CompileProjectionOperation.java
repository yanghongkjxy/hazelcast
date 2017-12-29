package com.hazelcast.dataset.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.dataset.ProjectionRecipe;

import java.io.IOException;

import static com.hazelcast.dataset.impl.SimpleMapDataSerializerHook.COMPILE_PROJECTION;

public class CompileProjectionOperation extends SimpleMapOperation {

    public ProjectionRecipe projectionRecipe;
    private String compiledQueryUuid;

    public CompileProjectionOperation() {
    }

    public CompileProjectionOperation(String name, String compiledQueryUuid, ProjectionRecipe projectionRecipe) {
        super(name);
        this.projectionRecipe = projectionRecipe;
        this.compiledQueryUuid = compiledQueryUuid;
    }

    @Override
    public int getId() {
        return COMPILE_PROJECTION;
    }

    @Override
    public void run() throws Exception {
        container.getRecordStore(getPartitionId()).compileProjection(compiledQueryUuid, projectionRecipe);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(compiledQueryUuid);
        out.writeObject(projectionRecipe);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        compiledQueryUuid = in.readUTF();
        projectionRecipe = in.readObject();
    }
}
