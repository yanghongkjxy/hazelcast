package com.hazelcast.simplemap.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.simplemap.ProjectionInfo;

import java.io.IOException;

import static com.hazelcast.simplemap.impl.SimpleMapDataSerializerHook.COMPILE_PROJECTION;

public class CompileProjectionOperation extends SimpleMapOperation {

    public ProjectionInfo projectionInfo;
    private String compiledQueryUuid;

    public CompileProjectionOperation() {
    }

    public CompileProjectionOperation(String name, String compiledQueryUuid, ProjectionInfo projectionInfo) {
        super(name);
        this.projectionInfo = projectionInfo;
        this.compiledQueryUuid = compiledQueryUuid;
    }

    @Override
    public int getId() {
        return COMPILE_PROJECTION;
    }

    @Override
    public void run() throws Exception {
        container.getRecordStore(getPartitionId()).compileProjection(compiledQueryUuid, projectionInfo);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(compiledQueryUuid);
        out.writeObject(projectionInfo);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        compiledQueryUuid = in.readUTF();
        projectionInfo = in.readObject();
    }
}
