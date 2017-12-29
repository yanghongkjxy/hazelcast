package com.hazelcast.dataset.impl;

import com.hazelcast.config.SimpleMapConfig;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.spi.serialization.SerializationService;
import sun.misc.Unsafe;

import java.util.Map;

import static java.lang.String.format;

public class SimpleRecordStore {

    private final SerializationService serializationService;
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final long slabPointer;
    private final Compiler compiler;
    private long recordIndex = 0;
    private RecordMetadata recordMetadata;

    public SimpleRecordStore(SimpleMapConfig config, SerializationService serializationService, Compiler compiler) {
        this.compiler = compiler;
        this.recordMetadata = new RecordMetadata(config);
        this.serializationService = serializationService;
        this.slabPointer = unsafe.allocateMemory(config.getSizeBytesPerPartition());

    }

    public void set(Data keyData, Data valueData) {
        Object record = serializationService.toObject(valueData);
        if (record.getClass() != recordMetadata.getValueClass()) {
            throw new RuntimeException(format("Expected value of class '%s', but found '%s' ",
                    record.getClass().getName(), recordMetadata.getValueClass().getClass().getName()));
        }

        unsafe.copyMemory(
                record,
                recordMetadata.getRecordDataOffset(),
                null,
                slabPointer + (recordIndex * recordMetadata.getRecordDataSize()),
                recordMetadata.getRecordDataSize());
        recordIndex++;
    }

    public void compilePredicate(String compiledQueryUuid, Predicate predicate) {
        ScanCodeGenerator codeGenerator = new QueryScanCodeGenerator(
                compiledQueryUuid, predicate, recordMetadata);
        codeGenerator.generate();

        compiler.compile(codeGenerator.getClassName(), codeGenerator.getCode());

        System.out.println("compile:" + predicate);
        System.out.println(codeGenerator.getCode() + "\n");
    }

    public void fullTableScan(String compiledQueryUuid, Map<String, Object> bindings) {
        Class<Scan> clazz = compiler.load("QueryScan_" + compiledQueryUuid);
        QueryScan scan;
        try {
            scan = (QueryScan) clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        scan.unsafe = unsafe;
        scan.recordDataSize = recordMetadata.getRecordDataSize();
        scan.recordIndex = recordIndex;
        scan.slabPointer = slabPointer;
        scan.init(bindings);
        scan.run();
    }

    public void compileProjection(String compiledQueryUuid, ProjectionRecipe extraction) {
        ScanCodeGenerator codeGenerator = new ProjectionCodeGenerator(
                compiledQueryUuid, extraction, recordMetadata);
        codeGenerator.generate();

        compiler.compile(codeGenerator.getClassName(), codeGenerator.getCode());

        System.out.println("compile:" + extraction.getPredicate());
        System.out.println(codeGenerator.getCode() + "\n");
    }

    public long size() {
        return recordIndex;
    }

    public void projection(String compiledQueryUuid, Map<String, Object> bindings) {
        Class<Scan> fullTableScanClass = compiler.load("ProjectionScan_" + compiledQueryUuid);
        ProjectionScan scan;
        try {
            scan = (ProjectionScan) fullTableScanClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        scan.unsafe = unsafe;
        scan.recordDataSize = recordMetadata.getRecordDataSize();
        scan.recordIndex = recordIndex;
        scan.slabPointer = slabPointer;
        //scan.consumer = todo;
        scan.init(bindings);
        scan.run();
    }

    public void aggregate(String compiledQueryUuid, Map<String, Object> bindings) {

    }
}
