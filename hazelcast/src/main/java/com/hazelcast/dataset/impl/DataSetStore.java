package com.hazelcast.dataset.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.impl.aggregation.AggregationCodeGenerator;
import com.hazelcast.dataset.impl.aggregation.AggregationScan;
import com.hazelcast.dataset.impl.projection.ProjectionCodeGenerator;
import com.hazelcast.dataset.impl.projection.ProjectionScan;
import com.hazelcast.dataset.impl.query.QueryScan;
import com.hazelcast.dataset.impl.query.QueryScanCodeGenerator;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.spi.serialization.SerializationService;
import sun.misc.Unsafe;

import java.util.Map;

import static java.lang.String.format;

public class DataSetStore {

    private final SerializationService serializationService;
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final long slabPointer;
    private final Compiler compiler;
    private long recordIndex = 0;
    private RecordMetadata recordMetadata;

    public DataSetStore(DataSetConfig config, SerializationService serializationService, Compiler compiler) {
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

    public void compilePredicate(String compileId, Predicate predicate) {
        ScanCodeGenerator codeGenerator = new QueryScanCodeGenerator(
                compileId, predicate, recordMetadata);
        codeGenerator.generate();

        compiler.compile(codeGenerator.className(), codeGenerator.getCode());

        //System.out.println("compile:" + predicate);
        //System.out.println(codeGenerator.getCode() + "\n");
    }

    public void fullTableScan(String compileId, Map<String, Object> bindings) {
        Class<Scan> clazz = compiler.load("QueryScan_" + compileId);
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
        scan.bind(bindings);
        scan.run();
    }

    public void compileProjection(String compileId, ProjectionRecipe extraction) {
        ScanCodeGenerator codeGenerator = new ProjectionCodeGenerator(
                compileId, extraction, recordMetadata);
        codeGenerator.generate();

        compiler.compile(codeGenerator.className(), codeGenerator.getCode());

        //System.out.println("compile:" + extraction.getPredicate());
        //System.out.println(codeGenerator.getCode() + "\n");
    }

    public long size() {
        return recordIndex;
    }

    public long usedMemory() {
        return recordIndex * recordMetadata.getRecordDataSize();
    }
    public void projection(String compileId, Map<String, Object> bindings) {
        Class<Scan> fullTableScanClass = compiler.load("ProjectionScan_" + compileId);
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
        scan.bind(bindings);
        scan.run();
    }

    public Aggregator aggregate(String compileId, Map<String, Object> bindings) {
        Class<Scan> scanClass = compiler.load("Aggregation_" + compileId);
        AggregationScan aggregation;
        try {
            aggregation = (AggregationScan) scanClass.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        aggregation.unsafe = unsafe;
        aggregation.recordDataSize = recordMetadata.getRecordDataSize();
        aggregation.recordIndex = recordIndex;
        aggregation.slabPointer = slabPointer;
        aggregation.bind(bindings);
        aggregation.run();
        return aggregation.getAggregator();
    }

    public void compileAggregation(String compileId, AggregationRecipe aggregationRecipe) {
        ScanCodeGenerator codeGenerator = new AggregationCodeGenerator(
                compileId, aggregationRecipe, recordMetadata);
        codeGenerator.generate();

        compiler.compile(codeGenerator.className(), codeGenerator.getCode());

        //System.out.println("compile:" + aggregationRecipe.getPredicate());
        //System.out.println(codeGenerator.getCode() + "\n");
    }

}
