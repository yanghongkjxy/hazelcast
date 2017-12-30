package com.hazelcast.dataset.impl;

import com.hazelcast.config.DataSetConfig;
import com.hazelcast.internal.memory.impl.UnsafeUtil;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.CHAR_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.DOUBLE_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.SHORT_SIZE_IN_BYTES;

public class RecordMetadata {

    private final Map<String, Field> fields = new HashMap<String, Field>();
    private long recordDataOffset;
    private long recordDataSize;
    private final DataSetConfig config;
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;

    public RecordMetadata(DataSetConfig dataSetConfig) {
        this.config = dataSetConfig;
        initRecordData(config.getValueClass());
        //System.out.println("record size:" + recordDataSize);
    }

    public Class getValueClass() {
        return config.getValueClass();
    }

    public Map<String, Field> getFields() {
        return fields;
    }

    public Field getField(String fieldName) {
        return fields.get(fieldName);
    }

    public long getRecordDataOffset() {
        return recordDataOffset;
    }

    public long getRecordDataSize() {
        return recordDataSize;
    }

    private void initRecordData(Class clazz) {
        long end = 0;
        long minFieldOffset = Long.MAX_VALUE;
        long maxFieldOffset = 0;
        do {
            for (Field f : clazz.getDeclaredFields()) {
                if (Modifier.isStatic(f.getModifiers())) {
                    continue;
                }

                fields.put(f.getName(), f);

                long fieldOffset = unsafe.objectFieldOffset(f);

                if (fieldOffset > maxFieldOffset) {
                    maxFieldOffset = fieldOffset;
                    //System.out.println("fieldOffset:" + fieldOffset + " field.name:" + f.getName());
                    end = fieldOffset + fieldSize(f);
                }

                if (fieldOffset < minFieldOffset) {
                    minFieldOffset = fieldOffset;
                }
            }
        } while ((clazz = clazz.getSuperclass()) != null);

       // System.out.println("minFieldOffset:" + minFieldOffset);
       // System.out.println("maxFieldOffset:" + maxFieldOffset);
       // System.out.println("end:" + end);

        this.recordDataSize = end - minFieldOffset;
        this.recordDataOffset = minFieldOffset;
    }

    private static int fieldSize(Field field) {
        if (field.getType().equals(Integer.TYPE)) {
            return INT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Long.TYPE)) {
            return LONG_SIZE_IN_BYTES;
        } else if (field.getType().equals(Short.TYPE)) {
            return SHORT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Float.TYPE)) {
            return FLOAT_SIZE_IN_BYTES;
        } else if (field.getType().equals(Double.TYPE)) {
            return DOUBLE_SIZE_IN_BYTES;
        } else if (field.getType().equals(Boolean.TYPE)) {
            return BOOLEAN_SIZE_IN_BYTES;
        } else if (field.getType().equals(Character.TYPE)) {
            return CHAR_SIZE_IN_BYTES;
        } else {
            throw new RuntimeException(
                    "Unrecognized field type: field '" + field.getName() + "' ,type '" + field.getType().getName() + "' ");
        }
    }
}
