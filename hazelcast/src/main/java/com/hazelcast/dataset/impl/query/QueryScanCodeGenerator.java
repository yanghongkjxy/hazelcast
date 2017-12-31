package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.RecordMetadata;
import com.hazelcast.dataset.impl.ScanCodeGenerator;
import com.hazelcast.query.Predicate;

import java.lang.reflect.Field;

public class QueryScanCodeGenerator extends ScanCodeGenerator {

    public QueryScanCodeGenerator(String compilationId, Predicate predicate, RecordMetadata recordMetadata) {
        super(compilationId, predicate, recordMetadata);
    }

    @Override
    public void generate() {
        append("import java.util.*;\n");
        append("public class " + className() + " extends com.hazelcast.dataset.impl.query.QueryScan {\n\n");
        append("    private long count;\n");
        generateRunMethod();
        generateBindFields();
        generateBindMethod();
        append("}\n");
    }

    @Override
    public String className() {
        return "QueryScan_" + compilationId;
    }

    private void generateRunMethod() {
        append("    public List run(){\n");
        append("       List result = new LinkedList();\n");
        append("       long offset=slabPointer;\n");
        append("       for(long l=0;l<recordIndex;l++){\n");
        append("           if(");
        toCode(predicate, 0);
        append("){\n");
        append("               " + recordMetadata.getRecordClassName() + " record = new " + recordMetadata.getRecordClassName() + "();\n");
        for (String fieldName : recordMetadata.getFields().keySet()) {
            append("               record." + fieldName + "=");
            generateGetField(fieldName, 0);
            append(";\n");
        }
        append("               result.add(record);\n");
        append("           }\n");
        append("           offset += recordDataSize;\n");
        append("        }\n");
        append("        return result;\n");
        append("    }\n\n");
    }
}
