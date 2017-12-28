package com.hazelcast.simplemap.impl;

import com.hazelcast.query.Predicate;

public class QueryScanCodeGenerator extends ScanCodeGenerator {

    public QueryScanCodeGenerator(String compiledQueryUuid, Predicate predicate, RecordMetadata recordMetadata) {
        super("QueryScan_" + compiledQueryUuid, predicate, recordMetadata);
    }

    @Override
    public void generate() {
        append("import java.util.*;\n");
        append("public class " + className + " extends com.hazelcast.simplemap.impl.QueryScan{\n");
        append("    private long count;\n");
        generateRunMethod();
        generateBindFields();
        generateBindMethod();
        append("}\n");
    }

    private void generateRunMethod() {
        append("    public void run(){\n");
        append("       long offset=slabPointer;\n");
        append("       for(long l=0;l<recordIndex;l++){\n");
        append("           if(");
        toCode(predicate);
        append("){\n");
        append("                count++;\n");
        append("           }\n");
        append("           offset+=recordDataSize;\n");
        append("        }\n");

        append("        if(count>0)System.out.println(\"count=\"+count);\n");
        append("    }\n");
    }
}
