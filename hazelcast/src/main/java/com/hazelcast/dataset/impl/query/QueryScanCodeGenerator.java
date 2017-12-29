package com.hazelcast.dataset.impl.query;

import com.hazelcast.dataset.impl.RecordMetadata;
import com.hazelcast.dataset.impl.ScanCodeGenerator;
import com.hazelcast.query.Predicate;

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
        append("    public void run(){\n");
        append("       long offset = slabPointer;\n");
        append("       for(long l=0; l<recordIndex; l++){\n");
        append("           if(");
        toCode(predicate);
        append("){\n");
        append("                count++;\n");
        append("           }\n");
        append("           offset += recordDataSize;\n");
        append("        }\n");

        append("        if(count>0) System.out.println(\"count=\"+count);\n");
        append("    }\n\n");
    }
}
