package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.CountAggregator;
import com.hazelcast.aggregation.impl.LongSumAggregator;
import com.hazelcast.aggregation.impl.MaxAggregator;
import com.hazelcast.aggregation.impl.MinAggregator;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.impl.RecordMetadata;
import com.hazelcast.dataset.impl.ScanCodeGenerator;
import com.hazelcast.dataset.impl.projection.ProjectionCodeGenerator;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

public class AggregationCodeGenerator extends ScanCodeGenerator {

    private final Set<Field> extractedFields;
    private final Class<?> projectionClass;
    private final Aggregator aggregator;

    public AggregationCodeGenerator(String compilationId, AggregationRecipe aggregationRecipe, RecordMetadata recordMetadata) {
        super(compilationId, aggregationRecipe.getPredicate(), recordMetadata);
        try {
            this.projectionClass = ProjectionCodeGenerator.class.getClassLoader().loadClass(aggregationRecipe.getProjectionClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        try {
            Class<Aggregator> aggregatorClass = (Class<Aggregator>) ProjectionCodeGenerator.class.getClassLoader().loadClass(aggregationRecipe.getAggregatorClassName());
            this.aggregator = aggregatorClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.extractedFields = extractedFields();

    }

    @Override
    public String className() {
        return aggregator.getClass().getSimpleName()+"_" + compilationId;
    }

    public Field field() {
        return extractedFields.iterator().next();
    }

    @Override
    public void generate() {
        append("import java.util.*;\n");
        append("public class " + className() + " extends com.hazelcast.dataset.impl.aggregation.AggregationScan {\n");
        generateRunMethod();
        generateBindFields();
        generateBindMethod();
        append("}\n");
    }

    private void generateRunMethod() {
        append("    public void run(){\n");

        if (aggregator instanceof CountAggregator) {
            append("       long result=0;\n");
        } else if (aggregator instanceof LongSumAggregator) {
            append("       long result=0;\n");
        } else if (aggregator instanceof MinAggregator) {
            append("       long result = Long.MAX_VALUE;\n");
        } else if (aggregator instanceof MaxAggregator) {
            append("       long result = Long.MIN_VALUE;\n");
        } else {
            throw new RuntimeException();
        }

        append("       long offset=slabPointer;\n");
        append("       for(long l=0; l<recordIndex; l++){\n");
        append("           if(");
        toCode(predicate);
        append("){\n");

        if (aggregator instanceof CountAggregator) {
            append("               result+=1;\n");
        } else if (aggregator instanceof LongSumAggregator) {
            append("               result+=");
            generateGetField(field().getName());
            append(";\n");
        } else if (aggregator instanceof MinAggregator) {
            append("               long value=");
            generateGetField(field().getName());
            append(";\n");
            append("               if(value<result) result=value;\n");
        } else if (aggregator instanceof MaxAggregator) {
            append("               long value=");
            generateGetField(field().getName());
            append(";\n");
            append("               if(value>result) result=value;\n");
        }
        //append("                count++;\n");
        append("           }\n");
        append("           offset += recordDataSize;\n");
        append("        }\n");
        append("        aggregator.accumulate(result);\n");
        append("    }\n");
    }

    private Set<Field> extractedFields() {
        Set<Field> fields = new HashSet<Field>();

        for (Field f : projectionClass.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            Field recordField = recordMetadata.getField(f.getName());
            if (recordField == null) {
                throw new RuntimeException(
                        "Field '" + projectionClass.getName() + '.' + f.getName()
                                + "' is not found on value-class '" + recordMetadata.getValueClass() + "'");
            }

            if (!recordField.getType().equals(f.getType())) {
                throw new RuntimeException(
                        "Field '" + projectionClass.getName() + '.' + f.getName()
                                + "' has a different type compared to '" + recordMetadata.getValueClass() + "'");
            }

            fields.add(f);
        }

        return fields;
    }
}
