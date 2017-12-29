package com.hazelcast.dataset.impl.aggregation;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.aggregation.impl.CountAggregator;
import com.hazelcast.aggregation.impl.DoubleSumAggregator;
import com.hazelcast.aggregation.impl.LongAverageAggregator;
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
        return "Aggregation_" + compilationId;
    }

    public Field field() {
        return extractedFields.iterator().next();
    }

    @Override
    public void generate() {
        append("import java.util.*;\n");
        append("public class " + className() + " extends com.hazelcast.dataset.impl.aggregation.AggregationScan {\n\n");
        generateAggregatorField();
        generateAggregatorGetter();
        generateRunMethod();
        generateBindFields();
        generateBindMethod();
        append("}\n");
    }

    private void generateAggregatorField() {
        append("    private final " + aggregator.getClass().getName() + " aggregator = new " + aggregator.getClass().getName() + "();\n\n");
    }

    private void generateAggregatorGetter() {
        append("    public " + aggregator.getClass().getName() + " getAggregator(){return aggregator;}\n\n");
    }

    private void generateRunMethod() {
        append("    public void run(){\n");

        int unrollCount = 4;

        for (int unrollIndex = 0; unrollIndex < unrollCount; unrollIndex++) {
            if (aggregator instanceof CountAggregator) {
                append("       long result_%d=0;\n", unrollIndex);
            } else if (aggregator instanceof LongSumAggregator) {
                append("       long result_%d=0;\n", unrollIndex);
            } else if (aggregator instanceof MinAggregator) {
                append("       long result_%d=Long.MAX_VALUE;\n", unrollIndex);
            } else if (aggregator instanceof MaxAggregator) {
                append("       long result_%d=Long.MIN_VALUE;\n", unrollIndex);
            } else if (aggregator instanceof LongAverageAggregator) {
                append("       long sum_%d=0;\n", unrollIndex);
                append("       long count_%d=0;\n", unrollIndex);
            } else if (aggregator instanceof DoubleSumAggregator) {
                append("       double result_%d=0;\n", unrollIndex);
            } else {
                throw new RuntimeException();
            }
        }

        append("       long offset=slabPointer;\n");
        append("       for(long l=0;l<recordIndex;l+=%d){\n", unrollCount);

        for (int unrollIndex = 0; unrollIndex < unrollCount; unrollIndex++) {
            append("           if(");
            toCode(predicate, unrollIndex);
            append("){\n");

            if (aggregator instanceof CountAggregator) {
                append("               result_%d+=1;\n", unrollIndex);
            } else if (aggregator instanceof LongSumAggregator) {
                append("               result_%d+=", unrollIndex);
                generateGetField(field().getName(), unrollIndex);
                append(";\n");
            } else if (aggregator instanceof LongAverageAggregator) {
                append("               count_%d+=1;\n", unrollIndex);
                append("               sum_%d+=", unrollIndex);
                generateGetField(field().getName(), unrollIndex);
                append(";\n");
            } else if (aggregator instanceof MinAggregator) {
                append("               long value_%d=", unrollIndex);
                generateGetField(field().getName(), unrollIndex);
                append(";\n");
                append("               if(value_%d<result_%d) result_%d=value_%d;\n", unrollIndex, unrollIndex, unrollIndex, unrollIndex);
            } else if (aggregator instanceof MaxAggregator) {
                append("               long value=");
                generateGetField(field().getName(), unrollIndex);
                append(";\n");
                append("               if(value_%d>result_%d) result_%d=value_%d;\n", unrollIndex, unrollIndex, unrollIndex, unrollIndex);
            } else if (aggregator instanceof DoubleSumAggregator) {
                append("               result_%d+=", unrollIndex);
                generateGetField(field().getName(), unrollIndex);
                append(";\n");
            }
            //append("                count++;\n");
            append("           }\n");
        }

        append("           offset+=%d*recordDataSize;\n", unrollCount);
        append("        }\n");
        if (aggregator instanceof LongAverageAggregator) {
            append("        long sum=0;\n");
            append("        long count=0;\n");

            for (int unrollIndex = 0; unrollIndex < unrollCount; unrollIndex++) {
                append("        sum+=sum_%d;\n", unrollIndex);
                append("        count+=count_%d;\n", unrollIndex);
            }

            append("        aggregator.init(sum,count);\n");
        } else {
            for (int unrollIndex = 0; unrollIndex < unrollCount; unrollIndex++) {
                append("        aggregator.accumulate(result_%d);\n", unrollIndex);
            }
        }
        append("    }\n\n");
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
