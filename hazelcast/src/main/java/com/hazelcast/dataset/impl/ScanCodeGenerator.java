package com.hazelcast.dataset.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.BetweenPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.NotPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public abstract class ScanCodeGenerator {

    private final StringBuffer codeBuffer = new StringBuffer();
    private final Map<String, Field> variables = new HashMap<String, Field>();
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    protected final Predicate predicate;
    protected final RecordMetadata recordMetadata;
    protected final String compilationId;

    public ScanCodeGenerator(String compilationId, Predicate predicate, RecordMetadata recordMetadata) {
        this.compilationId = compilationId;
        this.predicate = predicate;
        this.recordMetadata = recordMetadata;
    }

    public abstract String className();

    public abstract void generate();

    protected void generateBindFields() {
        for (Map.Entry<String, Field> variable : variables.entrySet()) {
            Field variableField = variable.getValue();
            String variableName = variable.getKey();
            append("    private ").append(variableField.getType().getName()).append(" ");

            append(variableName).append(";\n");
        }
        append("\n");
    }

    public ScanCodeGenerator append(String s, Object... args) {
        codeBuffer.append(String.format(s, (Object[]) args));
        return this;
    }

    public ScanCodeGenerator append(long i) {
        codeBuffer.append(i);
        return this;
    }

    protected void generateBindMethod() {
        append("    public void bind(Map<String, Object> binding){\n");
        for (Map.Entry<String, Field> variable : variables.entrySet()) {
            Field variableField = variable.getValue();
            String variableName = variable.getKey();
            append("        ").append(variableName).append("=");
            append("(");
            if (variableField.getType().equals(Integer.TYPE)) {
                append("Integer");
            } else if (variableField.getType().equals(Long.TYPE)) {
                append("Long");
            } else if (variableField.getType().equals(Short.TYPE)) {
                append("Short");
            } else if (variableField.getType().equals(Float.TYPE)) {
                append("Float");
            } else if (variableField.getType().equals(Double.TYPE)) {
                append("Double");
            } else if (variableField.getType().equals(Boolean.TYPE)) {
                append("Boolean");
            } else if (variableField.getType().equals(Short.TYPE)) {
                append("Short");
            } else {
                throw new RuntimeException();
            }
            append(")binding.get(\"").append(variableName).append("\");\n");
        }
        append("    }\n\n");
    }

    public String getCode() {
        return codeBuffer.toString();
    }

    protected void toCode(Predicate predicate, int unrollIndex) {
        if (predicate instanceof SqlPredicate) {
            toCode((SqlPredicate) predicate, unrollIndex);
        } else if (predicate instanceof TruePredicate) {
            append(" true ");
        } else if (predicate instanceof NotPredicate) {
            toCode((NotPredicate) predicate, unrollIndex);
        } else if (predicate instanceof AndPredicate) {
            toCode((AndPredicate) predicate, unrollIndex);
        } else if (predicate instanceof OrPredicate) {
            toCode((OrPredicate) predicate, unrollIndex);
        } else if (predicate instanceof BetweenPredicate) {
            toCode((BetweenPredicate) predicate, unrollIndex);
        } else if (predicate instanceof NotEqualPredicate) {
            toCode((NotEqualPredicate) predicate, unrollIndex);
        } else if (predicate instanceof EqualPredicate) {
            toCode((EqualPredicate) predicate, unrollIndex);
        } else if (predicate instanceof GreaterLessPredicate) {
            toCode((GreaterLessPredicate) predicate, unrollIndex);
        } else {
            throw new RuntimeException("Unhandled predicate:" + predicate.getClass());
        }
    }

    private void toCode(GreaterLessPredicate predicate, int unrollIndex) {
        String operator;
        if (predicate.isEqual()) {
            if (predicate.isLess()) {
                operator = "<=";
            } else {
                operator = ">=";
            }
        } else {
            if (predicate.isLess()) {
                operator = "<";
            } else {
                operator = ">";
            }
        }

        comparisonToCode(predicate.getAttributeName(), predicate.getValue(), operator, unrollIndex);
    }

    private void toCode(EqualPredicate predicate, int unrollIndex) {
        String attributeName = predicate.getAttributeName();
        Comparable value = predicate.getValue();
        if (attributeName.equals("true") && value.equals("true")) {
            append(" true ");
        } else {
            comparisonToCode(attributeName, value, "==", unrollIndex);
        }
    }

    private void toCode(NotEqualPredicate predicate, int unrollIndex) {
        comparisonToCode(predicate.getAttributeName(), predicate.getValue(), "!=", unrollIndex);
    }

    private void toCode(BetweenPredicate predicate, int unrollIndex) {
        // between predicate is rewritten to:((attribute>=from) and (attribute<=to))
        GreaterLessPredicate left = new GreaterLessPredicate(
                predicate.getAttributeName(), predicate.getFrom(), true, false);
        GreaterLessPredicate right = new GreaterLessPredicate(
                predicate.getAttributeName(), predicate.getTo(), true, true);
        AndPredicate andPredicate = new AndPredicate(left, right);
        toCode((Predicate) andPredicate, unrollIndex);
    }

    private void toCode(OrPredicate predicate, int unrollIndex) {
        boolean first = true;
        for (Predicate p : predicate.getPredicates()) {
            if (first) {
                first = false;
            } else {
                append(" || ");
            }
            append("(");
            toCode(p, unrollIndex);
            append(")");
        }
    }

    private void toCode(AndPredicate predicate, int unrollIndex) {
        boolean first = true;
        for (Predicate p : predicate.getPredicates()) {
            if (first) {
                first = false;
            } else {
                append(" && ");
            }
            append("(");
            toCode(p, unrollIndex);
            append(")");
        }
    }

    private void toCode(NotPredicate predicate, int unrollIndex) {
        append(" !(");
        toCode((Predicate) predicate, unrollIndex);
        append(")");
    }

    private void toCode(SqlPredicate predicate, int unrollIndex) {
        toCode(predicate.getPredicate(), unrollIndex);
    }

    private void comparisonToCode(String attributeName, Comparable value, String operator, int unrollIndex) {
        Field field = generateGetField(attributeName, unrollIndex);
        append(operator);

        if (value instanceof String) {
            String valueString = (String) value;
            if (valueString.startsWith("$")) {
                String variableName = valueString.substring(1);
                variables.put(variableName, field);
                append(variableName);
            } else {
                append(valueString);
            }
        } else {
            append(value.toString());
        }
    }

    protected Field generateGetField(String attributeName, int unrollIndex) {
        Field field = recordMetadata.getField(attributeName);
        long offset = unsafe.objectFieldOffset(field) - recordMetadata.getRecordDataOffset();

        if (field.getType().equals(Integer.TYPE)) {
            append("unsafe.getInt(");
        } else if (field.getType().equals(Long.TYPE)) {
            append("unsafe.getLong(");
        } else if (field.getType().equals(Short.TYPE)) {
            append("unsafe.getShort(");
        } else if (field.getType().equals(Float.TYPE)) {
            append("unsafe.getFloat(");
        } else if (field.getType().equals(Double.TYPE)) {
            append("unsafe.getDouble(");
        } else if (field.getType().equals(Boolean.TYPE)) {
            append("unsafe.getBoolean(null,");
        } else if (field.getType().equals(Short.TYPE)) {
            append("unsafe.getShort(");
        } else if (field.getType().equals(Character.TYPE)) {
            append("unsafe.getChar(");
        } else {
            throw new RuntimeException("Unhandled field comparison: '" + field.getType() + "' for attribute:" + attributeName);
        }

        append("offset+").append("" + (recordMetadata.getRecordDataSize() * unrollIndex)).append("+").append(offset);
        append(")");
        return field;
    }
}
