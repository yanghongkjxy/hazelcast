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
    protected final String className;
    protected final RecordMetadata recordMetadata;

    public ScanCodeGenerator(String className,
                             Predicate predicate, RecordMetadata recordMetadata) {
        this.predicate = predicate;
        this.recordMetadata = recordMetadata;
        this.className = className;
    }

    public abstract void generate();

    protected void generateBindFields() {
        for (Map.Entry<String, Field> variable : variables.entrySet()) {
            Field variableField = variable.getValue();
            String variableName = variable.getKey();
            append("    private ").append(variableField.getType()).append(" ");

            append(variableName).append(";\n");
        }
    }

    protected ScanCodeGenerator append(Object s){
        codeBuffer.append(s);
        return this;
    }

    protected void generateBindMethod() {
        append("    public void init(Map<String, Object> binding){\n");
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
        append("    }\n");
    }

    public String getClassName() {
        return className;
    }

    public String getCode() {
        return codeBuffer.toString();
    }

    protected void toCode(Predicate predicate) {
        if (predicate instanceof SqlPredicate) {
            toCode((SqlPredicate) predicate);
        } else if (predicate instanceof TruePredicate) {
            append(" true ");
        } else if (predicate instanceof NotPredicate) {
            toCode((NotPredicate) predicate);
        } else if (predicate instanceof AndPredicate) {
            toCode((AndPredicate) predicate);
        } else if (predicate instanceof OrPredicate) {
            toCode((OrPredicate) predicate);
        } else if (predicate instanceof BetweenPredicate) {
            toCode((BetweenPredicate) predicate);
        } else if (predicate instanceof NotEqualPredicate) {
            toCode((NotEqualPredicate) predicate);
        } else if (predicate instanceof EqualPredicate) {
            toCode((EqualPredicate) predicate);
        } else if (predicate instanceof GreaterLessPredicate) {
            toCode((GreaterLessPredicate) predicate);
        } else {
            throw new RuntimeException("Unhandled predicate:" + predicate.getClass());
        }
    }

    private void toCode(GreaterLessPredicate predicate) {
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

        comparisonToCode( predicate.getAttributeName(), predicate.getValue(), operator);
    }

    private void toCode(EqualPredicate predicate) {
        comparisonToCode( predicate.getAttributeName(), predicate.getValue(), "==");
    }

    private void toCode(NotEqualPredicate predicate) {
        comparisonToCode(predicate.getAttributeName(), predicate.getValue(), "!=");
    }

    private void toCode(BetweenPredicate predicate) {
        // between predicate is rewritten to:((attribute>=from) and (attribute<=to))
        GreaterLessPredicate left = new GreaterLessPredicate(
                predicate.getAttributeName(), predicate.getFrom(), true, false);
        GreaterLessPredicate right = new GreaterLessPredicate(
                predicate.getAttributeName(), predicate.getTo(), true, true);
        AndPredicate andPredicate = new AndPredicate(left, right);
        toCode((Predicate) andPredicate);
    }

    private void toCode(OrPredicate predicate) {
        boolean first = true;
        for (Predicate p : predicate.getPredicates()) {
            if (first) {
                first = false;
            } else {
                append(" || ");
            }
            append("(");
            toCode(p);
            append(")");
        }
    }

    private void toCode(AndPredicate predicate) {
        boolean first = true;
        for (Predicate p : predicate.getPredicates()) {
            if (first) {
                first = false;
            } else {
                append(" && ");
            }
            append("(");
            toCode(p);
            append(")");
        }
    }

    private void toCode(NotPredicate predicate) {
        append(" !(");
        toCode((Predicate) predicate);
        append(")");
    }

    private void toCode(SqlPredicate predicate) {
        toCode(predicate.getPredicate());
    }

    private void comparisonToCode(String attributeName, Comparable value, String operator) {
        Field field = generateGetField(attributeName);
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
            append(value);
        }
    }

    protected Field generateGetField(String attributeName) {
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

        append("offset+").append(offset);
        append(")");
        return field;
    }
}
