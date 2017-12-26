package com.hazelcast.simplemap.impl;

import com.hazelcast.internal.memory.impl.UnsafeUtil;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.GreaterLessPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.NotPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import com.hazelcast.util.UuidUtil;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

class TableScanCompiler {

    private final StringBuffer codeBuffer = new StringBuffer();
    private final Map<String, Field> variables = new HashMap<String, Field>();
    private final Unsafe unsafe = UnsafeUtil.UNSAFE;
    private final Map<String, Field> fields;
    private final Predicate predicate;
    private final long recordDataOffset;
    private final long recordDataSize;
    private final String className;

    public TableScanCompiler(Map<String, Field> fields, Predicate predicate, long recordDataOffset, long recordDataSize) {
        this.fields = fields;
        this.predicate = predicate;
        this.recordDataOffset = recordDataOffset;
        this.recordDataSize = recordDataSize;
        this.className = "FullTableScan_" + UuidUtil.newUnsecureUuidString().replace("-","");
    }

    public void compile() {
        codeBuffer.append("public class " + className + " extends com.hazelcast.simplemap.impl.FullTableScan{\n");
        codeBuffer.append("    public void run(){\n");
        codeBuffer.append("       long offset=slabPointer;\n");
        codeBuffer.append("       for(long l=0;l<index;l++){\n");
        codeBuffer.append("           if(");
        predicateToCode(predicate);
        codeBuffer.append("){\n");
        codeBuffer.append("                //todo\n");
        codeBuffer.append("           }\n");
        codeBuffer.append("           offset+=recordDataSize;\n");
        codeBuffer.append("        }\n");
        codeBuffer.append("\n");
        codeBuffer.append("    }\n");
        for (Map.Entry<String, Field> variable : variables.entrySet()) {
            Field variableField = variable.getValue();
            String variableName = variable.getKey();
            codeBuffer.append("    private final ").append(variableField.getType()).append(" ").append(variableName).append(";\n");
        }

        codeBuffer.append("    public " + className + "(Map<String, Object> binding){\n");
        for (Map.Entry<String, Field> variable : variables.entrySet()) {
            String variableName = variable.getKey();
            codeBuffer.append("        ").append(variableName).append("=").append("binding.get(").append(variableName).append(");\n");
        }
        codeBuffer.append("    }\n");

        codeBuffer.append("}\n");

        System.out.println("compile:" + predicate);
        System.out.println(codeBuffer + "\n");
    }

    public String getClassName() {
        return className;
    }

    public String toJavacode() {
        return codeBuffer.toString();
    }

    private void predicateToCode(Predicate predicate) {
        if (predicate instanceof SqlPredicate) {
            predicateToCode(((SqlPredicate) predicate).getPredicate());
        } else if (predicate instanceof TruePredicate) {
            codeBuffer.append(" true ");
        } else if (predicate instanceof NotPredicate) {
            NotPredicate notPredicate = (NotPredicate) predicate;
            codeBuffer.append(" !(");
            predicateToCode(notPredicate);
            codeBuffer.append(")");
        } else if (predicate instanceof AndPredicate) {
            AndPredicate andPredicate = (AndPredicate) predicate;
            boolean first = true;
            for (Predicate p : andPredicate.getPredicates()) {
                if (first) {
                    first = false;
                } else {
                    codeBuffer.append(" && ");
                }
                codeBuffer.append("(");
                predicateToCode(p);
                codeBuffer.append(")");
            }
        } else if (predicate instanceof OrPredicate) {
            OrPredicate orPredicate = (OrPredicate) predicate;
            boolean first = true;
            for (Predicate p : orPredicate.getPredicates()) {
                if (first) {
                    first = false;
                } else {
                    codeBuffer.append(" || ");
                }
                codeBuffer.append("(");
                predicateToCode(p);
                codeBuffer.append(")");
            }
        } else if (predicate instanceof NotEqualPredicate) {
            NotEqualPredicate notEqualPredicate = (NotEqualPredicate) predicate;
            addComparison(codeBuffer, notEqualPredicate.getAttributeName(), notEqualPredicate.getValue(), "!=");
        } else if (predicate instanceof EqualPredicate) {
            EqualPredicate equalPredicate = (EqualPredicate) predicate;
            addComparison(codeBuffer, equalPredicate.getAttributeName(), equalPredicate.getValue(), "==");
        } else if (predicate instanceof GreaterLessPredicate) {
            GreaterLessPredicate greaterLessPredicate = (GreaterLessPredicate) predicate;
            String operator;
            if (greaterLessPredicate.isEqual()) {
                if (greaterLessPredicate.isLess()) {
                    operator = "<=";
                } else {
                    operator = ">=";
                }
            } else {
                if (greaterLessPredicate.isLess()) {
                    operator = "<";
                } else {
                    operator = ">";
                }
            }

            addComparison(codeBuffer, greaterLessPredicate.getAttributeName(), greaterLessPredicate.getValue(), operator);
        } else {
            throw new RuntimeException("Unhandled predicate:" + predicate.getClass());
        }
    }

    private void addComparison(StringBuffer sb, String attributeName, Comparable value, String operator) {
        Field field = fields.get(attributeName);
        long offset = unsafe.objectFieldOffset(field) - recordDataOffset;

        if (field.getType().equals(Integer.TYPE)) {
            sb.append("unsafe.getInt(");
        } else if (field.getType().equals(Long.TYPE)) {
            sb.append("unsafe.getLong(");
        } else if (field.getType().equals(Short.TYPE)) {
            sb.append("unsafe.getShort(");
        } else if (field.getType().equals(Float.TYPE)) {
            sb.append("unsafe.getFloat(");
        } else if (field.getType().equals(Double.TYPE)) {
            sb.append("unsafe.getDouble(");
        } else if (field.getType().equals(Boolean.TYPE)) {
            sb.append("unsafe.getBoolean(");
        } else if (field.getType().equals(Short.TYPE)) {
            sb.append("unsafe.getShort(");
        } else {
            throw new RuntimeException();
        }
        sb.append("offset+").append(offset);
        sb.append(")");
        sb.append(operator);

        if (value instanceof String) {
            String valueString = (String) value;
            if (valueString.startsWith("$")) {
                String variableName = valueString.substring(1);
                variables.put(variableName, field);
                sb.append(variableName);
            } else {
                sb.append(valueString);
            }
        } else {
            sb.append(value);
        }
    }
}
