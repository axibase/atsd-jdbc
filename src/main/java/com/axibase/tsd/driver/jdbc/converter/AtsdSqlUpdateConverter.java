package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.commons.lang3.StringUtils;

class AtsdSqlUpdateConverter extends AtsdSqlConverter<SqlUpdate> {

    private static final String UPDATE = "UPDATE ";
    private static final String SET = "SET ";
    private static final String WHERE = "WHERE ";

    AtsdSqlUpdateConverter(boolean timestampTz) {
        super(timestampTz);
    }

    @Override
    protected String prepareSql(String sql) {
        logger.debug("[prepareSql] in: {}", sql);
        final int begin = StringUtils.indexOfIgnoreCase(sql, SET) + SET.length();
        final int end = StringUtils.indexOfIgnoreCase(sql, WHERE);
        StringBuilder buffer = new StringBuilder();
        final String tableName = StringUtils.replace(sql.substring(UPDATE.length(), begin - SET.length()), "'", "\"");
        buffer.append(UPDATE).append(tableName).append(' ').append(SET);
        String[] pairs = StringUtils.split(sql.substring(begin, end), ',');
        String name;
        String value;
        int idx;
        for (int i = 0; i < pairs.length; i++) {
            idx = pairs[i].indexOf('=');
            if (idx == -1 || idx == pairs[i].length() - 1) {
                throw new AtsdRuntimeException("Invalid part of clause: " + pairs[i]);
            }
            name = pairs[i].substring(0, idx).trim();
            value = pairs[i].substring(idx + 1).trim();
            if (i > 0) {
                buffer.append(", ");
            }
            if (EnumUtil.isReservedSqlToken(name.toUpperCase()) || name.startsWith(PREFIX_TAGS)) {
                buffer.append('\"').append(name).append('\"');
            } else {
                buffer.append(name);
            }
            buffer.append('=').append(value);
        }

        buffer.append(' ').append(WHERE);
        String tmp = sql.substring(end + WHERE.length());
        pairs = tmp.split("(?i)( and )");
        for (int i = 0; i < pairs.length; i++) {
            idx = pairs[i].indexOf('=');
            if (idx == -1 || idx == pairs[i].length() - 1) {
                throw new AtsdRuntimeException("Invalid part of clause: " + pairs[i]);
            }
            name = pairs[i].substring(0, idx).trim().toLowerCase();
            value = pairs[i].substring(idx + 1).trim();
            if (i > 0) {
                buffer.append(" AND ");
            }
            if (EnumUtil.isReservedSqlToken(name.toUpperCase()) || name.startsWith(PREFIX_TAGS)) {
                buffer.append('\"').append(name).append('\"');
            } else {
                int valueIndex = name.indexOf(VALUE);
                if (valueIndex != -1) {
                    if (valueIndex > 0 && name.charAt(valueIndex) != '"') {
                        name = StringUtils.replace(name, VALUE, "\"value\"");
                    }
                }
                buffer.append(name);
            }
            buffer.append('=').append(value);
        }
        String result = buffer.toString();
        logger.debug("[prepareSql] out: {}", result);
        return result;
    }

    @Override
    protected String getTargetTableName() {
        return getName((SqlIdentifier) rootNode.getTargetTable());
    }

    @Override
    protected List<String> getColumnNames() {
        SqlNodeList columnNodes = rootNode.getTargetColumnList();
        List<String> result = new ArrayList<>(columnNodes.size());
        for (SqlNode columnNode : columnNodes) {
            result.add(getName((SqlIdentifier) columnNode));
        }
        SqlBasicCall conditionNode = (SqlBasicCall) rootNode.getCondition();
        List<String> tmp = getColumnNames(conditionNode);
        result.addAll(tmp);
        return result;
    }

    private static List<String> getColumnNames(final SqlBasicCall inputNode) {
        List<SqlNode> operands = inputNode.getOperandList();
        List<String> result = new ArrayList<>(1);
        for (SqlNode operand : operands) {
            if (SqlKind.IDENTIFIER == operand.getKind()) {
                result.add(getName((SqlIdentifier) operand));
                if (operands.size() == 2) {
                    break;
                }
            } else if (operand instanceof SqlBasicCall) {
                result.addAll(getColumnNames((SqlBasicCall) operand));
            }
        }
        return result;
    }

    @Override
    protected List<Object> getColumnValues(List<Object> parameterValues) {
        SqlNodeList expressionList = rootNode.getSourceExpressionList();
        List<Object> result = new ArrayList<>(expressionList.size());
        Object value;
        for (SqlNode node : expressionList) {
            value = getOperandValue(node);
            if (value instanceof DynamicParam) {
                if (parameterValues == null || parameterValues.isEmpty()) {
                    throw new IllegalArgumentException("Parameter values: " + parameterValues);
                }
                value = parameterValues.get(((DynamicParam) value).index);
            }
            result.add(value);
        }

        SqlBasicCall conditionNode = (SqlBasicCall) rootNode.getCondition();
        List<Object> tmp = getColumnValues(conditionNode);
        result.addAll(tmp);

        return result;
    }

    private static List<Object> getColumnValues(final SqlBasicCall inputNode) {
        List<SqlNode> operands = inputNode.getOperandList();
        List<Object> result = new ArrayList<>(1);
        for (SqlNode operand : operands) {
            if (SqlKind.LITERAL == operand.getKind()) {
                result.add(getOperandValue(operand));
                if (operands.size() == 2) {
                    break;
                }
            } else if (operand instanceof SqlBasicCall) {
                result.addAll(getColumnValues((SqlBasicCall) operand));
            }
        }
        return result;
    }

    @Override
    protected List<List<Object>> getColumnValuesBatch(List<List<Object>> parameterValuesBatch) {
        List<List<Object>> result = new ArrayList<>(parameterValuesBatch.size());
        for (List<Object> parameterValues : parameterValuesBatch) {
            result.add(getColumnValues(parameterValues));
        }
        return result;
    }

}
