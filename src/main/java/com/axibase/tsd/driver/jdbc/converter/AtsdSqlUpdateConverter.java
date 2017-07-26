package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import com.axibase.tsd.driver.jdbc.util.AtsdColumn;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.commons.lang3.StringUtils;

class AtsdSqlUpdateConverter extends AtsdSqlConverter<SqlUpdate> {

    private static final String UPDATE = "update ";
    private static final String SET = "set ";
    private static final String WHERE = "where ";
    private static final String IS = " is ";
    private static final String LIKE = " like ";
    private static final String ESCAPE = " escape ";

    private final Map<String, String> escapeMap = new HashMap<>();

    AtsdSqlUpdateConverter(boolean timestampTz) {
        super(timestampTz);
    }

    @Override
    public String prepareSql(String sql) {
        logger.debug("[prepareSql] in: {}", sql);
        final int begin = StringUtils.indexOfIgnoreCase(sql, SET) + SET.length();
        final int end = StringUtils.indexOfIgnoreCase(sql, WHERE);
        StringBuilder buffer = new StringBuilder();
        final String tableName = StringUtils.replace(sql.substring(UPDATE.length(), begin - SET.length()), "'", "\"");
        buffer.append(UPDATE).append(tableName).append(SET);
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
            appendColumnName(buffer, name);
            buffer.append('=').append(value);
        }

        buffer.append(' ').append(WHERE);
        String tmp = sql.substring(end + WHERE.length());
        pairs = tmp.split("(?i)( and )");
        String pair;
        for (int i = 0; i < pairs.length; i++) {
            pair = StringUtils.trim(pairs[i]);
            idx = pair.indexOf('=');
            if (idx == -1 || idx == pair.length() - 1) {
                idx = StringUtils.indexOfIgnoreCase(pair, IS);
                if (idx != -1) {
                    value = pair.substring(idx).toUpperCase();
                } else {
                    idx = StringUtils.indexOfIgnoreCase(pair, LIKE);
                    if (idx == -1) {
                        throw new AtsdRuntimeException("Invalid part of clause: " + pair);
                    }

                    int idxOfEscape = StringUtils.indexOfIgnoreCase(pair, ESCAPE);
                    if (idxOfEscape == -1) {
                        value = LIKE + pair.substring(idx + LIKE.length());
                    } else {
                        StringBuilder valueBuffer = new StringBuilder();
                        valueBuffer.append(LIKE)
                                .append(pair.substring(idx + LIKE.length(), idxOfEscape).trim())
                                .append(ESCAPE)
                                .append(pair.substring(idxOfEscape + ESCAPE.length()));
                        value = valueBuffer.toString();
                    }
                }
            } else {
                value = '=' + pair.substring(idx + 1).trim();
            }
            name = pair.substring(0, idx).trim().toLowerCase();
            if (i > 0) {
                buffer.append(" AND ");
            }
            if (EnumUtil.isReservedSqlToken(name.toUpperCase()) || name.startsWith(PREFIX_ENTITY)
                    || name.startsWith(PREFIX_METRIC) || name.startsWith(PREFIX_SERIES_TAGS)) {
                buffer.append('\"').append(name).append('\"');
            } else {
                int valueIndex = name.indexOf(AtsdColumn.VALUE);
                if (valueIndex != -1) {
                    if (valueIndex > 0 && name.charAt(valueIndex) != '"') {
                        name = StringUtils.replace(name, AtsdColumn.VALUE, "\"value\"");
                    }
                }
                buffer.append(name);
            }
            buffer.append(value);
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
        for (SqlNode node : expressionList) {
            result.add(getOperandValue(node, parameterValues));
        }

        SqlBasicCall conditionNode = (SqlBasicCall) rootNode.getCondition();
        List<Object> tmp = getColumnValues(conditionNode, parameterValues);
        result.addAll(tmp);

        return result;
    }

    private static List<Object> getColumnValues(final SqlBasicCall inputNode, List<Object> parameterValues) {
        List<SqlNode> operands = inputNode.getOperandList();
        List<Object> result = new ArrayList<>(1);
        if (isOperatorKindOf(inputNode.getOperator(), SqlKind.IS_NULL) && operands.size() == 1) {
            result.add(null);
        } else if (isOperatorKindOf(inputNode.getOperator(), SqlKind.LIKE) && operands.size() == 3) {
            Object value = getOperandValue(inputNode.getOperandList().get(1), parameterValues);
            if (!(value instanceof String)) {
                throw new IllegalArgumentException("Invalid value: " + value + ". Actual type: " + value.getClass().getSimpleName() + ", expected type: " +
                        "String");
            }
            String escapeValue = (String) getOperandValue(inputNode.getOperandList().get(2), parameterValues);
            result.add(StringUtils.remove((String) value, escapeValue));
        } else {
            for (SqlNode operand : operands) {
                if (SqlKind.LITERAL == operand.getKind()) {
                    result.add(getOperandValue(operand, parameterValues));
                    if (operands.size() == 2) {
                        break;
                    }
                } else if (operand instanceof SqlBasicCall) {
                    result.addAll(getColumnValues((SqlBasicCall) operand, parameterValues));
                }
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

    private static boolean isOperatorKindOf(SqlOperator operator, SqlKind kind) {
        return operator == null || kind == null ? false : operator.getKind() == kind;
    }
}
