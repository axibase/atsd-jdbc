package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang3.StringUtils;

class AtsdSqlInsertConverter extends AtsdSqlConverter<SqlInsert> {

    AtsdSqlInsertConverter(boolean timestampTz) {
        super(timestampTz);
    }

    @Override
    protected String prepareSql(String sql) {
        logger.debug("[prepareSql] in: {}", sql);
        final int begin = sql.indexOf('(') + 1;
        final int end = sql.indexOf(')');
        final String beforeValues = StringUtils.replace(sql.substring(0, end), "'", "\"");
        StringBuilder buffer = new StringBuilder();
        buffer.append(beforeValues.substring(0, begin));
        String[] names = StringUtils.split(beforeValues.substring(begin, end), ',');
        String name;
        for (int i=0;i<names.length;i++) {
            name = names[i].trim();
            if (i > 0) {
                buffer.append(", ");
            }
            if (EnumUtil.isReservedSqlToken(name.toUpperCase()) || name.startsWith(PREFIX_TAGS)) {
                buffer.append('\"').append(name).append('\"');
            } else {
                buffer.append(name);
            }
        }
        buffer.append(sql.substring(end));
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
        return result;
    }

    @Override
    protected List<Object> getColumnValues(List<Object> parameterValues) {
        SqlBasicCall sourceNode = (SqlBasicCall) rootNode.getSource();
        SqlBasicCall valuesNode = (SqlBasicCall) sourceNode.getOperandList().get(0);
        List<SqlNode> operands = valuesNode.getOperandList();
        List<Object> result = new ArrayList<>(operands.size());
        Object value;
        for (SqlNode operand : operands) {
            value = getOperandValue(operand);
            if (value instanceof DynamicParam) {
                if (parameterValues == null || parameterValues.isEmpty()) {
                    throw new IllegalArgumentException("Parameter values is null or empty");
                }
                value = parameterValues.get(((DynamicParam) value).index);
            }
            result.add(value);
        }
        return result;
    }

    @Override
    protected List<List<Object>> getColumnValuesBatch(List<List<Object>> parameterValueBatch) {
        List<List<Object>> result = new ArrayList<>(parameterValueBatch.size());
        for (List<Object> parameterValues : parameterValueBatch) {
            result.add(getColumnValues(parameterValues));
        }
        return result;
    }

}
