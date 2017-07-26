package com.axibase.tsd.driver.jdbc.converter;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang3.StringUtils;

class AtsdSqlInsertConverter extends AtsdSqlConverter<SqlInsert> {

    private static final String VALUES = " values ";

    AtsdSqlInsertConverter(boolean timestampTz) {
        super(timestampTz);
    }

    @Override
    public String prepareSql(String sql) {
        logger.debug("[prepareSql] in: {}", sql);
        final int begin = sql.indexOf('(') + 1;
        final int end = sql.indexOf(')');
        final String columnNames = StringUtils.replace(sql.substring(begin, end), "'", "\"");
        StringBuilder buffer = new StringBuilder();
        buffer.append(StringUtils.replace(sql.substring(0, begin), "'", "\"").toLowerCase());
        String[] names = StringUtils.split(columnNames, ',');
        String name;
        for (int i=0;i<names.length;i++) {
            name = names[i].trim();
            if (i > 0) {
                buffer.append(", ");
            }
            appendColumnName(buffer, name);
        }
        String tail = sql.substring(end).trim();
        buffer.append(')').append(VALUES).append(tail.substring(tail.indexOf('(')));
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
        for (SqlNode operand : operands) {
            result.add(getOperandValue(operand, parameterValues));
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
