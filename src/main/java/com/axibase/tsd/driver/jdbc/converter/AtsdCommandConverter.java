package com.axibase.tsd.driver.jdbc.converter;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.StringUtils;

import com.axibase.tsd.driver.jdbc.util.EnumUtil;

public class AtsdCommandConverter {

    private static final String TBL_ATSD_SERIES = "atsd_series";

    private static final String ENTITY = "ENTITY";
    private static final String DATETIME = "DATETIME";
    private static final String METRIC = "METRIC";
    private static final String VALUE = "value";
    private static final String TEXT = "TEXT";
    private static final String PREFIX_TAGS = "tags.";

    public String convertSqlToCommand(String sql) throws SqlParseException {
        SqlNode rootNode = parseSql(sql);
        switch (rootNode.getKind()) {
            case INSERT:
                return createCommand((SqlInsert) rootNode, null);
            case UPDATE:
                return createCommand((SqlUpdate) rootNode, null);
            default:
                throw new IllegalArgumentException("Illegal SQL type: " + rootNode.getKind().name());
        }
    }

    public String convertSqlToCommand(String sql, List<Object> parameterValues) throws SqlParseException {
        SqlNode rootNode = parseSql(sql);
        switch (rootNode.getKind()) {
            case INSERT:
                return createCommand((SqlInsert) rootNode, parameterValues);
            case UPDATE:
                return createCommand((SqlUpdate) rootNode, parameterValues);
            default:
                throw new IllegalArgumentException("Illegal SQL type: " + rootNode.getKind().name());
        }
    }

    public String convertBatchToCommands(String sql, List<List<Object>> parameterValueBatch) throws SqlParseException {
        SqlNode rootNode = parseSql(sql);
        switch (rootNode.getKind()) {
            case INSERT:
                return createCommandBatch((SqlInsert) rootNode, parameterValueBatch);
            case UPDATE:
                return createCommandBatch((SqlUpdate) rootNode, parameterValueBatch);
            default:
                throw new IllegalArgumentException("Illegal query type: " + rootNode.getKind().name());
        }
    }

    private SqlNode parseSql(String sql) throws SqlParseException {
        sql = prepareSql(sql);
        SqlParser sqlParser = SqlParser.create(sql, SqlParser.configBuilder()
                .setParserFactory(SqlParserImpl.FACTORY)
                .setUnquotedCasing(Casing.TO_LOWER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build());
        return sqlParser.parseStmt();
    }

    private static String prepareSql(String sql) {
        final int begin = sql.indexOf('(') + 1;
        final int end = sql.indexOf(')');
        StringBuilder sb = new StringBuilder();
        sb.append(sql.substring(0, begin));
        String[] names = StringUtils.split(sql.substring(begin, end), ',');
        String name;
        for (int i=0;i<names.length;i++) {
            name = names[i].trim();
            if (i > 0) {
                sb.append(", ");
            }
            if (EnumUtil.isReservedSqlToken(name.toUpperCase()) || name.startsWith(PREFIX_TAGS)) {
                sb.append('\"').append(name).append('\"');
            } else {
                sb.append(name);
            }
        }
        sb.append(sql.substring(end));
        return sb.toString();
    }

    private String createCommand(final SqlInsert rootNode, List<Object> parameterValues) {
        SqlIdentifier targetTable = (SqlIdentifier) rootNode.getTargetTable();
        String tableName = targetTable.getSimple().toLowerCase();
        if (TBL_ATSD_SERIES.equals(tableName)) {
            return createSeriesCommand(rootNode, parameterValues);
        }
        return createSeriesCommand(tableName, rootNode, parameterValues);
    }

    private String createCommandBatch(final SqlInsert rootNode, List<List<Object>> parameterValueBatch) {
        SqlIdentifier targetTable = (SqlIdentifier) rootNode.getTargetTable();
        String tableName = targetTable.getSimple().toLowerCase();
        if (TBL_ATSD_SERIES.equals(tableName)) {
            return createSeriesCommandBatch(rootNode, parameterValueBatch);
        }
        return createSeriesCommandBatch(tableName, rootNode, parameterValueBatch);
    }

    private String createCommand(final SqlUpdate rootNode, List<Object> parameterValues) {
        throw new UnsupportedOperationException("Update not yet implemented");
    }

    private String createCommandBatch(final SqlUpdate rootNode, List<List<Object>> parameterValueBatch) {
        throw new UnsupportedOperationException("Update not yet implemented");
    }

    private String createSeriesCommand(final SqlInsert rootNode, List<Object> parameterValues) {
        final List<String> columnNames = getColumnNames(rootNode);
        final List<Object> values = getInsertValues(rootNode, parameterValues);

        return composeSeriesCommand(columnNames, values);
    }

    private String createSeriesCommandBatch(final SqlInsert rootNode, List<List<Object>> parameterValueBatch) {
        final List<String> columnNames = getColumnNames(rootNode);
        final List<List<Object>> valueBatch = getInsertValueBatch(rootNode, parameterValueBatch);

        StringBuilder sb = new StringBuilder();
        for (List<Object> values : valueBatch) {
            sb.append(composeSeriesCommand(columnNames, values));
        }
        return sb.toString();
    }

    private String createSeriesCommand(String metricName, final SqlInsert rootNode, List<Object> parameterValues) {
        final List<String> columnNames = getColumnNames(rootNode);
        final List<Object> values = getInsertValues(rootNode, parameterValues);
        return composeSeriesCommand(metricName, columnNames, values);
    }

    private String createSeriesCommandBatch(String metricName, final SqlInsert rootNode, List<List<Object>> parameterValueBatch) {
        final List<String> columnNames = getColumnNames(rootNode);
        final List<List<Object>> valueBatch = getInsertValueBatch(rootNode, parameterValueBatch);

        StringBuilder sb = new StringBuilder();
        for (List<Object> values : valueBatch) {
            sb.append(composeSeriesCommand(metricName, columnNames, values));
        }
        return sb.toString();
    }

    private static String composeSeriesCommand(final String metricName, final List<String> columnNames, final List<Object> values) {
        if (columnNames.size() != values.size()) {
            throw new IndexOutOfBoundsException(
                    String.format("Number of values [%d] does not match to number of columns [%d]",
                            values.size(), columnNames.size()));
        }
        SeriesCommand command = new SeriesCommand();
        String columnName;
        Object value;
        for (int i = 0; i<columnNames.size(); i++) {
            columnName = columnNames.get(i);
            value = values.get(i);
            if (value == null) {
                continue;
            }
            if (ENTITY.equalsIgnoreCase(columnName)) {
                command.setEntity(String.valueOf(value));
            } else if (DATETIME.equalsIgnoreCase(columnName)) {
                command.setDateTime(String.valueOf(value));
            } else if (VALUE.equalsIgnoreCase(columnName)) {
                if (!(value instanceof Double)) {
                    throw new IllegalArgumentException("Invalid column value: index=" + i + ", value=" + value);
                }
                command.addValue(metricName, (Double) value);
            } else if (TEXT.equalsIgnoreCase(columnName)) {
                command.addValue(metricName, String.valueOf(value));
            } else if (columnName.startsWith(PREFIX_TAGS)) {
                String tagName = columnName.substring(columnName.indexOf('.') + 1);
                command.addTag(tagName, String.valueOf(value));
            }
        }
        return command.compose();
    }

    private static String composeSeriesCommand(final List<String> columnNames, final List<Object> values) {
        if (columnNames.size() != values.size()) {
            throw new IndexOutOfBoundsException(
                    String.format("Number of values [%d] does not match to number of columns [%d]",
                            values.size(), columnNames.size()));
        }
        SeriesCommand command = new SeriesCommand();
        String columnName;
        Object value;
        String metricName = null;
        for (int i = 0; i<columnNames.size(); i++) {
            columnName = columnNames.get(i);
            value = values.get(i);
            if (value == null) {
                continue;
            }
            if (ENTITY.equalsIgnoreCase(columnName)) {
                command.setEntity(value.toString());
            } else if (DATETIME.equalsIgnoreCase(columnName)) {
                command.setDateTime(value.toString());
            } else if (VALUE.equalsIgnoreCase(columnName) && metricName != null) {
                if (!(value instanceof Double)) {
                    throw new IllegalArgumentException("Invalid column value: index=" + i + ", value=" + value);
                }
                command.addValue(metricName, (Double) value);
            } else if (TEXT.equalsIgnoreCase(columnName) && metricName != null) {
                command.addValue(metricName, value.toString());
            } else if (columnName.startsWith(PREFIX_TAGS) && value != null) {
                String tagName = columnName.substring(columnName.indexOf('.') + 1);
                command.addTag(tagName, value.toString());
            } else if (METRIC.equalsIgnoreCase(columnName) && value instanceof String && StringUtils.isNotBlank((String) value)) {
                metricName = (String) value;
            } else if (value instanceof Double) {
                command.addValue(columnName, (Double) value);
            } else if (value instanceof String) {
                command.addValue(columnName, (String) value);
            }
        }
        return command.compose();
    }

    private static List<String> getColumnNames(SqlInsert rootNode) {
        SqlNodeList columnNodes = rootNode.getTargetColumnList();
        List<String> result = new ArrayList<>(columnNodes.size());
        for (SqlNode columnNode : columnNodes) {
            result.add(((SqlIdentifier) columnNode).getSimple());
        }
        return result;
    }

    private static List<Object> getInsertValues(SqlInsert rootNode) {
        SqlBasicCall sourceNode = (SqlBasicCall) rootNode.getSource();
        SqlBasicCall valuesNode = (SqlBasicCall) sourceNode.getOperandList().get(0);
        List<SqlNode> operands = valuesNode.getOperandList();
        List<Object> result = new ArrayList<>(operands.size());
        for (SqlNode operand : operands) {
            result.add(getOperandValue(operand));
        }
        return result;
    }

    private static List<Object> getInsertValues(SqlInsert rootNode, List<Object> parameterValues) {
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

    private static List<List<Object>> getInsertValueBatch(SqlInsert rootNode, List<List<Object>> parameterValueBatch) {
        SqlBasicCall sourceNode = (SqlBasicCall) rootNode.getSource();
        SqlBasicCall valuesNode = (SqlBasicCall) sourceNode.getOperandList().get(0);
        List<SqlNode> operands = valuesNode.getOperandList();
        List<List<Object>> result = new ArrayList<>(operands.size());
        for (List<Object> parameterValues : parameterValueBatch) {
            result.add(getInsertValues(rootNode, parameterValues));
        }
        return result;
    }

    private static Object getOperandValue(SqlNode node) {
        if (SqlKind.DYNAMIC_PARAM == node.getKind()) {
            return DynamicParam.create(((SqlDynamicParam) node).getIndex());
        }
        if (SqlKind.LITERAL != node.getKind()) {
            throw new IllegalArgumentException("Illegal operand kind: " + node.getKind());
        }
        SqlLiteral literal = (SqlLiteral) node;
        switch (literal.getTypeName().getFamily()) {
            case BOOLEAN:
                return literal.booleanValue();
            case CHARACTER:
                return ((NlsString) literal.getValue()).getValue();
            case NULL:
                return null;
            case NUMERIC:
                return literal.bigDecimalValue().doubleValue();
            default: {
                throw new IllegalArgumentException("Unknown operand type: " + literal.getTypeName());
            }
        }
    }

    public static final class DynamicParam {

        private final int index;

        private DynamicParam(int index) {
            this.index = index;
        }

        private static DynamicParam create(int index) {
            return new DynamicParam(index);
        }

    }
}
