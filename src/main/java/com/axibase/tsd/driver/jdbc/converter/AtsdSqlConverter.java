package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.ext.AtsdMeta;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.util.ISO8601Utils;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.StringUtils;

import static com.axibase.tsd.driver.jdbc.DriverConstants.DEFAULT_TABLE_NAME;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public abstract class AtsdSqlConverter<T extends SqlCall> {

    protected final LoggingFacade logger = LoggingFacade.getLogger(getClass());

    private static final Pattern DATETIME_ISO_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$");
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?$");
    private static final char TAGS_DELIMETER = ';';

    private static final String ENTITY = "entity";
    private static final String TIME = "time";
    private static final String DATETIME = "datetime";
    private static final String METRIC = "metric";
    protected static final String VALUE = "value";
    private static final String TEXT = "text";
    protected static final String TAGS = "tags";
    protected static final String PREFIX_TAGS = "tags.";

    protected T rootNode;
    private final boolean timestampTz;

    protected AtsdSqlConverter(boolean timestampTz) {
        this.timestampTz = timestampTz;
    }

    public String convertToCommand(String sql) throws SQLException {
        return convertToCommand(sql, null);
    }

    public String convertToCommand(String sql, List<Object> parameterValues) throws SQLException {
        logger.debug("[convertToCommand] parameterCount: {}", getSize(parameterValues));
        try {
            sql = prepareSql(sql);
        } catch (RuntimeException e) {
            throw new SQLException("SQL prepare error: " + sql, e);
        }
        this.rootNode = parseSql(sql);
        final String result = createSeriesCommand(parameterValues);
        logger.trace("[convertToCommand] result: {}", result);
        return result;
    }

    public String convertBatchToCommands(String sql, List<List<Object>> parameterValuesBatch) throws SQLException {
        logger.debug("[convertBatchToCommands] batchSize: {}", getSize(parameterValuesBatch));
        try {
            sql = prepareSql(sql);
        } catch (RuntimeException e) {
            throw new SQLException("SQL prepare error: " + sql, e);
        }
        this.rootNode = parseSql(sql);
        final String result =createSeriesCommandBatch(parameterValuesBatch);
        logger.trace("[convertBatchToCommands] result: {}", result);
        return result;
    }

    protected abstract String prepareSql(String sql) throws SQLException;
    protected abstract String getTargetTableName();
    protected abstract List<String> getColumnNames();
    protected abstract List<Object> getColumnValues(List<Object> parameterValues);
    protected abstract List<List<Object>> getColumnValuesBatch(List<List<Object>> parameterValuesBatch);

    private T parseSql(String sql) throws SQLException {
        logger.debug("[parseSql]");
        try {
            SqlParser sqlParser = SqlParser.create(sql, SqlParser.configBuilder()
                    .setParserFactory(SqlParserImpl.FACTORY)
                    .setUnquotedCasing(Casing.TO_LOWER)
                    .setQuoting(Quoting.DOUBLE_QUOTE)
                    .build());
            return (T) sqlParser.parseStmt();
        } catch (SqlParseException exc) {
            throw new SQLException("Could not parse sql: " + sql, exc);
        }
    }

    private String createSeriesCommand(List<Object> parameterValues) throws SQLDataException {
        logger.debug("[createSeriesCommand]");
        final String tableName = getTargetTableName();
        final List<String> columnNames = getColumnNames();
        logger.debug("[createSeriesCommand] tableName: {} columnCount: {}", tableName, columnNames.size());
        logger.trace("[createSeriesCommand] columnNames: {}", columnNames);
        final List<Object> values = getColumnValues(parameterValues);
        logger.trace("[createSeriesCommand] values: {}", values);
        return DEFAULT_TABLE_NAME.equals(tableName) ? composeSeriesCommand(logger, columnNames, values, timestampTz) : composeSeriesCommand(logger, tableName,
                columnNames, values, timestampTz);
    }

    private String createSeriesCommandBatch(List<List<Object>> parameterValueBatch) throws SQLDataException {
        logger.debug("[createSeriesCommandBatch]");
        final String tableName = getTargetTableName();
        final List<String> columnNames = getColumnNames();
        logger.debug("[createSeriesCommandBatch] tableName: {} columnCount: {}", tableName, columnNames.size());
        logger.trace("[createSeriesCommandBatch] columnNames: {}", columnNames);
        final List<List<Object>> valueBatch = getColumnValuesBatch(parameterValueBatch);
        StringBuilder buffer = new StringBuilder();
        if (DEFAULT_TABLE_NAME.equals(tableName)) {
            for (List<Object> values : valueBatch) {
                buffer.append(composeSeriesCommand(logger, columnNames, values, timestampTz));
            }
        } else {
            for (List<Object> values : valueBatch) {
                buffer.append(composeSeriesCommand(logger, tableName, columnNames, values, timestampTz));
            }
        }
        return buffer.toString();
    }

    private static String composeSeriesCommand(LoggingFacade logger, final String metricName, final List<String> columnNames, final List<Object> values,
                                               boolean timestampTz)
            throws SQLDataException {
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
            } else if (TIME.equalsIgnoreCase(columnName)) {
                command.setTime(validate(value, Number.class).longValue());
            } else if (DATETIME.equalsIgnoreCase(columnName)) {
                command.setDateTime(validateDateTime(value, timestampTz));
            } else if (VALUE.equalsIgnoreCase(columnName)) {
                command.addValue(metricName, validate(value, Number.class).doubleValue());
            } else if (TEXT.equalsIgnoreCase(columnName)) {
                command.addValue(metricName, value.toString());
            } else if (TAGS.equalsIgnoreCase(columnName)) {
                command.addTags(parseTags(value.toString()));
            } else if (columnName.startsWith(PREFIX_TAGS)) {
                String tagName = columnName.substring(PREFIX_TAGS.length());
                command.addTag(tagName, String.valueOf(value));
            }
        }
        logger.trace("Command: {}", command);
        return command.compose();
    }

    private static String composeSeriesCommand(LoggingFacade logger, final List<String> columnNames, final List<Object> values, boolean timestampTz) throws
            SQLDataException {
        if (columnNames.size() != values.size()) {
            throw new IndexOutOfBoundsException(
                    String.format("Number of values [%d] does not match to number of columns [%d]",
                            values.size(), columnNames.size()));
        }
        SeriesCommand command = new SeriesCommand();
        String columnName;
        Object value;
        String metricName = null;
        Double metricValue = null;
        String metricText = null;
        for (int i = 0; i<columnNames.size(); i++) {
            columnName = columnNames.get(i);
            value = values.get(i);
            if (value == null) {
                continue;
            }
            if (ENTITY.equalsIgnoreCase(columnName)) {
                command.setEntity(value.toString());
            } else if (TIME.equalsIgnoreCase(columnName)) {
                command.setTime(validate(value, Number.class).longValue());
            } else if (DATETIME.equalsIgnoreCase(columnName)) {
                command.setDateTime(validateDateTime(value, timestampTz));
            } else if (VALUE.equalsIgnoreCase(columnName)) {
                if (metricName == null) {
                    metricValue = validate(value, Number.class).doubleValue();
                } else {
                    command.addValue(metricName, validate(value, Number.class).doubleValue());
                }
            } else if (TEXT.equalsIgnoreCase(columnName)) {
                if (metricName == null) {
                    metricText = value.toString();
                } else {
                    command.addValue(metricName, value.toString());
                }
            } else if (TAGS.equalsIgnoreCase(columnName)) {
                command.addTags(parseTags(value.toString()));
            } else if (columnName.startsWith(PREFIX_TAGS) && value != null) {
                String tagName = columnName.substring(columnName.indexOf('.') + 1);
                command.addTag(tagName, value.toString());
            } else if (METRIC.equalsIgnoreCase(columnName) && value instanceof String && StringUtils.isNotBlank((String) value)) {
                metricName = (String) value;
                if (metricValue != null) {
                    command.addValue(metricName, metricValue);
                    metricValue = null;
                }
                if (metricText != null) {
                    command.addValue(metricName, metricText);
                    metricText = null;
                }
            } else if (value instanceof Number) {
                command.addValue(columnName, validate(value, Number.class).doubleValue());
            } else if (value instanceof String) {
                command.addValue(columnName, (String) value);
            }
        }
        logger.trace("Command: {}", command);
        return command.compose();
    }

    private static int getSize(List list) {
        return list == null ? -1 : list.size();
    }

    protected static String getName(SqlIdentifier identifier) {
        if (identifier.isSimple()) {
            return identifier.getSimple().toLowerCase();
        } else {
            return StringUtils.join(identifier.names, '.').toLowerCase();
        }
    }

    protected static Object getOperandValue(SqlNode node) {
        if (SqlKind.DYNAMIC_PARAM == node.getKind()) {
            return DynamicParam.create(((SqlDynamicParam) node).getIndex());
        }
        if (SqlKind.LITERAL != node.getKind()) {
            if (SqlKind.IDENTIFIER == node.getKind()) {
                SqlIdentifier identifier = (SqlIdentifier) node;
                if (identifier.isSimple() && "nan".equals(identifier.getSimple())) {
                    return Double.NaN;
                }
            }
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
                return literal.getValue();
            default: {
                throw new IllegalArgumentException("Unknown operand type: " + literal.getTypeName());
            }
        }
    }

    private static <T> T validate(Object value, Class<T> targetClass) throws SQLDataException {
        if (targetClass.isInstance(value)) {
            return (T) value;
        }
        throw new SQLDataException("Invalid value: " + value + ". Current type: " + value.getClass().getSimpleName()
                + ", expected type: " + targetClass.getSimpleName());
    }

    private static String validateDateTime(Object value, boolean timestampTz) throws SQLDataException {
        if (value instanceof Number) {
            return AtsdMeta.TIMESTAMP_FORMATTER.format(((Number) value).longValue());
        } else if (!(value instanceof String)) {
            throw new SQLDataException("Invalid value: " + value + ". Current type: " + value.getClass().getSimpleName()
                    + ", expected type: " + Timestamp.class.getSimpleName());
        }
        final String dateTime = value.toString();
        Matcher matcher = DATETIME_ISO_PATTERN.matcher(dateTime);
        if (matcher.matches()) {
            return dateTime;
        }
        matcher = TIMESTAMP_PATTERN.matcher(dateTime);
        if (matcher.matches()) {
            final Timestamp timestamp = Timestamp.valueOf(dateTime);
            final Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timestamp.getTime());
            if (timestampTz) {
                calendar.set(Calendar.ZONE_OFFSET, TimeZone.getTimeZone("UTC").getRawOffset());
            }
            return ISO8601Utils.format(calendar.getTime(), true);
        }
        throw new SQLDataException("Invalid datetime value: " + value + ". Expected formats: yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z', yyyy-MM-dd HH:mm:ss[.fffffffff]");
    }

    private static Map<String, String> parseTags(String value) throws SQLDataException {
        if (StringUtils.isBlank(value)) {
            return Collections.emptyMap();
        }

        String[] tags = StringUtils.split(value, TAGS_DELIMETER);
        Map<String, String> result = new LinkedHashMap<>();
        Pair<String, String> nameAndValue;
        for (String tag : tags) {
            nameAndValue = parseTag(StringUtils.trim(tag));
            if (nameAndValue != null) {
                result.put(nameAndValue.getKey(), nameAndValue.getValue());
            }
        }
        return result;
    }

    private static Pair<String, String> parseTag(String value) throws SQLDataException {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        final int idx = value.indexOf('=');
        if (idx < 1) {
            throw new SQLDataException("Invalid tags value: " + value);
        }

        String tagName = StringUtils.trim(value.substring(0, idx));
        String tagValue = StringUtils.trim(value.substring(idx + 1));

        return StringUtils.isBlank(tagValue) ? null : new ImmutablePair<>(tagName, tagValue);
    }

    public static final class DynamicParam {

        final int index;

        private DynamicParam(int index) {
            this.index = index;
        }

        private static DynamicParam create(int index) {
            return new DynamicParam(index);
        }

    }
}
