package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.ext.AtsdMeta;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import lombok.Getter;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.util.ISO8601Utils;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.util.NlsString;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.axibase.tsd.driver.jdbc.DriverConstants.DEFAULT_TABLE_NAME;
import static com.axibase.tsd.driver.jdbc.util.AtsdColumn.*;

public abstract class AtsdSqlConverter<T extends SqlCall> {

    protected final LoggingFacade logger = LoggingFacade.getLogger(getClass());

    private static final Pattern DATETIME_ISO_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{3})?Z$");
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?$");
    private static final char TAGS_DELIMETER = ';';

    protected static final String PREFIX_ENTITY = "entity.";
    private static final String PREFIX_ENTITY_TAGS = ENTITY_TAGS + '.';
    protected static final String PREFIX_METRIC = "metric.";
    private static final String PREFIX_METRIC_TAGS = METRIC_TAGS + '.';
    protected static final String PREFIX_SERIES_TAGS = TAGS + '.';

    protected T rootNode;
    private final boolean timestampTz;

    @Getter
    private long[] commandCounts;

    protected AtsdSqlConverter(boolean timestampTz) {
        this.timestampTz = timestampTz;
    }

    public List<String> convertToCommands(String sql) throws SQLException {
        return convertToCommands(sql, null);
    }

    public List<String> convertToCommands(String sql, List<Object> parameterValues) throws SQLException {
        logger.debug("[convertToCommands] parameterCount: {}", getSize(parameterValues));
        try {
            sql = prepareSql(sql);
        } catch (RuntimeException e) {
            throw new SQLException("SQL prepare error: " + sql, e);
        }
        this.rootNode = parseSql(sql);
        final List<String> result = createCommands(parameterValues);
        logger.trace("[convertToCommands] result: {}", result);
        return result;
    }

    public List<String> convertBatchToCommands(String sql, List<List<Object>> parameterValuesBatch) throws SQLException {
        logger.debug("[convertBatchToCommands] batchSize: {}", getSize(parameterValuesBatch));
        try {
            sql = prepareSql(sql);
        } catch (RuntimeException e) {
            throw new SQLException("SQL prepare error: " + sql, e);
        }
        this.rootNode = parseSql(sql);
        final List<String> result = createCommandBatch(parameterValuesBatch);
        logger.trace("[convertBatchToCommands] result: {}", result);
        return result;
    }

    public abstract String prepareSql(String sql) throws SQLException;
    protected abstract String getTargetTableName();
    protected abstract List<String> getColumnNames();
    protected abstract List<Object> getColumnValues(List<Object> parameterValues);
    protected abstract List<List<Object>> getColumnValuesBatch(List<List<Object>> parameterValuesBatch);

    @SuppressWarnings("unchecked")
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

    private List<String> createCommands(List<Object> parameterValues) throws SQLException {
        logger.debug("[createCommands]");
        final String tableName = getTargetTableName();
        final List<String> columnNames = getColumnNames();
        logger.debug("[createCommands] tableName: {} columnCount: {}", tableName, columnNames.size());
        logger.trace("[createCommands] columnNames: {}", columnNames);
        final List<Object> values = getColumnValues(parameterValues);
        logger.trace("[createCommands] values: {}", values);
        List<String> result = DEFAULT_TABLE_NAME.equals(tableName) ? composeCommands(logger, columnNames, values, timestampTz)
                : composeCommands(logger, tableName, columnNames, values, timestampTz);
        commandCounts = new long[] {result.size()};
        return result;
    }

    private List<String> createCommandBatch(List<List<Object>> parameterValueBatch) throws SQLException {
        logger.debug("[createCommandBatch]");
        final String tableName = getTargetTableName();
        final List<String> columnNames = getColumnNames();
        logger.debug("[createCommandBatch] tableName: {} columnCount: {}", tableName, columnNames.size());
        logger.trace("[createCommandBatch] columnNames: {}", columnNames);
        final List<List<Object>> valueBatch = getColumnValuesBatch(parameterValueBatch);
        List<String> result = new ArrayList<>();
        List<String> commands;
        commandCounts = new long[valueBatch.size()];
        int idx = 0;
        if (DEFAULT_TABLE_NAME.equals(tableName)) {
            for (List<Object> values : valueBatch) {
                commands = composeCommands(logger, columnNames, values, timestampTz);
                commandCounts[idx++] = commands.size();
                result.addAll(commands);
            }
        } else {
            for (List<Object> values : valueBatch) {
                commands = composeCommands(logger, tableName, columnNames, values, timestampTz);
                commandCounts[idx++] = commands.size();
                result.addAll(commands);
            }
        }
        return result;
    }

    private static List<String> composeCommands(LoggingFacade logger, final String metricName, final List<String> columnNames, final List<Object> values,
                                                boolean timestampTz) throws SQLException {
        if (columnNames.size() != values.size()) {
            throw new IndexOutOfBoundsException(
                    String.format("Number of values [%d] does not match to number of columns [%d]",
                            values.size(), columnNames.size()));
        }
        final CommandBuilder commandBuilder = new CommandBuilder();
        commandBuilder.setMetricName(metricName);
        String columnName;
        Object value;
        for (int i = 0; i < columnNames.size(); i++) {
            columnName = columnNames.get(i);
            value = values.get(i);
            if (value == null) {
                continue;
            }

            switch (columnName) {
                case ENTITY:
                    commandBuilder.setEntity(value.toString());
                    break;
                case ENTITY_ENABLED :
                    commandBuilder.setEntityEnabled(validate(value, Boolean.class));
                    break;
                case ENTITY_GROUPS:
                    throw new SQLFeatureNotSupportedException(ENTITY_GROUPS);
                case ENTITY_INTERPOLATE:
                    commandBuilder.setEntityInterpolate(validate(value, String.class).toLowerCase());
                    break;
                case ENTITY_LABEL:
                    commandBuilder.setEntityLabel(validate(value, String.class));
                    break;
                case ENTITY_TAGS:
                    commandBuilder.addEntityTags(parseTags(value.toString()));
                    break;
                case ENTITY_TIME_ZONE:
                    commandBuilder.setEntityTimeZone(validate(value, String.class));
                    break;
                case TIME:
                    commandBuilder.setTime(validate(value, Number.class).longValue());
                    break;
                case DATETIME:
                    commandBuilder.setDateTime(validateDateTime(value, timestampTz));
                    break;
                case VALUE:
                    commandBuilder.addSeriesValue(metricName, validate(value, Number.class).doubleValue());
                    break;
                case TEXT:
                    commandBuilder.addSeriesValue(metricName, value.toString());
                    break;
                case TAGS:
                    commandBuilder.addSeriesTags(parseTags(value.toString()));
                    break;
                case METRIC_DATA_TYPE:
                    commandBuilder.setMetricDataType(validate(value, String.class).toLowerCase());
                    break;
                case METRIC_DESCRIPTION:
                    commandBuilder.setMetricDescription(validate(value, String.class));
                    break;
                case METRIC_ENABLED:
                    commandBuilder.setMetricEnabled(validate(value, Boolean.class));
                    break;
                case METRIC_FILTER:
                    commandBuilder.setMetricFilter(validate(value, String.class));
                    break;
                case METRIC_INTERPOLATE:
                    commandBuilder.setMetricInterpolate(validate(value, String.class));
                    break;
                case METRIC_INVALID_VALUE_ACTION:
                    commandBuilder.setMetricInvalidValueAction(validate(value, String.class));
                    break;
                case METRIC_LABEL:
                    commandBuilder.setMetricLabel(validate(value, String.class));
                    break;
                case METRIC_LAST_INSERT_TIME:
                    throw new SQLFeatureNotSupportedException(METRIC_LAST_INSERT_TIME);
                case METRIC_MAX_VALUE:
                    commandBuilder.setMetricMaxValue(validate(value, Number.class).doubleValue());
                    break;
                case METRIC_MIN_VALUE:
                    commandBuilder.setMetricMinValue(validate(value, Number.class).doubleValue());
                    break;
                case METRIC_NAME:
                    commandBuilder.setMetricName(validate(value, String.class));
                    break;
                case METRIC_PERSISTENT:
                    throw new SQLFeatureNotSupportedException(METRIC_PERSISTENT);
                case METRIC_RETENTION_INTERVAL_DAYS:
                    throw new SQLFeatureNotSupportedException(METRIC_RETENTION_INTERVAL_DAYS);
                case METRIC_TAGS:
                    commandBuilder.addMetricTags(parseTags(value.toString()));
                    break;
                case METRIC_TIME_PRECISION:
                    throw new SQLFeatureNotSupportedException(METRIC_TIME_PRECISION);
                case METRIC_TIME_ZONE:
                    commandBuilder.setMetricTimeZone(validate(value, String.class));
                    break;
                case METRIC_VERSIONING:
                    commandBuilder.setMetricVersioning(validate(value, Boolean.class));
                    break;
                case METRIC_UNITS:
                    commandBuilder.setMetricUnits(validate(value, String.class));
                    break;
                default: {
                    if (columnName.startsWith(PREFIX_SERIES_TAGS)) {
                        String tagName = columnName.substring(PREFIX_SERIES_TAGS.length());
                        commandBuilder.addSeriesTag(tagName, String.valueOf(value));
                    } else if (columnName.startsWith(PREFIX_ENTITY_TAGS)) {
                        String tagName = columnName.substring(PREFIX_ENTITY_TAGS.length());
                        commandBuilder.addEntityTag(tagName, String.valueOf(value));
                    } else if (columnName.startsWith(PREFIX_METRIC_TAGS)) {
                        String tagName = columnName.substring(PREFIX_METRIC_TAGS.length());
                        commandBuilder.addMetricTag(tagName, String.valueOf(value));
                    }
                }
            }
        }
        List<String> commands = commandBuilder.buildCommands();
        logger.trace("Commands: {}", commands);
        return commands;
    }

    private static List<String> composeCommands(LoggingFacade logger, final List<String> columnNames, final List<Object> values, boolean timestampTz)
            throws SQLDataException, SQLFeatureNotSupportedException {
        if (columnNames.size() != values.size()) {
            throw new IndexOutOfBoundsException(
                    String.format("Number of values [%d] does not match to number of columns [%d]",
                            values.size(), columnNames.size()));
        }
        final CommandBuilder commandBuilder = new CommandBuilder();
        String columnName;
        Object value;
        String metricName = null;
        Double metricValue = null;
        String metricText = null;
        for (int i = 0; i < columnNames.size(); i++) {
            columnName = columnNames.get(i);
            value = values.get(i);
            if (value == null) {
                continue;
            }

            switch (columnName) {
                case ENTITY:
                    commandBuilder.setEntity(validate(value, String.class));
                    break;
                case ENTITY_ENABLED :
                    commandBuilder.setEntityEnabled(validate(value, Boolean.class));
                    break;
                case ENTITY_GROUPS:
                    throw new SQLFeatureNotSupportedException(ENTITY_GROUPS);
                case ENTITY_INTERPOLATE:
                    commandBuilder.setEntityInterpolate(validate(value, String.class).toLowerCase());
                    break;
                case ENTITY_LABEL:
                    commandBuilder.setEntityLabel(validate(value, String.class));
                    break;
                case ENTITY_TAGS:
                    commandBuilder.addEntityTags(parseTags(value.toString()));
                    break;
                case ENTITY_TIME_ZONE:
                    commandBuilder.setEntityTimeZone(validate(value, String.class));
                    break;
                case TIME:
                    commandBuilder.setTime(validate(value, Number.class).longValue());
                    break;
                case DATETIME:
                    commandBuilder.setDateTime(validateDateTime(value, timestampTz));
                    break;
                case METRIC: {
                    String str = validate(value, String.class);
                    if (StringUtils.isNotBlank(str)) {
                        metricName = str;
                        commandBuilder.setMetricName(metricName);
                        if (metricValue != null) {
                            commandBuilder.addSeriesValue(metricName, metricValue);
                            metricValue = null;
                        }
                        if (metricText != null) {
                            commandBuilder.addSeriesValue(metricName, metricText);
                            metricText = null;
                        }
                    } else {
                        commandBuilder.addSeriesValue(columnName, str);
                    }
                    break;
                }
                case VALUE: {
                    if (metricName == null) {
                        metricValue = validate(value, Number.class).doubleValue();
                    } else {
                        commandBuilder.addSeriesValue(metricName, validate(value, Number.class).doubleValue());
                    }
                    break;
                }
                case TEXT: {
                    if (metricName == null) {
                        metricText = value.toString();
                    } else {
                        commandBuilder.addSeriesValue(metricName, value.toString());
                    }
                    break;
                }
                case TAGS:
                    commandBuilder.addSeriesTags(parseTags(value.toString()));
                    break;
                case METRIC_DATA_TYPE:
                    commandBuilder.setMetricDataType(validate(value, String.class).toLowerCase());
                    break;
                case METRIC_DESCRIPTION:
                    commandBuilder.setMetricDescription(validate(value, String.class));
                    break;
                case METRIC_ENABLED:
                    commandBuilder.setMetricEnabled(validate(value, Boolean.class));
                    break;
                case METRIC_FILTER:
                    commandBuilder.setMetricFilter(validate(value, String.class));
                    break;
                case METRIC_INTERPOLATE:
                    commandBuilder.setMetricInterpolate(validate(value, String.class));
                    break;
                case METRIC_INVALID_VALUE_ACTION:
                    commandBuilder.setMetricInvalidValueAction(validate(value, String.class));
                    break;
                case METRIC_LABEL:
                    commandBuilder.setMetricLabel(validate(value, String.class));
                    break;
                case METRIC_LAST_INSERT_TIME:
                    throw new SQLFeatureNotSupportedException(METRIC_LAST_INSERT_TIME);
                case METRIC_MAX_VALUE:
                    commandBuilder.setMetricMaxValue(validate(value, Number.class).doubleValue());
                    break;
                case METRIC_MIN_VALUE:
                    commandBuilder.setMetricMinValue(validate(value, Number.class).doubleValue());
                    break;
                case METRIC_NAME:
                    commandBuilder.setMetricName(validate(value, String.class));
                    break;
                case METRIC_PERSISTENT:
                    throw new SQLFeatureNotSupportedException(METRIC_PERSISTENT);
                case METRIC_RETENTION_INTERVAL_DAYS:
                    throw new SQLFeatureNotSupportedException(METRIC_RETENTION_INTERVAL_DAYS);
                case METRIC_TAGS:
                    commandBuilder.addMetricTags(parseTags(value.toString()));
                    break;
                case METRIC_TIME_PRECISION:
                    throw new SQLFeatureNotSupportedException(METRIC_TIME_PRECISION);
                case METRIC_TIME_ZONE:
                    commandBuilder.setMetricTimeZone(validate(value, String.class));
                    break;
                case METRIC_VERSIONING:
                    commandBuilder.setMetricVersioning(validate(value, Boolean.class));
                    break;
                case METRIC_UNITS:
                    commandBuilder.setMetricUnits(validate(value, String.class));
                    break;
                default: {
                    if (columnName.startsWith(PREFIX_SERIES_TAGS)) {
                        String tagName = columnName.substring(PREFIX_SERIES_TAGS.length());
                        commandBuilder.addSeriesTag(tagName, String.valueOf(value));
                    } else if (columnName.startsWith(PREFIX_ENTITY_TAGS)) {
                        String tagName = columnName.substring(PREFIX_ENTITY_TAGS.length());
                        commandBuilder.addEntityTag(tagName, String.valueOf(value));
                    } else if (columnName.startsWith(PREFIX_METRIC_TAGS)) {
                        String tagName = columnName.substring(PREFIX_METRIC_TAGS.length());
                        commandBuilder.addMetricTag(tagName, String.valueOf(value));
                    } else if (value instanceof Number) {
                        commandBuilder.setMetricName(columnName);
                        commandBuilder.addSeriesValue(columnName, validate(value, Number.class).doubleValue());
                    } else if (value instanceof String) {
                        commandBuilder.addSeriesValue(columnName, (String) value);
                    }
                }
            }
        }

        List<String> commands = commandBuilder.buildCommands();
        logger.trace("Commands: {}", commands);
        return commands;
    }

    private static int getSize(List list) {
        return list == null ? -1 : list.size();
    }

    protected static String getName(SqlIdentifier identifier) {
        if (identifier.isSimple()) {
            return identifier.getSimple();
        } else {
            return StringUtils.join(identifier.names, '.');
        }
    }

    protected static Object getOperandValue(SqlNode node, List<Object> parameterValues) {
        if (SqlKind.DYNAMIC_PARAM == node.getKind()) {
            if (parameterValues == null || parameterValues.isEmpty()) {
                throw new IllegalArgumentException("Parameter values: " + parameterValues);
            }
            return parameterValues.get(((SqlDynamicParam) node).getIndex());
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

    @SuppressWarnings("unchecked")
    private static <T> T validate(Object value, Class<T> targetClass) throws SQLDataException {
        if (targetClass.isInstance(value)) {
            return (T) value;
        }
        throw new SQLDataException("Invalid value: " + value + ". Current type: " + value.getClass().getSimpleName()
                + ", expected type: " + targetClass.getSimpleName());
    }

    private static String validateDateTime(Object value, boolean timestampTz) throws SQLDataException {
        if (value instanceof Number) {
            return AtsdMeta.TIMESTAMP_PRINTER.format(((Number) value).longValue());
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
                calendar.set(Calendar.ZONE_OFFSET, 0);
                calendar.set(Calendar.DST_OFFSET, 0);
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

    protected static void appendColumnName(final StringBuilder buffer, String name) {
        if (EnumUtil.isReservedSqlToken(name)) {
            buffer.append('\"').append(name.toLowerCase()).append('\"');
        } else {
            final String lcName = name.toLowerCase();
            if (lcName.startsWith(PREFIX_ENTITY)
                    || lcName.startsWith(PREFIX_METRIC)
                    || lcName.startsWith(PREFIX_SERIES_TAGS)) {
                buffer.append('\"').append(name).append('\"');
            } else{
                buffer.append(name);
            }
        }
    }

}
