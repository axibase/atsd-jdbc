package com.axibase.tsd.driver.jdbc.enums;

import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;
import com.axibase.tsd.driver.jdbc.util.AtsdColumn;
import lombok.Getter;

import static com.axibase.tsd.driver.jdbc.enums.AtsdType.*;

@Getter
public enum MetricColumn implements MetadataColumnDefinition {
    DATA_TYPE(AtsdColumn.METRIC_DATA_TYPE, STRING_DATA_TYPE),
    DESCRIPTION(AtsdColumn.METRIC_DESCRIPTION, STRING_DATA_TYPE),
    ENABLED(AtsdColumn.METRIC_ENABLED, BOOLEAN_DATA_TYPE),
    FILTER(AtsdColumn.METRIC_FILTER, STRING_DATA_TYPE),
    INTERPOLATE(AtsdColumn.METRIC_INTERPOLATE, STRING_DATA_TYPE),
    INVALID_VALUE_ACTION(AtsdColumn.METRIC_INVALID_VALUE_ACTION, STRING_DATA_TYPE),
    LABEL(AtsdColumn.METRIC_LABEL, STRING_DATA_TYPE),
    LAST_INSERT_TIME(AtsdColumn.METRIC_LAST_INSERT_TIME, STRING_DATA_TYPE),
    MAX_VALUE(AtsdColumn.METRIC_MAX_VALUE, FLOAT_DATA_TYPE),
    MIN_VALUE(AtsdColumn.METRIC_MIN_VALUE, FLOAT_DATA_TYPE),
    NAME(AtsdColumn.METRIC_NAME, STRING_DATA_TYPE),
    PERSISTENT(AtsdColumn.METRIC_PERSISTENT, BOOLEAN_DATA_TYPE),
    RETENTION_INTERVAL_DAYS(AtsdColumn.METRIC_RETENTION_INTERVAL_DAYS, INTEGER_DATA_TYPE),
    TAGS(AtsdColumn.METRIC_TAGS, STRING_DATA_TYPE),
    TIME_ZONE(AtsdColumn.METRIC_TIME_ZONE, STRING_DATA_TYPE),
    VERSIONING(AtsdColumn.METRIC_VERSIONING, BOOLEAN_DATA_TYPE),
    UNITS(AtsdColumn.METRIC_UNITS, STRING_DATA_TYPE);

    private final String columnNamePrefix;
    private final AtsdType type;
    private final int nullable = 1;
    private final boolean metaColumn = true;

    MetricColumn(String prefix, AtsdType type) {
        this.columnNamePrefix = prefix;
        this.type = type;
    }

    private static final int PREFIX_LENGTH = "metric.".length();

    @Override
    public String getNullableAsString() {
        return NULLABLE_AS_STRING[nullable];
    }

    @Override
    public AtsdType getType(AtsdType metricType) {
        return type;
    }

    @Override
    public String getShortColumnNamePrefix() {
        return columnNamePrefix.substring(PREFIX_LENGTH);
    }

}