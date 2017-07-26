package com.axibase.tsd.driver.jdbc.enums;

import static com.axibase.tsd.driver.jdbc.enums.AtsdType.BOOLEAN_DATA_TYPE;
import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;
import com.axibase.tsd.driver.jdbc.util.AtsdColumn;
import lombok.Getter;

import static com.axibase.tsd.driver.jdbc.enums.AtsdType.STRING_DATA_TYPE;

@Getter
public enum EntityColumn implements MetadataColumnDefinition {

    ENABLED(AtsdColumn.ENTITY_ENABLED, BOOLEAN_DATA_TYPE),
    GROUPS(AtsdColumn.ENTITY_GROUPS, STRING_DATA_TYPE),
    INTERPOLATE(AtsdColumn.ENTITY_INTERPOLATE, STRING_DATA_TYPE),
    LABEL(AtsdColumn.ENTITY_LABEL, STRING_DATA_TYPE),
    TAGS(AtsdColumn.ENTITY_TAGS, STRING_DATA_TYPE),
    TIME_ZONE(AtsdColumn.ENTITY_TIME_ZONE, STRING_DATA_TYPE);

    private final String columnNamePrefix;
    private final AtsdType type;
    private final int nullable = 1;
    private final boolean metaColumn = true;

    EntityColumn(String prefix, AtsdType type) {
        this.columnNamePrefix = prefix;
        this.type = type;
    }

    @Override
    public String getNullableAsString() {
        return NULLABLE_AS_STRING[nullable];
    }

    @Override
    public AtsdType getType(AtsdType metricType) {
        return type;
    }

}
