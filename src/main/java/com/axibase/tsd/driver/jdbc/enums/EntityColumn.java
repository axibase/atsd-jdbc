package com.axibase.tsd.driver.jdbc.enums;

import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;
import com.axibase.tsd.driver.jdbc.util.AtsdColumn;
import lombok.Getter;

import static com.axibase.tsd.driver.jdbc.enums.AtsdType.STRING_DATA_TYPE;

@Getter
public enum EntityColumn implements MetadataColumnDefinition {

    LABEL(AtsdColumn.ENTITY_LABEL),
    TIME_ZONE(AtsdColumn.ENTITY_TIME_ZONE),
    INTERPOLATE(AtsdColumn.ENTITY_INTERPOLATE),
    TAGS(AtsdColumn.ENTITY_TAGS),
    GROUPS(AtsdColumn.ENTITY_GROUPS);

    private final String columnNamePrefix;
    private final AtsdType type = STRING_DATA_TYPE;
    private final int nullable = 1;
    private final boolean metaColumn = true;

    EntityColumn(String prefix) {
        this.columnNamePrefix = prefix;
    }

    public String getNullableAsString() {
        return NULLABLE_AS_STRING[nullable];
    }

}
