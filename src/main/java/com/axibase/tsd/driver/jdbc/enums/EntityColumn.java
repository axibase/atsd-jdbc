package com.axibase.tsd.driver.jdbc.enums;

import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;
import com.axibase.tsd.driver.jdbc.util.AtsdColumn;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;

import static com.axibase.tsd.driver.jdbc.enums.AtsdType.*;

@Getter
public enum EntityColumn implements MetadataColumnDefinition {
    CREATION_TIME(AtsdColumn.ENTITY_CREATION_TIME, BIGINT_DATA_TYPE),
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

    private static final int PREFIX_LENGTH = "entity.".length();

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

    public static EntityColumn[] values(boolean withCreationTime) {
        final EntityColumn[] values = EntityColumn.values();
        return withCreationTime ? values : ArrayUtils.removeElements(values, CREATION_TIME);
    }

}
