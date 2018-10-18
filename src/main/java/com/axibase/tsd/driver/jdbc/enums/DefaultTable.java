package com.axibase.tsd.driver.jdbc.enums;

import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;

public enum DefaultTable {
    ATSD_SERIES("SELECT metric, entity, tags.collector, " +
            "tags.host, datetime, time, value FROM atsd_series WHERE metric = 'gc_time_percent' " +
            "AND entity = 'atsd' AND datetime >= now - 5*MINUTE ORDER BY datetime DESC LIMIT 10"),
    ATSD_METRIC(prepareRemark("atsd_metric", MetricColumn.values(), "metric = 'gc_time_percent'")),
    ATSD_ENTITY(prepareRemark("atsd_entity", EntityColumn.values(), "entity = 'atsd'"));

    public final String tableName = name().toLowerCase();

    public final String remark;

    DefaultTable(String remark) {
        this.remark = remark;
    }

    public static boolean isDefaultTable(String table) {
        return ATSD_SERIES.tableName.equalsIgnoreCase(table)
                || ATSD_ENTITY.tableName.equalsIgnoreCase(table)
                || ATSD_METRIC.tableName.equalsIgnoreCase(table);
    }

    private static String prepareRemark(String tableName, MetadataColumnDefinition[] columns, String where) {
        final StringBuilder buffer = new StringBuilder("SELECT ");
        for (MetadataColumnDefinition column : columns) {
            buffer.append(column.getColumnNamePrefix()).append(", ");
        }
        buffer.setLength(buffer.length() - 2);
        return buffer.append(" FROM ").append(tableName)
                .append(" WHERE ").append(where)
                .append(" LIMIT 1").toString();
    }
}
