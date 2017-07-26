package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.util.CaseInsensitiveLinkedHashMap;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

@Setter
class CommandBuilder {

    private static final long MAX_TIME = 4291747200000l; //2106-01-01 00:00:00.000
    private static final String ENTITY = "entity";
    private static final String METRIC = "metric";
    private static final String SERIES = "series";

    private static final Set<String> DATA_TYPES = Collections.unmodifiableSet(Sets.newHashSet("short", "integer", "long", "float", "double", "decimal"));
    private static final Set<String> INVALID_ACTION_TYPES = Collections.unmodifiableSet(Sets.newHashSet( "none", "discard", "transform", "raise_error"));

    CommandBuilder() {
    }

    //series
    private String entity;
    private String dateTime;
    private String metricName;
    private Long time;

    //entity
    private Boolean entityEnabled;
    private String entityInterpolate;
    private String entityLabel;
    private String entityTimeZone;

    //metric
    private String metricDataType;
    private String metricDescription;
    private Boolean metricEnabled;
    private String metricFilter;
    private String metricInterpolate;
    private String metricInvalidValueAction;
    private String metricLabel;
    private Double metricMaxValue;
    private Double metricMinValue;
    private String metricTimeZone;
    private Boolean metricVersioning;
    private String metricUnits;

    private final Map<String, String> entityTags = new CaseInsensitiveLinkedHashMap<>();
    private final Map<String, String> metricTags = new CaseInsensitiveLinkedHashMap<>();
    private final Map<String, String> seriesTags = new CaseInsensitiveLinkedHashMap<>();
    private final Map<String, Double> seriesNumericValues = new CaseInsensitiveLinkedHashMap<>();
    private final Map<String, String> seriesTextValues = new CaseInsensitiveLinkedHashMap<>();

    public void addEntityTag(String name, String value) {
        addValue(entityTags, name, value);
    }

    public void addEntityTags(Map<String, String> tags) {
        entityTags.putAll(tags);
    }

    public void addMetricTag(String name, String value) {
        addValue(metricTags, name, value);
    }

    public void addMetricTags(Map<String, String> tags) {
        metricTags.putAll(tags);
    }

    public void addSeriesTag(String name, String value) {
        addValue(seriesTags, name, value);
    }

    public void addSeriesTags(Map<String, String> tags) {
        seriesTags.putAll(tags);
    }

    public void addSeriesValue(String name, Double value) {
        addValue(seriesNumericValues, name, value);
    }

    public void addSeriesValue(String name, String value) {
        addValue(seriesTextValues, name, value);
    }

    private <N,V> void addValue(Map<N, V> map, N name, V value) {
        if (name == null || value == null) {
            return;
        }
        map.put(name, value);
    }

    public List<String> buildCommands() {
        String command = buildSeriesCommand();
        List<String> result = new ArrayList<>(3);
        result.add(command);
        command = buildEntityCommand();
        if (command != null) {
            result.add(command);
        }
        command = buildMetricCommand();
        if (command != null) {
            result.add(command);
        }
        return result;
    }

    private String buildSeriesCommand() {
        validateSeriesData();
        StringBuilder buffer = new StringBuilder(SERIES);
        buffer.append(" e:").append(handleName(entity));
        if (time == null) {
            buffer.append(" d:").append(dateTime);
        } else {
            buffer.append(" ms:").append(time);
        }
        appendKeysAndValues(buffer, " t:", seriesTags);
        for (Map.Entry<String, Double> entry : seriesNumericValues.entrySet()){
            buffer.append(" m:").append(handleName(entry.getKey())).append('=').append(formatMetricValue(entry.getValue()));
        }
        appendKeysAndValues(buffer, " x:", seriesTextValues);
        return buffer.toString();
    }

    private String buildMetricCommand() {
        if (StringUtils.isBlank(metricName)) {
            throw new IllegalArgumentException("Metric not defined");
        }

        validateMetricData();
        StringBuilder buffer = new StringBuilder(METRIC);
        buffer.append(" m:").append(handleName(metricName));
        final int length = buffer.length();

        if (metricEnabled != null) {
            buffer.append(" b:").append(metricEnabled);
        }
        if (StringUtils.isNotBlank(metricLabel)) {
            buffer.append(" l:").append(handleStringValue(metricLabel));
        }
        if (StringUtils.isNotBlank(metricDescription)) {
            buffer.append(" d:").append(handleStringValue(metricDescription));
        }
        if (StringUtils.isNotEmpty(metricDataType)) {
            buffer.append(" p:").append(metricDataType);
        }
        if (StringUtils.isNotEmpty(metricInterpolate)) {
            buffer.append(" i:").append(metricInterpolate);
        }
        if (StringUtils.isNotEmpty(metricUnits)) {
            buffer.append(" u:").append(handleStringValue(metricUnits));
        }
        if (StringUtils.isNotBlank(metricFilter)) {
            buffer.append(" f:").append(handleStringValue(metricFilter));
        }
        if (StringUtils.isNotBlank(metricTimeZone)) {
            buffer.append(" z:").append(handleStringValue(metricTimeZone));
        }
        if (metricVersioning != null) {
            buffer.append(" v:").append(metricVersioning);
        }
        if (StringUtils.isNotBlank(metricInvalidValueAction)) {
            buffer.append(" a:").append(handleStringValue(metricInvalidValueAction));
        }
        if (metricMinValue != null) {
            buffer.append(" min:").append(formatMetricValue(metricMinValue));
        }
        if (metricMaxValue != null) {
            buffer.append(" max:").append(formatMetricValue(metricMaxValue));
        }
        appendKeysAndValues(buffer, " t:", metricTags);
        return  length == buffer.length() ? null : buffer.toString();
    }

    private String buildEntityCommand() {
        validateEntityCommand();
        StringBuilder buffer = new StringBuilder(ENTITY);
        buffer.append(" e:").append(handleName(entity));
        final int length = buffer.length();
        if (entityEnabled != null) {
            buffer.append(" b:").append(entityEnabled);
        }
        if (StringUtils.isNotEmpty(entityLabel)) {
            buffer.append(" l:").append(handleStringValue(entityLabel));
        }
        if (StringUtils.isNotEmpty(entityInterpolate)) {
            buffer.append(" i:").append(entityInterpolate);
        }
        if (StringUtils.isNotEmpty(entityTimeZone)) {
            buffer.append(" z:").append(handleStringValue(entityTimeZone));
        }
        appendKeysAndValues(buffer, " t:", entityTags);
        return length == buffer.length() ? null : buffer.toString();
    }

    private static String handleName(String key) {
        if (key.indexOf('"') != -1) {
            return '"' + key.replace("\"", "\"\"") + '"';
        } else if (key.indexOf('=') != -1 ) {
            return '"' + key + '"';
        } else {
            return key;
        }
    }

    private static void appendKeysAndValues(StringBuilder buffer, String prefix, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            buffer.append(prefix)
                    .append(handleName(entry.getKey()))
                    .append('=')
                    .append(handleStringValue(entry.getValue()));
        }
    }

    private static String handleStringValue(String value) {
        return StringUtils.containsAny(value, ' ', '"', '\n', '\t', '\r', '=') ? '"' + value.replace("\"", "\"\"") + '"' : value;
    }

    private static String formatMetricValue(double value) {
        if (Double.isNaN(value)) {
            return "NaN";
        }
        return Double.toString(value);
    }

    private void validateSeriesData() {
        if (StringUtils.isBlank(entity)) {
            throw new IllegalArgumentException("Entity not defined");
        }
        if (time == null && StringUtils.isBlank(dateTime)) {
            throw new IllegalArgumentException("Time and DateTime not defined");
        }
        if (time != null && (time < 0 || time > MAX_TIME)) {
            throw new IllegalArgumentException("Invalid time: " + time);
        }
        if (seriesNumericValues.isEmpty() && seriesTextValues.isEmpty()) {
            throw new IllegalArgumentException("Numeric and text values not defined");
        }
    }

    private void validateEntityCommand() {
        validateInterpolation(entityInterpolate, ENTITY);
    }

    private void validateMetricData() {
        validateInterpolation(metricInterpolate, METRIC);
        if (StringUtils.isNotEmpty(metricDataType) && !DATA_TYPES.contains(metricDataType)) {
            throw new IllegalArgumentException("Illegal metric data type: " + metricDataType);
        }
        if (StringUtils.isNotEmpty(metricInvalidValueAction) && !INVALID_ACTION_TYPES.contains(metricInvalidValueAction)) {
            throw new IllegalArgumentException("Illegal metric action: " + metricInvalidValueAction);
        }
    }

    private static void validateInterpolation(String interpolation, String commandType) {
        if (StringUtils.isNotEmpty(interpolation)
                && !"linear".equals(interpolation)
                && !"previous".equals(interpolation)) {
            throw new IllegalArgumentException("Illegal " + commandType + " interpolation: " + interpolation);
        }
    }

}
