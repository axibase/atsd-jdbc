package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.util.CaseInsensitiveLinkedHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

class CommandBuilder {

    private static final long MAX_TIME = 4291747200000l; //2106-01-01 00:00:00.000
    private static final String ENTITY = "entity";
    private static final String METRIC = "metric";
    private static final String SERIES = "series";

    CommandBuilder() {
    }

    @Setter
    private String entity;

    @Setter
    private String metricName;

    @Setter
    private Long time;

    @Setter
    private String dateTime;

    @Setter
    private String entityLabel;

    @Setter
    private String entityTimeZone;

    @Setter
    private String entityInterpolate;

    private final Map<String, String> entityTags = new CaseInsensitiveLinkedHashMap<>();
    private final Set<String> entityGroups = new LinkedHashSet<>();
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

    public void addEntityGroups(Collection<String> groups) {
        entityGroups.addAll(groups);
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
        if (metricTags.isEmpty()) {
            return null;
        }

        if (StringUtils.isBlank(metricName)) {
            throw new IllegalArgumentException("Metric not defined");
        }

        StringBuilder buffer = new StringBuilder(METRIC);
        buffer.append(" m:").append(handleName(metricName));
        appendKeysAndValues(buffer, " t:", metricTags);
        return buffer.toString();
    }

    private String buildEntityCommand() {
        validateEntityCommand();
        StringBuilder buffer = new StringBuilder(ENTITY);
        buffer.append(" e:").append(handleName(entity));
        final int length = buffer.length();
        if (StringUtils.isNotEmpty(entityLabel)) {
            buffer.append(" l:").append(handleName(entityLabel));
        }
        if (StringUtils.isNotEmpty(entityInterpolate)) {
            buffer.append(" i:").append(entityInterpolate);
        }
        if (StringUtils.isNotEmpty(entityTimeZone)) {
            buffer.append(" z:").append(entityTimeZone);
        }
        appendKeysAndValues(buffer, " t:", entityTags);
        if (buffer.length() == length) {
            return null;
        }
        return buffer.toString();
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
        return '"' + value.replace("\"", "\"\"") + '"';
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
        if (StringUtils.isNotEmpty(entityInterpolate)
                && !"linear".equals(entityInterpolate)
                && !"previous".equals(entityInterpolate)
                && !"none".equals(entityInterpolate)) {
            throw new IllegalArgumentException("Invalid entity interpolation: " + entityInterpolate);
        }
    }

}
