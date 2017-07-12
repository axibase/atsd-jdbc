package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.util.CaseInsensitiveLinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

abstract class AbstractCommand {

    private static final long MAX_TIME = 4291747200000l; //2106-01-01 00:00:00.000

    private final String commandName;
    protected String entity;
    private String dateTime;
    private long time;
    protected final Map<String, String> tags = new CaseInsensitiveLinkedHashMap<>();

    AbstractCommand(String commandName) {
        this.commandName = commandName;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public void addTag(String name, String value) {
        if (name == null || value == null) {
            return;
        }
        tags.put(name, value);
    }

    public void addTags(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return;
        }
        tags.putAll(map);
    }

    protected void validate() {
        if (StringUtils.isBlank(entity)) {
            throw new IllegalArgumentException("Entity not defined");
        }
        if (time == 0 && StringUtils.isBlank(dateTime)) {
            throw new IllegalArgumentException("Time and DateTime not defined");
        }
        if (time != 0 && (time < 0 || time > MAX_TIME)) {
            throw new IllegalArgumentException("Invalid time: " + time);
        }
    }

    public String compose() {
        validate();
        StringBuilder buffer = new StringBuilder(commandName);
        buffer.append(" e:").append(handleName(entity));
        if (time == 0) {
            buffer.append(" d:").append(dateTime);
        } else {
            buffer.append(" ms:").append(time);
        }
        appendKeysAndValues(buffer, " t:", tags);
        appendValues(buffer);
        return buffer.append('\n').toString();
    }

    protected abstract void appendValues(StringBuilder buffer);

    protected static void appendKeysAndValues(StringBuilder buffer, String prefix, Map<String, String> map) {
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

    protected static String handleName(String key) {
        if (key.indexOf('"') != -1) {
            return '"' + key.replace("\"", "\"\"") + '"';
        } else if (key.indexOf('=') != -1 ) {
            return '"' + key + '"';
        } else {
            return key;
        }
    }

    protected static String handleStringValue(String value) {
        return '"' + value.replace("\"", "\"\"") + '"';
    }

    @Override
    public String toString() {
        return "commandName=" + commandName +
                ", entity=" + entity +
                ", dateTime=" + dateTime +
                ", time=" + time +
                ", tags=" + tags;
    }
}
