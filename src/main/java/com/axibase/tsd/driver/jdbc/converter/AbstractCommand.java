package com.axibase.tsd.driver.jdbc.converter;

import com.axibase.tsd.driver.jdbc.util.CaseInsensitiveLinkedHashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

abstract class AbstractCommand {

    private final String commandName;
    protected String entity;
    private String dateTime;
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

    public void addTag(String name, String value) {
        if (name == null || value == null) {
            return;
        }
        tags.put(name, value);
    }

    protected void validate() {
        if (StringUtils.isBlank(entity)) {
            throw new IllegalArgumentException("Entity not defined");
        }
        if (StringUtils.isBlank(dateTime)) {
            throw new IllegalArgumentException("DateTime not defined");
        }
    }

    public String compose() {
        validate();
        StringBuilder sb = new StringBuilder(commandName);
        sb.append(" e:").append(handleName(entity));
        sb.append(" d:").append(dateTime);
        appendKeysAndValues(sb, " t:", tags);
        appendValues(sb);
        return sb.append('\n').toString();
    }

    protected abstract void appendValues(StringBuilder sb);

    protected static void appendKeysAndValues(StringBuilder sb, String prefix, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getValue() == null) {
                continue;
            }
            sb.append(prefix)
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

}
