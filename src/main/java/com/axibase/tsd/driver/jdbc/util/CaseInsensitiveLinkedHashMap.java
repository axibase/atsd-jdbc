package com.axibase.tsd.driver.jdbc.util;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

public class CaseInsensitiveLinkedHashMap<V> extends LinkedHashMap<String, V> {

    private final Set<String> keys = new HashSet<>();

    public V put(String key, V value) {
        if (notContainsIgnoreCase(key)) {
            return super.put(key, value);
        }
        return null;
    }

    private boolean notContainsIgnoreCase(String key) {
        return key == null ? false : keys.add(key.toLowerCase());
    }

}