package com.axibase.tsd.driver.jdbc.util;

import com.axibase.tsd.driver.jdbc.enums.JsonConvertedType;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonTypeResolver {
    public static JsonConvertedType resolve(String name) {
        switch (name) {
            case "tags":
            case "metric.tags":
            case "entity.tags":
                return JsonConvertedType.TAGS;
            default:
                return null;
        }
    }
}
