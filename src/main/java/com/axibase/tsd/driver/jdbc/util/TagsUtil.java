package com.axibase.tsd.driver.jdbc.util;

import lombok.experimental.UtilityClass;

import java.util.Map;
import java.util.TreeMap;

@UtilityClass
public class TagsUtil {
	private static final char TAGS_DELIMITER = ';';

	public static String tagsToString(Map<String, String> tags) {
		if (tags == null || tags.isEmpty()) {
			return "";
		} else if (tags instanceof TreeMap) {
			return mapToString(tags, TAGS_DELIMITER);
		}
		return mapToString(new TreeMap<>(tags), TAGS_DELIMITER);
	}

	private static String mapToString(Map<String, String> map, char delimiter) {
		StringBuilder buffer = new StringBuilder();
		for (Map.Entry<String, String> entry : map.entrySet()) {
			if (buffer.length() > 0) {
				buffer.append(delimiter);
			}
			buffer.append(entry.getKey()).append('=').append(entry.getValue());
		}
		return buffer.toString();
	}

}
