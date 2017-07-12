package com.axibase.tsd.driver.jdbc.enums;

public enum OnMissingMetricAction {
	WARNING,ERROR,NONE;

	public static OnMissingMetricAction fromString(String string) {
		for (OnMissingMetricAction action : OnMissingMetricAction.values()) {
			if (action.name().equalsIgnoreCase(string)) {
				return action;
			}
		}
		return null;
	}
}
