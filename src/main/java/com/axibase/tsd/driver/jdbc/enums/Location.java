package com.axibase.tsd.driver.jdbc.enums;

import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import lombok.Getter;

public enum Location {
	SQL_ENDPOINT("/api/sql?datetimeAsNumber=true"),
	SQL_META_ENDPOINT("/api/sql/meta"),
	CANCEL_ENDPOINT("/api/sql/cancel"),
	METRICS_ENDPOINT("/api/v1/metrics"),
	VERSION_ENDPOINT("/api/v1/version"),
	COMMAND_ENDPOINT("/api/v1/command");

	@Getter
	private final String endpoint;

	Location(String endpoint) {
		this.endpoint = endpoint;
	}

	public String getUrl(AtsdConnectionInfo info) {
		return info.protocol() + info.host() + endpoint;
	}
}
