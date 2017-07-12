package com.axibase.tsd.driver.jdbc.ext;

import java.sql.SQLException;

public class AtsdMetricNotFoundException extends SQLException {
	public AtsdMetricNotFoundException(String reason, Throwable cause) {
		super(reason, cause);
	}
}
