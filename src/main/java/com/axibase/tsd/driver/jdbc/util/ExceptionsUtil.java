package com.axibase.tsd.driver.jdbc.util;

import com.axibase.tsd.driver.jdbc.ext.AtsdMetricNotFoundException;
import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import java.sql.SQLDataException;
import java.sql.SQLFeatureNotSupportedException;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;

public class ExceptionsUtil {
	private ExceptionsUtil() {}

	public static SQLException unboxException(SQLException exception) {
		final Throwable cause = exception.getCause();
		if (cause instanceof SQLDataException) {
			return (SQLDataException) cause;
		} else if (cause instanceof SQLFeatureNotSupportedException) {
			return (SQLFeatureNotSupportedException) cause;
		} else if (!(cause instanceof RuntimeException)) {
			return exception;
		}
		Throwable finalCause = exception;
		if (cause instanceof AtsdRuntimeException) {
			Throwable inner = cause.getCause();
			if (inner instanceof SQLException) { // parsed result from ATSD
				finalCause = cause;
			}
		}

		if (isMetricNotFoundException(cause.getMessage())) {
			return new AtsdMetricNotFoundException(exception.getMessage(), finalCause);
		} else {
			return new SQLException(exception.getMessage(), finalCause);
		}
	}

	public static boolean isMetricNotFoundException(String message) {
		return StringUtils.startsWith(message, "Metric ") && StringUtils.endsWith(message, "not found");
	}
}
