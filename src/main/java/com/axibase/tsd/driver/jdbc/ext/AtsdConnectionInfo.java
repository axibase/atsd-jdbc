package com.axibase.tsd.driver.jdbc.ext;

import java.util.Properties;

import com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties;

import static com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties.*;

public class AtsdConnectionInfo {
	private static final int MILLIS = 1000;

	private final Properties info;

	public AtsdConnectionInfo(Properties info) {
		this.info = info;
	}

	public String host() {
		return (String) info.get("host");
	}

	public String toEndpoint(String endpoint) {
		return host() + endpoint;
	}

	public String url() {
		return (String) info.get("url");
	}

	public String user() {
		return propertyOrEmpty("user");
	}

	public String password() {
		return propertyOrEmpty("password");
	}

	public boolean trustCertificate() {
		final AtsdDriverConnectionProperties property = trustServerCertificate;
		final String result = info.getProperty(property.camelName());
		return result == null ? (Boolean) property.defaultValue() : Boolean.parseBoolean(result);
	}

	public int connectTimeoutMillis() {
		final AtsdDriverConnectionProperties property = connectTimeout;
		final String result = info.getProperty(property.camelName());
		int timeout = result == null ? (Integer) property.defaultValue() : Integer.parseInt(result);
		return timeout * MILLIS;
	}

	public int readTimeoutMillis() {
		final AtsdDriverConnectionProperties property = readTimeout;
		final String result = info.getProperty(property.camelName());
		int timeout = result == null ? (Integer) property.defaultValue() : Integer.parseInt(result);
		return timeout * MILLIS;
	}

	public String strategy() {
		final AtsdDriverConnectionProperties property = strategy;
		final String result = info.getProperty(property.camelName());
		return result == null ? (String) property.defaultValue() : result;
	}

	public String tables() {
		final AtsdDriverConnectionProperties property = tables;
		return info.getProperty(property.camelName());
	}

	public String catalog() {
		final AtsdDriverConnectionProperties property = catalog;
		return info.getProperty(property.camelName());
	}

	public boolean expandTags() {
		final AtsdDriverConnectionProperties property = expandTags;
		final String result = info.getProperty(property.camelName());
		return result == null ? (Boolean) property.defaultValue() : Boolean.parseBoolean(result);
	}

	public boolean metaColumns() {
		final AtsdDriverConnectionProperties property = metaColumns;
		final String result = info.getProperty(property.camelName());
		return result == null ? (Boolean) property.defaultValue() : Boolean.parseBoolean(result);
	}

	public boolean assignColumnNames() {
		final AtsdDriverConnectionProperties property = assignColumnNames;
		final String result = info.getProperty(property.camelName());
		return result == null ? (Boolean) property.defaultValue() : Boolean.parseBoolean(result);
	}

	private String propertyOrEmpty(String key) {
		final String result = (String) info.get(key);
		return result == null ? "" : result;
	}
}
