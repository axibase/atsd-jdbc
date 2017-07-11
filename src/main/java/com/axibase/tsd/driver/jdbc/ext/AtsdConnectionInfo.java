package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

import static com.axibase.tsd.driver.jdbc.DriverConstants.CONNECTION_STRING_PARAM_SEPARATOR;
import static com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties.*;

public class AtsdConnectionInfo {
	private static final int MILLIS = 1000;

	private final Properties info;
	private final HostAndCatalog hostAndCatalog;
	private final Set<String> tables;

	public AtsdConnectionInfo(Properties info) {
		this.info = info;
		this.hostAndCatalog = new HostAndCatalog(StringUtils.substringBefore(url(), CONNECTION_STRING_PARAM_SEPARATOR));
		this.tables = getTables();
	}

	public String host() {
		return hostAndCatalog.host;
	}

	public String protocol() {
		return (secure() ? "https://" : "http://");
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

	public boolean secure() {
		final AtsdDriverConnectionProperties property = secure;
		final String result = info.getProperty(property.camelName());
		return result == null ? (Boolean) property.defaultValue() : Boolean.parseBoolean(result);
	}

	public boolean trustCertificate() {
		final AtsdDriverConnectionProperties property = trust;
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

	public Set<String> tables() {
		return tables;
	}

	public String schema() {
		return null;
	}

	public String catalog() {
		return hostAndCatalog.catalog;
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

	private Set<String> getTables() {
		final String value = info.getProperty(AtsdDriverConnectionProperties.tables.camelName());
		if (value == null) {
			return Collections.emptySet();
		}

		String[] tables = StringUtils.split(value, ',');
		Set<String> result = new LinkedHashSet<>();
		for (String table : tables) {
			table = StringUtils.trim(StringUtils.removeAll(table, "[\"']"));
			if (StringUtils.isNotEmpty(table)) {
				result.add(table);
			}
		}
		return result;
	}

	private static final class HostAndCatalog {
		private final char CATALOG_SEPARATOR = '/';

		private final String host;
		private final String catalog;

		private HostAndCatalog(String urlPrefix) {
			final int catalogSeparatorIndex = urlPrefix.indexOf(CATALOG_SEPARATOR);
			if (catalogSeparatorIndex < 0) {
				this.host = urlPrefix;
				this.catalog = null;
			} else {
				this.host = urlPrefix.substring(0, catalogSeparatorIndex);
				this.catalog = urlPrefix.substring(catalogSeparatorIndex + 1);
			}
		}
	}
}
