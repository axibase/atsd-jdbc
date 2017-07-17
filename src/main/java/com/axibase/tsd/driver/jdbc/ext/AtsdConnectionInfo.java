package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties;
import com.axibase.tsd.driver.jdbc.enums.OnMissingMetricAction;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrTokenizer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.axibase.tsd.driver.jdbc.DriverConstants.CONNECTION_STRING_PARAM_SEPARATOR;
import static com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties.*;

public class AtsdConnectionInfo {
	private static final int MILLIS = 1000;

	private final Properties info;
	private final HostAndCatalog hostAndCatalog;
	private final List<String> tablePatterns;

	public AtsdConnectionInfo(Properties info) {
		this.info = info;
		this.hostAndCatalog = new HostAndCatalog(StringUtils.substringBefore(url(), CONNECTION_STRING_PARAM_SEPARATOR));
		this.tablePatterns = splitTablePatterns();
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
		return getBooleanValue(secure);
	}

	public boolean trustCertificate() {
		return getBooleanValue(trust);
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

	public List<String> tables() {
		return tablePatterns;
	}

	public String schema() {
		return null;
	}

	public String catalog() {
		return hostAndCatalog.catalog;
	}

	public boolean expandTags() {
		return getBooleanValue(expandTags);
	}

	public boolean metaColumns() {
		return getBooleanValue(metaColumns);
	}

	public boolean assignColumnNames() {
		return getBooleanValue(assignColumnNames);
	}

	public boolean timestampTz() {
		return getBooleanValue(timestamptz);
	}

	public OnMissingMetricAction missingMetric() {
		final AtsdDriverConnectionProperties property = missingMetric;
		final OnMissingMetricAction result = OnMissingMetricAction.fromString(info.getProperty(property.camelName()));
		return result == null ? (OnMissingMetricAction) property.defaultValue() : result;
	}

	private boolean getBooleanValue(AtsdDriverConnectionProperties property) {
		final String result = info.getProperty(property.camelName());
		return result == null ? (Boolean) property.defaultValue() : Boolean.parseBoolean(result);
	}

	private String propertyOrEmpty(String key) {
		final String result = (String) info.get(key);
		return result == null ? "" : result;
	}


	private List<String> splitTablePatterns() {
		final String value = info.getProperty(tables.camelName());
		if (value == null) {
			return Collections.emptyList();
		} else {
			return new StrTokenizer(value, ',', '"').getTokenList();
		}
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
