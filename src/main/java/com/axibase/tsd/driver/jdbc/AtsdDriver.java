/*
* Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* https://www.axibase.com/atsd/axibase-apache-2.0.pdf
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package com.axibase.tsd.driver.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.ext.AtsdDatabaseMetaData;
import com.axibase.tsd.driver.jdbc.ext.AtsdFactory;
import com.axibase.tsd.driver.jdbc.ext.AtsdMeta;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import org.apache.calcite.avatica.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import static com.axibase.tsd.driver.jdbc.DriverConstants.*;

public class AtsdDriver extends UnregisteredDriver {
	private static final LoggingFacade logger = LoggingFacade.getLogger(AtsdDriver.class);

	static {
		new AtsdDriver().register();
	}

	@Override
	protected DriverVersion createDriverVersion() {
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		try (final InputStream propertiesFromFile = classLoader.getResourceAsStream(DRIVER_PROPERTIES)) {
			if (propertiesFromFile != null) {
				final Properties properties = new Properties();
				properties.load(propertiesFromFile);
				return getDriverVersion(properties);
			}
		} catch (final IOException e) {
			if (logger.isDebugEnabled()) {
				logger.debug("[createDriverVersion] " + e.getMessage());
			}
		}
		return getDefaultDriverVersion();
	}

	private DriverVersion getDriverVersion(final Properties properties) {
		String driverName = properties.getProperty(DRIVER_NAME_KEY, JDBC_DRIVER_NAME);
		String driverVersion = properties.getProperty(DRIVER_VERSION_KEY, JDBC_DRIVER_VERSION_DEFAULT);
		String productName = properties.getProperty(PRODUCT_NAME_KEY, DATABASE_PRODUCT_NAME);
		String productVersion = properties.getProperty(PRODUCT_VERSION_KEY, DATABASE_PRODUCT_VERSION);
		String property = properties.getProperty(DATABASE_VERSION_MAJOR_KEY);
		int productVersionMajor = StringUtils.isNoneEmpty(property) ? NumberUtils.toInt(property) : 1;
		property = properties.getProperty(DATABASE_VERSION_MINOR_KEY);
		int productVersionMinor = StringUtils.isNoneEmpty(property) ? NumberUtils.toInt(property) : 0;
		property = properties.getProperty(DRIVER_VERSION_MAJOR_KEY);
		int driverVersionMajor = StringUtils.isNoneEmpty(property) ? NumberUtils.toInt(property)
				: DRIVER_VERSION_MAJOR_DEFAULT;
		property = properties.getProperty(DRIVER_VERSION_MINOR_KEY);
		int driverVersionMinor = StringUtils.isNoneEmpty(property) ? NumberUtils.toInt(property)
				: DRIVER_VERSION_MINOR_DEFAULT;
		if (logger.isDebugEnabled()) {
			logger.debug("[createDriverVersion] " + driverVersion);
		}
		return new DriverVersion(driverName, driverVersion, productName, productVersion, JDBC_COMPLIANT,
				driverVersionMajor, driverVersionMinor, productVersionMajor, productVersionMinor);
	}

	@Override
	protected String getConnectStringPrefix() {
		if (logger.isDebugEnabled()) {
			logger.debug("[getConnectStringPrefix]");
		}
		return CONNECT_URL_PREFIX;
	}

	@Override
	protected Collection<ConnectionProperty> getConnectionProperties() {
		final ConnectionProperty[] array = AtsdDriverConnectionProperties.values();
		return Arrays.asList(array);
	}

	@Override
	public Meta createMeta(AvaticaConnection connection) {
		if (logger.isDebugEnabled()) {
			logger.debug("[createMeta] " + connection.id);
		}
		return new AtsdMeta(connection);
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}
		if (logger.isDebugEnabled()) {
			logger.debug("[connect] " + url);
		}
		final String urlSuffix = url.substring(CONNECT_URL_PREFIX.length());
		final int firstSeparatorIndex = urlSuffix.indexOf(PARAM_SEPARATOR);
		final String sqlHost = firstSeparatorIndex < 0 ? urlSuffix : urlSuffix.substring(0, firstSeparatorIndex);

		info.setProperty("url", urlSuffix);
		info.setProperty("host", sqlHost);
		info.setProperty("schema", CONNECT_URL_PREFIX);
		info.setProperty(AvaticaConnection.NUM_EXECUTE_RETRIES_KEY, RETRIES_NUMBER);

		final int afterSeparator = urlSuffix.indexOf(PARAM_SEPARATOR) + 1;
		if (afterSeparator  < urlSuffix.length()) {
			info = ConnectStringParser.parse(urlSuffix.substring(afterSeparator), info);
		}

		final AtsdFactory atsdFactory = new AtsdFactory();
		final AvaticaConnection connection = atsdFactory.newConnection(this, atsdFactory, url, info);
		AtsdDatabaseMetaData atsdDatabaseMetaData = (AtsdDatabaseMetaData) connection.getMetaData();
		atsdDatabaseMetaData.initVersions(new AtsdConnectionInfo(info));
		if (atsdDatabaseMetaData.getDatabaseMajorVersion() < DriverConstants.MIN_SUPPORTED_ATSD_REVISION) {
			throw new SQLException("ATSD revision is not supported. Current revision: " + atsdDatabaseMetaData.getDatabaseMajorVersion()
					+ ". Minimum supported revision: " + DriverConstants.MIN_SUPPORTED_ATSD_REVISION);
		}
		handler.onConnectionInit(connection);
		return connection;
	}

	@Override
	public boolean acceptsURL(String url) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[acceptsURL] " + url);
		}
		return url.startsWith(CONNECT_URL_PREFIX);
	}

	private DriverVersion getDefaultDriverVersion() {
		if (logger.isDebugEnabled()) {
			logger.debug("[getDefaultDriverVersion]");
		}
		return new DriverVersion(JDBC_DRIVER_NAME, JDBC_DRIVER_VERSION_DEFAULT, DATABASE_PRODUCT_NAME,
				DATABASE_PRODUCT_VERSION, JDBC_COMPLIANT, DRIVER_VERSION_MAJOR_DEFAULT, DRIVER_VERSION_MINOR_DEFAULT, 1, 0);
	}

}
