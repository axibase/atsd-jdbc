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

import com.axibase.tsd.driver.jdbc.content.ContentDescription;
import com.axibase.tsd.driver.jdbc.content.json.Version;
import com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties;
import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.ext.AtsdFactory;
import com.axibase.tsd.driver.jdbc.ext.AtsdMeta;
import com.axibase.tsd.driver.jdbc.ext.AtsdVersion;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.protocol.ProtocolFactory;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import org.apache.calcite.avatica.*;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.StringTokenizer;

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
			logger.debug("[createDriverVersion] {}", e.getMessage());
		}
		return getDefaultDriverVersion();
	}

	private DriverVersion getDriverVersion(final Properties properties) {
		String driverName = properties.getProperty(DRIVER_NAME_KEY, JDBC_DRIVER_NAME);
		final DriverVersionParsed driverVersionParsed = new DriverVersionParsed(properties.getProperty(DRIVER_VERSION_KEY, JDBC_DRIVER_VERSION_DEFAULT));
		logger.debug("[createDriverVersion] {}", driverVersionParsed.versionString);
		String productName = properties.getProperty(PRODUCT_NAME_KEY, DATABASE_PRODUCT_NAME);
		String productVersion = properties.getProperty(PRODUCT_VERSION_KEY, DATABASE_PRODUCT_VERSION);
		int productVersionMajor = readIntProperty(properties, DATABASE_VERSION_MAJOR_KEY);
		int productVersionMinor = readIntProperty(properties, DATABASE_VERSION_MINOR_KEY);
		return new DriverVersion(driverName, driverVersionParsed.versionString, productName, productVersion, JDBC_COMPLIANT,
				driverVersionParsed.major, driverVersionParsed.minor, productVersionMajor, productVersionMinor);
	}

	private static int readIntProperty(Properties props, String key) {
		final String property = props.getProperty(key);
		return NumberUtils.isDigits(property) ? NumberUtils.toInt(property) : 0;
	}

	@Override
	protected String getConnectStringPrefix() {
		logger.debug("[getConnectStringPrefix]");
		return CONNECT_URL_PREFIX;
	}

	@Override
	protected Collection<ConnectionProperty> getConnectionProperties() {
		final ConnectionProperty[] array = AtsdDriverConnectionProperties.values();
		return Arrays.asList(array);
	}

	@Override
	public Meta createMeta(AvaticaConnection connection) {
		logger.debug("[createMeta] {}", connection.id);
		return new AtsdMeta(connection);
	}

	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (!acceptsURL(url)) {
			return null;
		}
		logger.debug("[connect] {}", url);
		final String urlSuffix = url.substring(CONNECT_URL_PREFIX.length());
		info.setProperty("url", urlSuffix);
		info.setProperty(AvaticaConnection.NUM_EXECUTE_RETRIES_KEY, RETRIES_NUMBER);

		final int afterSeparator = urlSuffix.indexOf(CONNECTION_STRING_PARAM_SEPARATOR) + 1;
		if (afterSeparator  < urlSuffix.length()) {
			info = ConnectStringParser.parse(urlSuffix.substring(afterSeparator), info);
		}
		final AtsdConnectionInfo atsdConnectionInfo = new AtsdConnectionInfo(info);
		if (atsdConnectionInfo.timestampTz()) {
			info.setProperty("timeZone", "UTC");
		}
		final AtsdVersion atsdVersion = getAtsdVersion(atsdConnectionInfo);
		if (atsdVersion.getRevision() < DriverConstants.MIN_SUPPORTED_ATSD_REVISION) {
			throw new SQLException("ATSD revision is not supported. Current revision: " + atsdVersion.getRevision()
					+ ". Minimum supported revision: " + DriverConstants.MIN_SUPPORTED_ATSD_REVISION);
		}
		final AtsdFactory atsdFactory = new AtsdFactory(atsdVersion);
		@SuppressWarnings("squid:S2095")
		final AvaticaConnection connection = atsdFactory.newConnection(this, atsdFactory, url, info);
		handler.onConnectionInit(connection);
		return connection;
	}

	private static AtsdVersion getAtsdVersion(AtsdConnectionInfo atsdConnectionInfo) throws SQLException {
		final String versionUrl = Location.VERSION_ENDPOINT.getUrl(atsdConnectionInfo);
		final ContentDescription contentDescription = new ContentDescription(versionUrl, atsdConnectionInfo);
		try (final IContentProtocol protocol = ProtocolFactory.create(SdkProtocolImpl.class, contentDescription)) {
			assert protocol != null;
			final InputStream databaseInfo = protocol.readInfo();
			final Version version = JsonMappingUtil.mapToVersion(databaseInfo);
			logger.trace("[getAtsdVersion] {}", version);
			final AtsdVersion atsdVersion = version.toAtsdVersion();
			if (logger.isDebugEnabled()) {
				logger.debug("[getAtsdVersion] edition: {}", atsdVersion.getEdition());
				logger.debug("[getAtsdVersion] revision: {}", atsdVersion.getRevision());
			}
			return atsdVersion;
		} catch (UnknownHostException e) {
			logger.debug(e.getMessage());
			throw new SQLException("Unknown host specified", e);
		} catch (final Exception e) {
			logger.debug(e.getMessage());
			throw new SQLException(e);
		}
	}

	@Override
	public boolean acceptsURL(String url) {
		logger.debug("[acceptsURL] {}", url);
		return url.startsWith(CONNECT_URL_PREFIX);
	}

	private DriverVersion getDefaultDriverVersion() {
		logger.debug("[getDefaultDriverVersion]");
		return new DriverVersion(JDBC_DRIVER_NAME, JDBC_DRIVER_VERSION_DEFAULT, DATABASE_PRODUCT_NAME,
				DATABASE_PRODUCT_VERSION, JDBC_COMPLIANT, DRIVER_VERSION_MAJOR_DEFAULT, DRIVER_VERSION_MINOR_DEFAULT, 1, 0);
	}

	private static class DriverVersionParsed {
		private final String versionString;
		private final int major;
		private final int minor;

		private DriverVersionParsed(String versionString) {
			this.versionString = versionString;
			final StringTokenizer stringTokenizer = new StringTokenizer(versionString, ".", false);
			this.major = stringTokenizer.hasMoreTokens() ? parseInt(stringTokenizer.nextToken()) : 0;
			this.minor = stringTokenizer.hasMoreTokens() ? parseInt(stringTokenizer.nextToken()) : 0;
		}

		private int parseInt(String number) {
			try {
				return Integer.parseInt(number);
			} catch (Exception e) {
				return 0;
			}
		}
	}

}
