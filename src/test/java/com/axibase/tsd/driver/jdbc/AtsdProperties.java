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

import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.DriverManager;

import static com.axibase.tsd.driver.jdbc.TestConstants.*;

public class AtsdProperties {
	private static final LoggingFacade logger = LoggingFacade.getLogger(AtsdProperties.class);
	protected static Boolean TRUST_URL;
	protected static String HTTP_ATSD_URL;
	protected static String JDBC_ATSD_URL;
	protected static String LOGIN_NAME;
	protected static String LOGIN_PASSWORD;
	protected static String READ_STRATEGY;
	protected static AtsdDriver driver;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String trustProp = System.getProperty("axibase.tsd.driver.jdbc.trust");
		TRUST_URL = trustProp != null ? Boolean.valueOf(trustProp) : null;
		LOGIN_NAME = System.getProperty("axibase.tsd.driver.jdbc.username");
		LOGIN_PASSWORD = System.getProperty("axibase.tsd.driver.jdbc.password");
		HTTP_ATSD_URL = System.getProperty("axibase.tsd.driver.jdbc.url");
		final StringBuilder sb = new StringBuilder(DriverConstants.CONNECT_URL_PREFIX).append(HTTP_ATSD_URL);
		if (TRUST_URL != null) {
			sb.append(TRUST_URL ? TRUST_PARAMETER_IN_QUERY : UNTRUST_PARAMETER_IN_QUERY);
		}
		READ_STRATEGY = System.getProperty("axibase.tsd.driver.jdbc.strategy");
		if (READ_STRATEGY != null) {
			if (TRUST_URL == null) {
				sb.append(PARAM_SEPARATOR);
			}
			sb.append(DriverConstants.STRATEGY_PARAM_NAME).append('=').append(READ_STRATEGY);
		}
		JDBC_ATSD_URL = sb.toString();
		driver = new AtsdDriver();
		Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");
		if (logger.isDebugEnabled()) {
			logger.debug("System properties has been set");
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		DriverManager.deregisterDriver(driver);
	}

}
