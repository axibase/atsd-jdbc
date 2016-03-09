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
package com.axibase.tsd.driver.jdbc.ext;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;

public class AtsdFactory implements AvaticaFactory {
	@SuppressWarnings("unused")
	private static final LoggingFacade logger = LoggingFacade.getLogger(AtsdFactory.class);

	private final int major;
	private final int minor;

	public AtsdFactory() {
		this(4, 1);
	}

	protected AtsdFactory(int major, int minor) {
		this.major = major;
		this.minor = minor;
	}

	@Override
	public int getJdbcMajorVersion() {
		return major;
	}

	@Override
	public int getJdbcMinorVersion() {
		return minor;
	}

	@Override
	public AvaticaDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
		return new AtsdDatabaseMetaData(connection);
	}

	@Override
	public AvaticaStatement newStatement(AvaticaConnection connection, Meta.StatementHandle h, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability) {
		return new AtsdStatement(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h,
			Meta.Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
					throws SQLException {
		return new AtsdPreparedStatement(connection, h, signature, resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	@Override
	public AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state, Meta.Signature signature,
			TimeZone timeZone, Meta.Frame firstFrame) {
		final ResultSetMetaData metaData = newResultSetMetaData(statement, signature);
		return new AtdsResultSet(statement, state, signature, metaData, timeZone, firstFrame);
	}

	@Override
	public AvaticaResultSetMetaData newResultSetMetaData(AvaticaStatement statement, Meta.Signature signature) {
		return new AvaticaResultSetMetaData(statement, null, signature);
	}

	@Override
	public AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory, String url,
			Properties info) throws SQLException {
		return new AtsdConnection(driver, factory, url, info);
	}
}
