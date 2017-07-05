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
package com.axibase.tsd.driver.jdbc.content;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.UUID;

import org.apache.calcite.avatica.Meta;

public class StatementContext {
	private SQLException exception;
	private SQLWarning warning;
	private String queryId;
	private int version;

	public StatementContext() {
		this.queryId = UUID.randomUUID().toString().substring(0, 10);
	}

	public StatementContext(Meta.StatementHandle statementHandle) {
		this.queryId = statementHandle.connectionId.substring(0, 8) + Integer.toHexString(statementHandle.id);
	}

	public StatementContext(SQLException exception, SQLWarning warning) {
		this();
		this.exception = exception;
		this.warning = warning;
	}

	public SQLException getException() {
		return exception;
	}

	public void setException(SQLException exception) {
		this.exception = exception;
	}

	public void addException(SQLException ex) {
		if (this.exception != null)
			this.exception.setNextException(ex);
		else
			setException(ex);
	}

	public SQLWarning getWarning() {
		return warning;
	}

	public void setWarning(SQLWarning warning) {
		this.warning = warning;
	}

	public void addWarning(SQLWarning warn) {
		if (this.warning != null)
			this.warning.setNextWarning(warn);
		else
			setWarning(warn);
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public String getQueryId() {
		return queryId;
	}

	public boolean isAbleToCancelAtsdQueries() {
		return true;
	}
}
