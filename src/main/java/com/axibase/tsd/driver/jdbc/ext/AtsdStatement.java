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

import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.ExceptionsUtil;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;

public class AtsdStatement extends AvaticaStatement {
	private static final LoggingFacade logger = LoggingFacade.getLogger(AtsdStatement.class);

	protected AtsdStatement(AvaticaConnection connection, StatementHandle statementHandle, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability) {
		super(connection, statementHandle, resultSetType, resultSetConcurrency, resultSetHoldability);
		logger.trace("[AtsdStatement#new] {}", this.handle.id);
	}

	protected AtsdStatement(AvaticaConnection connection, StatementHandle statementHandle, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability, Signature signature) {
		super(connection, statementHandle, resultSetType, resultSetConcurrency, resultSetHoldability, signature);
		logger.trace("[AtsdStatement#new] {}", this.handle.id);
	}

	@Override
	protected void executeInternal(String sql) throws SQLException {
		sql = StringUtils.stripStart(sql, null);
		try {
			super.executeInternal(sql);
		} catch (SQLException e) {
			throw ExceptionsUtil.unboxException(e);
		}
	}

	/*
		Method should close current result set and return true if another one can be fetched.
		As we always use one result set, always return false;
	*/
	@Override
	public boolean getMoreResults() throws SQLException {
		if (openResultSet != null) {
			openResultSet.close();
		}
		return false;
	}

	@Override
	public synchronized void cancel() throws SQLException {
		if (!this.cancelFlag.get()) {
			final AtsdConnection atsdConnection = (AtsdConnection) this.connection;
			final AtsdMeta meta = (AtsdMeta) atsdConnection.getMeta();
			meta.cancelStatement(this.handle);
		}

		super.cancel();
		logger.trace("[AtsdStatement#cancel]");
	}

	@Override
	public synchronized void close() throws SQLException {
		super.close();
		logger.trace("[AtsdStatement#close] {}", this.handle.id);
	}

    @Override
    public Meta.StatementType getStatementType() {
        return getSignature() == null ? null : getSignature().statementType;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return getStatementType() != Meta.StatementType.SELECT ? super.getUpdateCount() : -1;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return getStatementType() != Meta.StatementType.SELECT ? super.getLargeUpdateCount() : -1L;
    }

	String getSql() {
		return getSignature() == null ? null : getSignature().sql;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		sql = StringUtils.stripStart(sql, null);
		super.addBatch(sql);
	}

}
