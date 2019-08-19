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

import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import com.axibase.tsd.driver.jdbc.util.ExceptionsUtil;
import com.axibase.tsd.driver.jdbc.util.TagsUtil;
import com.axibase.tsd.driver.jdbc.util.TimeDateExpression;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.remote.TypedValue;

import java.io.InputStream;
import java.io.Reader;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

import static org.apache.calcite.avatica.Meta.StatementType.SELECT;

public class AtsdPreparedStatement extends AvaticaPreparedStatement {
	private static final LoggingFacade logger = LoggingFacade.getLogger(AtsdPreparedStatement.class);

	@Getter
	@Setter
	private boolean tagsEncoding;

	protected AtsdPreparedStatement(AvaticaConnection connection, StatementHandle h, Signature signature,
									int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		super(connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
		logger.trace("[new] handle id={}", this.handle.id);
	}

	@Override
	protected void executeInternal(String sql) throws SQLException {
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
		return super.getMoreResults();
	}

	@Override
	public synchronized void close() throws SQLException {
		super.close();
		logger.trace("[close] {}", this.handle.id);
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object value, int targetSqlType) throws SQLException {
		final ColumnMetaData.Rep rep = EnumUtil.getAtsdTypeBySqlType(targetSqlType, AtsdType.JAVA_OBJECT_TYPE).rep;
		getParameterValues().set(parameterIndex - 1, TypedValue.ofSerial(rep, value));
	}

	@Override
	public void setObject(int parameterIndex, Object value) throws SQLException {
		getParameterValues().set(parameterIndex - 1, TypedValue.ofSerial(ColumnMetaData.Rep.OBJECT, value));
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRef(int parameterIndex, Ref value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setBlob(int parameterIndex, Blob value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClob(int parameterIndex, Clob value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setArray(int parameterIndex, Array value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date value, Calendar calendar) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setDate(int parameterIndex, Date value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time value, Calendar calendar) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTime(int parameterIndex, Time value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp value, Calendar calendar) throws SQLException {
		super.setTimestamp(parameterIndex, value, calendar);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setURL(int parameterIndex, URL value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setRowId(int parameterIndex, RowId value) throws SQLException {
		getSite(parameterIndex).setRowId(value);
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		getSite(parameterIndex).setNString(value);
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setAsciiStream(int parameterIndex, InputStream value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBinaryStream(int parameterIndex, InputStream value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	public void setTimeExpression(int parameterIndex, String value) throws SQLException {
		TimeDateExpression expression = new TimeDateExpression(value);
		setObject(parameterIndex, expression);
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

	private AtsdConnection getAtsdConnection() {
		return (AtsdConnection) connection;
	}

	public void setTags(int parameterIndex, Map<String, String> tags) throws SQLException {
		setString(parameterIndex, TagsUtil.tagsToString(tags));
	}

	@Override
	@SneakyThrows(SQLException.class)
	public ResultSetMetaData getMetaData() {
		logger.debug("[getMetaData]");
		final ResultSetMetaData resultSetMetaData;
		if (super.openResultSet == null) { // if result set is not created, metadata is accessed before query execution
			if (getStatementType() == SELECT) {
				getAtsdConnection().getMeta().updatePreparedStatementResultSetMetaData(this.getSignature(), this.handle);
				resultSetMetaData = super.getMetaData();
			} else {
				resultSetMetaData = null;
			}
		} else {
			resultSetMetaData = openResultSet.getMetaData();
		}
		return resultSetMetaData;
	}

    @Override
    public void addBatch(String sql) throws SQLException {
		throw Helper.INSTANCE.unsupported();
    }

    @Override
	public void addBatch() throws SQLException {
		logger.debug("[addBatch]");
		super.addBatch();
	}

}
