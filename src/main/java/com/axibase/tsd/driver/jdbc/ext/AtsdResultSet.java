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

import com.axibase.tsd.driver.jdbc.content.StatementContext;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.QueryState;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.TimeZone;

public class AtsdResultSet extends AvaticaResultSet {
	private static final LoggingFacade logger = LoggingFacade.getLogger(AtsdResultSet.class);
	private final AtsdMeta meta;
	private final Meta.StatementHandle handle;
	private final StatementContext context;

	public AtsdResultSet(AvaticaStatement statement, QueryState state, Signature signature,
						 ResultSetMetaData resultSetMetaData, TimeZone timeZone, Frame firstFrame) {
		super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
		final AtsdConnection connection = (AtsdConnection) statement.connection;
		this.meta = connection.getMeta();
		this.handle = statement.handle;
		this.context = meta.getContextFromMap(statement.handle);
		if (logger.isTraceEnabled()) {
			logger.trace("[ctor] " + this.handle.id);
		}
	}

	@Override
	public int getRow() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getRow]");
		}
		return super.getRow() + 1; // TODO remove when Avatica fixes row positions
	}

	@Override
	public boolean isFirst() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[isFirst]");
		}
		return this.getRow() == 1;
	}

	@Override
	public void afterLast() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[afterLast]");
		}
		super.afterLast();
	}

	@Override
	public boolean first() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[first]");
		}
		return super.first();
	}

	@Override
	public boolean last() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[last]");
		}
		return super.last();
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[isBeforeFirst]");
		}
		return this.getRow() < 1;
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[getWarnings]");
		}
		return context != null ? context.getWarning() : null;
	}

	@Override
	public void clearWarnings() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[clearWarnings]");
		}
		if (context != null) {
			context.setWarning(null);
		}
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[absolute]");
		}
		return super.absolute(row);
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[relative]");
		}
		return super.relative(rows);
	}

	@Override
	public boolean previous() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[previous]");
		}
		return super.previous();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[setFetchDirection]");
		}
		super.setFetchDirection(direction);
	}

	@Override
	public void setFetchSize(int fetchSize) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[setFetchSize]");
		}
		super.setFetchSize(fetchSize);
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateNull]");
		}
		super.updateNull(columnIndex);
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateBoolean]");
		}
		super.updateBoolean(columnIndex, x);
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateByte]");
		}
		super.updateByte(columnIndex, x);
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateShort]");
		}
		super.updateShort(columnIndex, x);
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateInt]");
		}
		super.updateInt(columnIndex, x);
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateLong]");
		}
		super.updateLong(columnIndex, x);
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateFloat]");
		}
		super.updateFloat(columnIndex, x);
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateDouble]");
		}
		super.updateDouble(columnIndex, x);
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateBigDecimal]");
		}
		super.updateBigDecimal(columnIndex, x);
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateString]");
		}
		super.updateString(columnIndex, x);
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateBytes]");
		}
		super.updateBytes(columnIndex, x);
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateDate]");
		}
		super.updateDate(columnIndex, x);
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateTime]");
		}
		super.updateTime(columnIndex, x);
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateTimestamp]");
		}
		super.updateTimestamp(columnIndex, x);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateAsciiStream]");
		}
		super.updateAsciiStream(columnIndex, x, length);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateBinaryStream]");
		}
		super.updateBinaryStream(columnIndex, x, length);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateCharacterStream]");
		}
		super.updateCharacterStream(columnIndex, x, length);
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateObject]");
		}
		super.updateObject(columnIndex, x, scaleOrLength);
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateObject]");
		}
		super.updateObject(columnIndex, x);
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateNull]");
		}
		super.updateNull(columnLabel);
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateBoolean]");
		}
		super.updateBoolean(columnLabel, x);
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateByte]");
		}
		super.updateByte(columnLabel, x);
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateShort]");
		}
		super.updateShort(columnLabel, x);
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateInt]");
		}
		super.updateInt(columnLabel, x);
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateLong]");
		}
		super.updateLong(columnLabel, x);
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateFloat]");
		}
		super.updateFloat(columnLabel, x);
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateDouble]");
		}
		super.updateDouble(columnLabel, x);
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateBigDecimal]");
		}
		super.updateBigDecimal(columnLabel, x);
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateString]");
		}
		super.updateString(columnLabel, x);
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateBytes]");
		}
		super.updateBytes(columnLabel, x);
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateDate]");
		}
		super.updateDate(columnLabel, x);
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateTime]");
		}
		super.updateTime(columnLabel, x);
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateTimestamp]");
		}
		super.updateTimestamp(columnLabel, x);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateAsciiStream]");
		}
		super.updateAsciiStream(columnLabel, x, length);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateBinaryStream]");
		}
		super.updateBinaryStream(columnLabel, x, length);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateCharacterStream]");
		}
		super.updateCharacterStream(columnLabel, reader, length);
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateObject]");
		}
		super.updateObject(columnLabel, x, scaleOrLength);
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateObject]");
		}
		super.updateObject(columnLabel, x);
	}

	@Override
	public void insertRow() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[insertRow]");
		}
		super.insertRow();
	}

	@Override
	public void updateRow() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[updateRow]");
		}
		super.updateRow();
	}

	@Override
	public void deleteRow() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[deleteRow]");
		}
		super.deleteRow();
	}

	@Override
	public void refreshRow() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[refreshRow]");
		}
		super.refreshRow();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		if (logger.isDebugEnabled()) {
			logger.debug("[cancelRowUpdates]");
		}
		super.cancelRowUpdates();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return super.getBigDecimal(columnIndex, 0);
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return super.getBigDecimal(columnLabel, 0);
	}

	@Override
	public boolean next() throws SQLException {
		final boolean next = super.next();
		if (!next) {
			meta.closeStatement(handle);
			if (context != null && context.getException() != null) {
				throw context.getException();
			}
		}
		return next;
	}

	@Override
	public void close() {
		super.close();
		if (context != null) {
			context.setWarning(null);
			context.setException(null);
		}
		if (logger.isTraceEnabled()) {
			logger.trace("[AtsdResultSet#closed]");
		}

	}

	@Override
	protected void cancel() {
		super.cancel();
		if (logger.isTraceEnabled()) {
			logger.trace("[AtsdResultSet#cancel]");
		}
	}

}
