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
import com.axibase.tsd.driver.jdbc.enums.DefaultColumn;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.ExceptionsUtil;
import com.axibase.tsd.driver.jdbc.util.TimeDateExpression;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

public class AtsdPreparedStatement extends AvaticaPreparedStatement {
	private static final LoggingFacade logger = LoggingFacade.getLogger(AtsdPreparedStatement.class);

	private final ConcurrentSkipListMap<Integer, TypedValue> parameters = new ConcurrentSkipListMap<>();

	boolean executed;

	protected AtsdPreparedStatement(AvaticaConnection connection, StatementHandle h, Signature signature,
									int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		super(connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability);
		logger.trace("[new] {}", this.handle.id);
	}

	@Override
	protected void executeInternal(String sql) throws SQLException {
		try {
			super.executeInternal(sql);
		} catch (SQLException e) {
			throw ExceptionsUtil.unboxException(e);
		}
	}

	@Override
	public void clearParameters() throws SQLException {
		super.clearParameters();
		parameters.clear();
	}

	@Override
	protected List<TypedValue> getParameterValues() {
		if (parameters.isEmpty()) {
			return Collections.emptyList();
		}
		if (parameters.lastKey() != parameters.size()) {
			throw new IndexOutOfBoundsException(
					String.format("Number of specified parameters [%d] is lower than the last key [%d]",
							parameters.size(), parameters.lastKey()));
		}
		final List<TypedValue> list = new ArrayList<>(parameters.values());
		if (logger.isDebugEnabled()) {
			for (TypedValue tv : list) {
				logger.debug("[TypedValue] {}", tv.value);
			}
		}
		return list;
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
	public int getUpdateCount() throws SQLException {
		return -1;
	}

	@Override
	public long getLargeUpdateCount() throws SQLException {
		return -1L;
	}

	@Override
	public synchronized void close() throws SQLException {
		super.close();
		logger.trace("[close] {}", this.handle.id);
	}

	@Override
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.OBJECT, null));
	}

	@Override
	public void setBoolean(int parameterIndex, boolean value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.BOOLEAN, value));
	}

	@Override
	public void setByte(int parameterIndex, byte value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.BYTE, value));
	}

	@Override
	public void setShort(int parameterIndex, short value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.SHORT, value));
	}

	@Override
	public void setInt(int parameterIndex, int value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.INTEGER, value));
	}

	@Override
	public void setLong(int parameterIndex, long value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.LONG, value));
	}

	@Override
	public void setFloat(int parameterIndex, float value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.FLOAT, value));
	}

	@Override
	public void setDouble(int parameterIndex, double value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.DOUBLE, value));
	}

	@Override
	public void setBigDecimal(int parameterIndex, BigDecimal value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.OBJECT, value));
	}

	@Override
	public void setString(int parameterIndex, String value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.STRING, value));
	}

	@Override
	public void setBytes(int parameterIndex, byte[] value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.STRING, value));
	}

	@Override
	public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setObject(int parameterIndex, Object value, int targetSqlType) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.STRING, value));
	}

	@Override
	public void setObject(int parameterIndex, Object value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.OBJECT, value));
	}

	@Override
	public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRef(int parameterIndex, Ref value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setBlob(int parameterIndex, Blob value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setClob(int parameterIndex, Clob value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setArray(int parameterIndex, Array value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDate(int parameterIndex, Date value, Calendar calendar) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.JAVA_SQL_DATE, value));
	}

	@Override
	public void setDate(int parameterIndex, Date value) throws SQLException {
		setDate(parameterIndex, value, getCalendar());
	}

	@Override
	public void setTime(int parameterIndex, Time value, Calendar calendar) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.JAVA_SQL_TIME, value));
	}

	@Override
	public void setTime(int parameterIndex, Time value) throws SQLException {
		setTime(parameterIndex, value, getCalendar());
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp value, Calendar calendar) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP, value));
	}

	@Override
	public void setTimestamp(int parameterIndex, Timestamp value) throws SQLException {
		setTimestamp(parameterIndex, value, getCalendar());
	}

	@Override
	public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setURL(int parameterIndex, URL value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setRowId(int parameterIndex, RowId value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.OBJECT, value));
	}

	@Override
	public void setNString(int parameterIndex, String value) throws SQLException {
		parameters.put(parameterIndex, TypedValue.ofSerial(ColumnMetaData.Rep.STRING, value));
	}

	@Override
	public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setAsciiStream(int parameterIndex, InputStream value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setBinaryStream(int parameterIndex, InputStream value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setClob(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}

	public void setTimeExpression(int parameterIndex, String value) throws SQLException {
		TimeDateExpression expression = new TimeDateExpression(value);
		setObject(parameterIndex, expression);
	}

	String getSql() {
		return getSignature() == null ? null : getSignature().sql;
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		sql = StringUtils.stripStart(sql, null);
		super.addBatch(sql);
	}

	@Override
	protected List<TypedValue> copyParameterValues() {
		List<TypedValue> current = getParameterValues();
		List<TypedValue> copy = new ArrayList<>(current.size());
		for (TypedValue value : current) {
			copy.add(value);
		}
		return copy;
	}

	@Override
	public Meta.StatementType getStatementType() {
		return getSignature() == null ? null : getSignature().statementType;
	}

	@Override
	public ResultSetMetaData getMetaData() {
		logger.debug("[getMetaData] executed: {} signature: {}", executed, getSignature());
		if (executed) {
			return super.getMetaData();
		} else if (getSignature() != null && getSignature().columns == null) {
			switch (getStatementType()) {
				case SELECT: {
					Pair<String, List<String>> pair = extractTableNameAndColumnNames(getSignature().sql);
					logger.debug("[getMetaData] tableName: {} columnNames: {}", pair.getKey(), pair.getValue());
					Signature signature = createNewSignature(getSignature(), pair.getKey(), pair.getValue());
					try {
						ResultSetMetaData result = getConnection().getFactory().newResultSetMetaData(this, signature);
						return result;
					} catch (SQLException exc) {
						throw new AtsdRuntimeException("", exc);
					}
				}
				default: throw new UnsupportedOperationException("Not yet implemented for statement type: " + getStatementType());
			}
		}

		return super.getMetaData();
	}

	public static Signature createNewSignature(final Signature input, String tableName, List<String> columnNames) {
		List<ColumnMetaData> columnMetaDataList = new ArrayList<>();
		int index = 0;
		for (String columnName : columnNames) {
			columnMetaDataList.add(createColumnMetaData(index++, tableName, columnName));
		}
		return new Signature(columnMetaDataList, input.sql, input.parameters, input.internalParameters, input.cursorFactory, input.statementType);
	}

	@Override
	public AtsdConnection getConnection() {
		return (AtsdConnection) connection;
	}

	private static Pair<String, List<String>> extractTableNameAndColumnNames(String sql) {
		sql = sql.toLowerCase();
		final int begin = StringUtils.indexOfIgnoreCase(sql, "select ") + 7;
		final int end = StringUtils.indexOfIgnoreCase(sql, " from ");
		String sqlColumns = sql.substring(begin, end);
		String[] names = StringUtils.split(sqlColumns, ',');
		List<String> columnNames = new ArrayList<>(names.length);
		for (String name : names) {
			columnNames.add(StringUtils.removeAll(name.trim(), "[\"'`]"));
		}
		String tail = sql.substring(end + 6);
		int spaceIndex = sql.indexOf(' ');
		final String tableName = StringUtils.removeAll(spaceIndex == -1 ? tail.trim() : sql.substring(0, spaceIndex), "[\"'`]");
		return new ImmutablePair<>(tableName, columnNames);
	}

	private static ColumnMetaData createColumnMetaData(int position, String tableName, String columnName) {
		DefaultColumn column = DefaultColumn.findByName(columnName);
		ColumnMetaData.Rep rep = column.getType().avaticaType;
		final AvaticaType avaticaType = new AvaticaType(column.getType().sqlTypeCode, column.getType().sqlType, rep);
		return new ColumnMetaData(position, false, false, false, false, column.getNullable(), false,
				column.getType().size, columnName, columnName, (String) null, 1,1, tableName, (String) null, avaticaType, true,
				false, false, rep.clazz.getName());
	}

}
