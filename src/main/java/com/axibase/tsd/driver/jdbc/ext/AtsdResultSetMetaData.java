package com.axibase.tsd.driver.jdbc.ext;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class AtsdResultSetMetaData implements ResultSetMetaData {

	protected final AvaticaStatement statement;
	protected final Object query;
	protected final Meta.Signature signature;

	public AtsdResultSetMetaData(AvaticaStatement statement, Object query, Meta.Signature signature) {
		this.statement = statement;
		this.query = query;
		this.signature = signature;
	}

	public Meta.Signature getSignature() {
		return signature;
	}

	private ColumnMetaData getColumnMetaData(int column) {
		if (signature == null) {
			throw new IllegalStateException("Signature is null");
		} else if (signature.columns == null) {
			throw new IllegalStateException("Columns is null");
		}
		return this.signature.columns.get(column - 1);
	}

	public int getColumnCount() throws SQLException {
		if (signature == null) {
			throw new IllegalStateException("Signature is null");
		} else if (signature.columns == null) {
			throw new IllegalStateException("Columns is null");
		}
		return this.signature.columns.size();
	}

	public boolean isAutoIncrement(int column) throws SQLException {
		return this.getColumnMetaData(column).autoIncrement;
	}

	public boolean isCaseSensitive(int column) throws SQLException {
		return this.getColumnMetaData(column).caseSensitive;
	}

	public boolean isSearchable(int column) throws SQLException {
		return this.getColumnMetaData(column).searchable;
	}

	public boolean isCurrency(int column) throws SQLException {
		return this.getColumnMetaData(column).currency;
	}

	public int isNullable(int column) throws SQLException {
		return this.getColumnMetaData(column).nullable;
	}

	public boolean isSigned(int column) throws SQLException {
		return this.getColumnMetaData(column).signed;
	}

	public int getColumnDisplaySize(int column) throws SQLException {
		return this.getColumnMetaData(column).displaySize;
	}

	public String getColumnLabel(int column) throws SQLException {
		return this.getColumnMetaData(column).label;
	}

	public String getColumnName(int column) throws SQLException {
		return this.getColumnMetaData(column).columnName;
	}

	public String getSchemaName(int column) throws SQLException {
		return this.getColumnMetaData(column).schemaName;
	}

	public int getPrecision(int column) throws SQLException {
		return this.getColumnMetaData(column).precision;
	}

	public int getScale(int column) throws SQLException {
		return this.getColumnMetaData(column).scale;
	}

	public String getTableName(int column) throws SQLException {
		return this.getColumnMetaData(column).tableName;
	}

	public String getCatalogName(int column) throws SQLException {
		return this.getColumnMetaData(column).catalogName;
	}

	public int getColumnType(int column) throws SQLException {
		final ColumnMetaData columnMetaData = this.getColumnMetaData(column);
		if (columnMetaData instanceof AtsdMetaResultSets.AtsdColumnMetaData) {
			return ((AtsdMetaResultSets.AtsdColumnMetaData) columnMetaData).exposedType.id;
		}
		return columnMetaData.type.id;
	}

	public String getColumnTypeName(int column) throws SQLException {
		return this.getColumnMetaData(column).type.name;
	}

	public boolean isReadOnly(int column) throws SQLException {
		return this.getColumnMetaData(column).readOnly;
	}

	public boolean isWritable(int column) throws SQLException {
		return this.getColumnMetaData(column).writable;
	}

	public boolean isDefinitelyWritable(int column) throws SQLException {
		return this.getColumnMetaData(column).definitelyWritable;
	}

	public String getColumnClassName(int column) throws SQLException {
		return this.getColumnMetaData(column).columnClassName;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		if(iface.isInstance(this)) {
			return iface.cast(this);
		} else {
			throw this.statement.connection.helper.createException("does not implement '" + iface + "'");
		}
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return iface.isInstance(this);
	}
}
