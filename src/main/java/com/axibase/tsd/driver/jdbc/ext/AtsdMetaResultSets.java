package com.axibase.tsd.driver.jdbc.ext;

import org.apache.calcite.avatica.MetaImpl;

public class AtsdMetaResultSets {
	private AtsdMetaResultSets(){}

	public static class AtsdMetaColumn implements MetaImpl.Named {
		public final String tableCat;
		public final String tableSchem;
		public final String tableName;
		public final String columnName;
		public final int dataType;
		public final String typeName;
		public final int columnSize;
		public final Integer bufferLength = null;
		public final Integer decimalDigits;
		public final int numPrecRadix;
		public final int nullable;
		public final String remarks = null;
		public final String columnDef = null;
		public final int sqlDataType;
		public final String sqlDatetimeSub = null;
		public final int charOctetLength;
		public final int ordinalPosition;
		public final String isNullable;
		public final String scopeCatalog = null;
		public final String scopeSchema = null;
		public final String scopeTable = null;
		public final Short sourceDataType = null;
		public final String isAutoincrement = "";
		public final String isGeneratedcolumn = "";

		public AtsdMetaColumn(
				String tableCat,
				String tableSchem,
				String tableName,
				String columnName,
				int dataType,
				String typeName,
				int columnSize,
				Integer decimalDigits,
				int numPrecRadix,
				int nullable,
				int charOctetLength,
				int ordinalPosition,
				String isNullable) {
			this.tableCat = tableCat;
			this.tableSchem = tableSchem;
			this.tableName = tableName;
			this.columnName = columnName;
			this.dataType = dataType;
			this.typeName = typeName;
			this.columnSize = columnSize;
			this.decimalDigits = decimalDigits;
			this.numPrecRadix = numPrecRadix;
			this.nullable = nullable;
			this.charOctetLength = charOctetLength;
			this.ordinalPosition = ordinalPosition;
			this.isNullable = isNullable;
			this.sqlDataType = dataType;
		}

		@Override
		public String getName() {
			return columnName;
		}
	}

	public static class AtsdMetaTable implements MetaImpl.Named {
		public final String tableCat;
		public final String tableSchem;
		public final String tableName;
		public final String tableType;
		public final String remarks;
		public final String typeCat = null;
		public final String typeSchem = null;
		public final String typeName = null;
		public final String selfReferencingColName = null;
		public final String refGeneration = null;

		public AtsdMetaTable(String tableCat,
		                 String tableSchem,
		                 String tableName,
		                 String tableType,
		                 String remarks) {
			this.tableCat = tableCat;
			this.tableSchem = tableSchem;
			this.tableName = tableName;
			this.tableType = tableType;
			this.remarks = remarks;
		}

		public String getName() {
			return tableName;
		}
	}
}

