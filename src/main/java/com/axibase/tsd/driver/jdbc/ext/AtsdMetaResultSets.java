package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import java.sql.Types;
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
		public final Integer bufferLength;
		public final Integer decimalDigits;
		public final int numPrecRadix;
		public final int nullable;
		public final String remarks = "";
		public final String columnDef = null;
		public final int sqlDataType;
		public final int sqlDatetimeSub = 0;
		public final Integer charOctetLength;
		public final int ordinalPosition;
		public final String isNullable;
		public final String scopeCatalog = null;
		public final String scopeSchema = null;
		public final String scopeTable = null;
		public final Short sourceDataType = null;
		public final String isAutoincrement = "NO";
		public final String isGeneratedcolumn = "NO";

		public AtsdMetaColumn(
				String tableCat,
				String tableSchem,
				String tableName,
				String columnName,
				AtsdType atsdType,
				int numPrecRadix,
				int nullable,
				int ordinalPosition,
				String isNullable) {
			this.tableCat = tableCat;
			this.tableSchem = tableSchem;
			this.tableName = tableName;
			this.columnName = columnName;
			this.dataType = atsdType.typeCode;
			this.typeName = atsdType.sqlType;
			this.columnSize = atsdType.size;
			this.bufferLength = columnSize;
			this.decimalDigits = getDecimalDigits(dataType);
			this.numPrecRadix = numPrecRadix;
			this.nullable = nullable;
			this.charOctetLength = Types.VARCHAR == dataType ? columnSize : null;
			this.ordinalPosition = ordinalPosition;
			this.isNullable = isNullable;
			this.sqlDataType = atsdType.sqlTypeCode;
		}

		private Integer getDecimalDigits(int dataType) {
			switch (dataType) {
				case Types.BIGINT:
				case Types.INTEGER:
				case Types.SMALLINT: return 0;
				default: return null;
			}
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

		public String toString() {
			return "AtsdMetaTable {catalog= " + tableCat + ", schema=" + tableSchem + ", name=" + tableName + ", type=" + tableType + ", remarks=" + remarks + "}";
		}

	}

	public static class AtsdMetaTypeInfo implements MetaImpl.Named {
		@MetaImpl.ColumnNoNulls
		public final String typeName;
		public final int dataType;
		public final Integer precision;
		public final String literalPrefix;
		public final String literalSuffix;
		public final String createParams = null;
		public final short nullable;
		public final int caseSensitive;
		public final short searchable;
		public final int unsignedAttribute;
		public final int fixedPrecScale;
		public final int autoIncrement;
		public final String localTypeName;
		public final Short minimumScale;
		public final Short maximumScale;
		public final Integer sqlDataType;
		public final Integer sqlDatetimeSub = null;
		public final Integer numPrecRadix;

		public AtsdMetaTypeInfo(String typeName, int dataType, Integer precision, String literalPrefix, String literalSuffix, short nullable, boolean caseSensitive, short searchable, boolean unsignedAttribute, boolean fixedPrecScale, boolean autoIncrement, Short minimumScale, Short maximumScale, Integer numPrecRadix) {
			this.typeName = typeName;
			this.dataType = dataType;
			this.sqlDataType = dataType;
			this.precision = precision;
			this.literalPrefix = literalPrefix;
			this.literalSuffix = literalSuffix;
			this.nullable = nullable;
			this.caseSensitive = cast(caseSensitive);
			this.searchable = searchable;
			this.unsignedAttribute = cast(unsignedAttribute);
			this.fixedPrecScale = cast(fixedPrecScale);
			this.autoIncrement = cast(autoIncrement);
			this.localTypeName = typeName;
			this.minimumScale = minimumScale;
			this.maximumScale = maximumScale;
			this.numPrecRadix = numPrecRadix;
		}

		public String getName() {
			return this.typeName;
		}
	}

	private static int cast(boolean value) {
		return value ? 1 : 0;
	}
}
