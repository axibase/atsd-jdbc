package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import lombok.experimental.UtilityClass;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.StringUtils;

import java.sql.Types;
import java.util.Comparator;

@UtilityClass
public class AtsdMetaResultSets {
	public static final int NUMBER_PRECISION_RADIX = 10;

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
		public final int numPrecRadix = NUMBER_PRECISION_RADIX;
		public final int nullable;
		public final String remarks = "";
		public final String columnDef = null;
		public final int sqlDataType;
		public final Integer sqlDatetimeSub = null;
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
				boolean odbcCompatible,
				String tableCat,
				String tableSchem,
				String tableName,
				String columnName,
				AtsdType atsdType,
				int nullable,
				int ordinalPosition,
				String isNullable) {
			this.tableCat = tableCat;
			this.tableSchem = tableSchem;
			this.tableName = tableName;
			this.columnName = columnName;
			this.dataType = atsdType.getTypeCode(odbcCompatible);
			this.sqlDataType = atsdType.sqlTypeCode;
			this.typeName = atsdType.sqlType;
			this.columnSize = atsdType.size;
			this.bufferLength = columnSize;
			this.decimalDigits = getDecimalDigits(sqlDataType);
			this.nullable = nullable;
			this.charOctetLength = Types.VARCHAR == sqlDataType ? columnSize : null;
			this.ordinalPosition = ordinalPosition;
			this.isNullable = isNullable;
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
		public final Integer numPrecRadix = NUMBER_PRECISION_RADIX;

		public AtsdMetaTypeInfo(boolean odbcCompatible, AtsdType atsdType, int nullable, int searchable, boolean unsignedAttribute, boolean fixedPrecScale,
								boolean autoIncrement, int minimumScale, int maximumScale) {
			this.typeName = atsdType.sqlType;
			this.dataType = atsdType.getTypeCode(odbcCompatible);
			this.sqlDataType = atsdType.sqlTypeCode;
			this.precision = atsdType.maxPrecision;
			this.literalPrefix = atsdType.getLiteral(true);
			this.literalSuffix = atsdType.getLiteral(false);
			this.nullable = (short) nullable;
			this.caseSensitive = cast(atsdType == AtsdType.STRING_DATA_TYPE);
			this.searchable = (short) searchable;
			this.unsignedAttribute = cast(unsignedAttribute);
			this.fixedPrecScale = cast(fixedPrecScale);
			this.autoIncrement = cast(autoIncrement);
			this.localTypeName = typeName;
			this.minimumScale = (short) minimumScale;
			this.maximumScale = (short) maximumScale;
		}

		public String getName() {
			return this.typeName;
		}
	}

	private static int cast(boolean value) {
		return value ? 1 : 0;
	}

	/**
	 * Comparator used in DatabaseMetadata.getTypeInfo method. Records are sorted by DATA_TYPE and then by how
	 * closely the data type maps to the corresponding JDBC SQL type.
	 */
	public static final Comparator<AtsdMetaTypeInfo> JDBC_TYPE_COMPARATOR = new Comparator<AtsdMetaTypeInfo>() {
		@Override
		public int compare(AtsdMetaTypeInfo left, AtsdMetaTypeInfo right) {
			int result = Integer.compare(left.dataType, right.dataType);
			if (result == 0) {
				result = Boolean.compare(left.dataType == left.sqlDataType, right.dataType == right.sqlDataType);
			}
			return result;
		}
	};

	/**
	 * Comparator used in DatabaseMetadata.getTables method. Records are sorted by:
	 * TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME.
	 * As TABLE_TYPE, TABLE_CAT, TABLE_SCHEM are always same, only TABLE_NAME is used as sorting key
	 */
	public static final Comparator<AtsdMetaTable> JDBC_TABLE_COMPARATOR = new Comparator<AtsdMetaTable>() {
		@Override
		public int compare(AtsdMetaTable left, AtsdMetaTable right) {
			return StringUtils.compare(left.tableName, right.tableName);
		}
	};

	/**
	 * Comparator used in DatabaseMetadata.getColumns method. Records are sorted by:
	 * TABLE_CAT, TABLE_SCHEM, TABLE_NAME, and ORDINAL_POSITION.
	 * As TABLE_CAT, TABLE_SCHEM are always same, only TABLE_NAME and ORDINAL_POSITION are used as sorting key
	 */
	public static final Comparator<AtsdMetaColumn> JDBC_COLUMN_COMPARATOR = new Comparator<AtsdMetaColumn>() {
		@Override
		public int compare(AtsdMetaColumn left, AtsdMetaColumn right) {
			int result = StringUtils.compare(left.tableName, right.tableName);
			if (result == 0) {
				result = Integer.compare(left.ordinalPosition, right.ordinalPosition);
			}
			return result;
		}
	};

}
