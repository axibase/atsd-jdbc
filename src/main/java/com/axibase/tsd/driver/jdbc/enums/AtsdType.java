package com.axibase.tsd.driver.jdbc.enums;

import com.axibase.tsd.driver.jdbc.intf.ParserRowContext;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.IsoDateParseUtil;
import com.axibase.tsd.driver.jdbc.util.TimestampParseUtil;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;

public enum AtsdType {
	BIGINT_DATA_TYPE("bigint", "bigint", Types.BIGINT, Rep.LONG, 19, 20, 0) {
		@Override
		protected Object readValueHelper(String cell) {
			return Long.valueOf(cell);
		}

		@Override
		protected Object readValueHelperFallback(String cell) {
			try {
				return "NaN".equals(cell) ? null : new BigDecimal(cell).longValue();
			} catch (Exception e) {
				return null;
			}
		}

		@Override
		public AtsdType getCompatibleType(boolean odbcCompatible) {
			return odbcCompatible ? DOUBLE_DATA_TYPE : this;
		}
	},
	BOOLEAN_DATA_TYPE("boolean", "boolean", Types.BOOLEAN, Rep.BOOLEAN, 1, 1, 0) {
		@Override
		protected Object readValueHelper(String cell) {
			return Boolean.valueOf(cell);
		}
	},
	DECIMAL_TYPE("decimal", "decimal", Types.DECIMAL, Rep.NUMBER, 0, 128 * 1024, 0) {
		@Override
		public Object readValueHelper(String values) {
			return new BigDecimal(values);
		}
	},
	DOUBLE_DATA_TYPE("double", "double", Types.DOUBLE, Rep.DOUBLE, 15, 25, 0) {
		@Override
		protected Object readValueHelper(String cell) {
			return Double.valueOf(cell);
		}
	},
	FLOAT_DATA_TYPE("float", "float", Types.REAL, Rep.FLOAT, 7, 15, 0) {
		@Override
		protected Object readValueHelper(String cell) {
			return Float.valueOf(cell);
		}
	},
	INTEGER_DATA_TYPE("integer", "integer", Types.INTEGER, Rep.INTEGER, 10, 11, 0) {
		@Override
		protected Object readValueHelper(String cell) {
			return Integer.valueOf(cell);
		}

		@Override
		protected Object readValueHelperFallback(String cell) {
			try {
				return "NaN".equals(cell) ? null : new BigDecimal(cell).intValue();
			} catch (Exception e) {
				return null;
			}
		}
	},
	JAVA_OBJECT_TYPE("java_object", "java_object", Types.JAVA_OBJECT, Rep.OBJECT, 2147483647, 128 * 1024, 0) {
		@Override
		public Object readValue(String[] values, int index, boolean nullable, ParserRowContext context) {
			final String cell = values[index];
			if (StringUtils.isEmpty(cell)) {
				return "";
			}
			final char firstCharacter = cell.charAt(0);
			if (!isNumberStart(firstCharacter) || context.hasQuote(index)) {
				return cell;
			}
			return Double.valueOf(cell);
		}

		private boolean isNumberStart(char character) {
			return Character.isDigit(character) || character == 'N';
		}

		@Override
		protected Object readValueHelper(String cell) {
			return cell.startsWith("\"") ? cell : new BigDecimal(cell);
		}
	},
	SMALLINT_DATA_TYPE("smallint", "smallint", Types.SMALLINT, Rep.SHORT, 5, 6, 0) {
		@Override
		protected Object readValueHelper(String cell) {
			return Short.valueOf(cell);
		}

		@Override
		protected Object readValueHelperFallback(String cell) {
			try {
				if (!"NaN".equals(cell)) {
					final int intValue = new BigDecimal(cell).intValue();
					return intValue > Short.MAX_VALUE ? Short.MAX_VALUE : Math.max(intValue, Short.MIN_VALUE);
				}
			} catch (Exception e) { //do nothing
			}
			return null;
		}
	},
	STRING_DATA_TYPE("string", "varchar", Types.VARCHAR, Rep.STRING, 128 * 1024, 128 * 1024, 0) {
		@Override
		public String getLiteral(boolean isPrefix) {
			return "'";
		}

		@Override
		protected Object readValueHelper(String cell) {
			return cell;
		}

		@Override
		public Object readValue(String[] values, int index, boolean nullable, ParserRowContext context) {
			final String cell = values[index];
			if (StringUtils.isEmpty(cell)) {
				return StringUtils.isNotEmpty(context.getColumnSource(index)) ? cell : null;
			}
			return cell;
		}
	},
	TIMESTAMP_DATA_TYPE("xsd:dateTimeStamp", "timestamp", Types.TIMESTAMP, Rep.JAVA_SQL_TIMESTAMP,
			"2016-01-01T00:00:00.000".length(), "2016-01-01T00:00:00.000".length(), 3) {
		@Override
		public String getLiteral(boolean isPrefix) {
			return "'";
		}

		@Override
		protected Object readValueHelper(String cell) {
			return null;
		}

		@Override
		public Object readValue(String[] values, int index, boolean nullable, ParserRowContext context) {
			String cell = values[index];
			if (StringUtils.isEmpty(cell)) {
				return null;
			}
			try {
				if (cell.charAt(cell.length() - 1) != 'Z') { // datetime is not in ISO format, hence datetimeAsNumber option used
					return TimestampParseUtil.parse(cell);
				}
				final long millis = IsoDateParseUtil.parseIso8601(cell);
				return new Timestamp(millis);
			} catch (Exception e) {
				log.debug("[readValue] {}", e.getMessage());
				return null;
			}
		}
	};

	protected static final LoggingFacade log = LoggingFacade.getLogger(AtsdType.class);
	private static final int TIMESTAMP_ODBC_TYPE = 11;
	public static final AtsdType DEFAULT_TYPE = AtsdType.STRING_DATA_TYPE;
	public static final AtsdType DEFAULT_VALUE_TYPE = AtsdType.FLOAT_DATA_TYPE;

	public final String originalType;
	public final String sqlType;
	public final int sqlTypeCode;
	public final Rep rep;
	public final int maxPrecision;
	public final int size;
	public final int scale;
	private final int odbcTypeCode;

	AtsdType(String atsdType, String sqlType, int sqlTypeCode, Rep rep, int maxPrecision, int size, int scale) {
		this.originalType = atsdType;
		this.sqlType = sqlType;
		this.sqlTypeCode = sqlTypeCode;
		this.odbcTypeCode = getOdbcTypeCode(sqlTypeCode);
		this.rep = rep;
		this.maxPrecision = maxPrecision;
		this.size = size;
		this.scale = scale;
	}

	protected abstract Object readValueHelper(String cell);

	protected Object readValueHelperFallback(String cell) {
		return null;
	}

	public Object readValue(String[] values, int index, boolean nullable, ParserRowContext context) {
		final String cell = values[index];
		if (StringUtils.isEmpty(cell)) {
			return null;
		}
		try {
			return readValueHelper(cell);
		} catch (NumberFormatException e) {
			if (log.isDebugEnabled()) {
				log.debug("[readValue] {} type mismatched: {} on {} position", sqlType, Arrays.toString(values), index);
			}
			return readValueHelperFallback(cell);
		}
	}

	public String getLiteral(boolean isPrefix) {
		return null;
	}

	private int getOdbcTypeCode(int sqlTypeCode) {
		return sqlTypeCode == Types.TIMESTAMP ? TIMESTAMP_ODBC_TYPE : sqlTypeCode;
	}

	public AtsdType getCompatibleType(boolean odbcCompatible) {
		return this;
	}

	public int getTypeCode(boolean odbcCompatible) {
		return odbcCompatible ? odbcTypeCode : sqlTypeCode;
	}

	public ColumnMetaData.AvaticaType getAvaticaType(boolean odbcCompatible) {
		return new ColumnMetaData.AvaticaType(getTypeCode(odbcCompatible), sqlType, rep);
	}

}
