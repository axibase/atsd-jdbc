package com.axibase.tsd.driver.jdbc.enums;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Date;

import com.axibase.tsd.driver.jdbc.intf.ParserRowContext;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.commons.lang3.StringUtils;

import static com.axibase.tsd.driver.jdbc.ext.AtsdMeta.TIMESTAMP_FORMATTER;
import static com.axibase.tsd.driver.jdbc.ext.AtsdMeta.TIMESTAMP_SHORT_FORMATTER;

public enum AtsdType {
	BIGINT_DATA_TYPE("bigint", "bigint", Types.BIGINT, Rep.LONG, 19, 20, 0) {
		@Override
		protected Object readValueHelper(String cell) {
			return Long.valueOf(cell);
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
				return readTimestampValue(cell);
			} catch (final ParseException e) {
				return readShortTimestampValue(cell);
			}
		}

		private Object readTimestampValue(String cell) throws ParseException {
			Date date = TIMESTAMP_FORMATTER.get().parse(cell);
			return new Timestamp(date.getTime());
		}

		private Object readShortTimestampValue(String cell) {
			Object value = null;
			try {
				final Date date = TIMESTAMP_SHORT_FORMATTER.get().parse(cell);
				value = new Timestamp(date.getTime());
			} catch (ParseException parseException) {
				if (log.isDebugEnabled()) {
					log.debug("[readShortTimestampValue] " + parseException.getMessage());
				}
			}
			return value;
		}
	};

	protected static final LoggingFacade log = LoggingFacade.getLogger(AtsdType.class);

	public final String originalType;
	public final String sqlType;
	public final int sqlTypeCode;
	public final Rep avaticaType;
	public final int maxPrecision;
	public final int size;
	public final int scale;

	AtsdType(String atsdType, String sqlType, int sqlTypeCode, Rep avaticaType, int maxPrecision, int size, int scale) {
		this.originalType = atsdType;
		this.sqlType = sqlType;
		this.sqlTypeCode = sqlTypeCode;
		this.avaticaType = avaticaType;
		this.maxPrecision = maxPrecision;
		this.size = size;
		this.scale = scale;
	}

	protected abstract Object readValueHelper(String cell);

	public Object readValue(String[] values, int index, boolean nullable, ParserRowContext context) {
		final String cell = values[index];
		if (StringUtils.isEmpty(cell)) {
			return this == AtsdType.STRING_DATA_TYPE && StringUtils.isNotEmpty(context.getColumnSource(index)) ? cell : null;
		}
		try {
			return readValueHelper(cell);
		} catch (NumberFormatException e) {
			if (log.isDebugEnabled()) {
				log.debug("[readValue] {} type mismatched: {} on {} position", sqlType, Arrays.toString(values), index);
			}
			return null;
		}
	}

	public String getLiteral(boolean isPrefix) {
		return null;
	}
}
