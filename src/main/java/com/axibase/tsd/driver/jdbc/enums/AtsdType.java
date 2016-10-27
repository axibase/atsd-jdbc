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
	BIGINT_DATA_TYPE("bigint", "bigint", Types.BIGINT, Rep.LONG, 19, 10) {
		@Override
		protected Object readValueHelper(String cell) {
			return Long.valueOf(cell);
		}
	},
	DECIMAL_TYPE("decimal", "decimal", Types.DECIMAL, Rep.OBJECT, -1, 10) {
		@Override
		public Object readValueHelper(String values) {
			return new BigDecimal(values);
		}
	},
	DOUBLE_DATA_TYPE("double", "double", Types.DOUBLE, Rep.DOUBLE, 52, 10) {
		@Override
		protected Object readValueHelper(String cell) {
			return Double.valueOf(cell);
		}
	},
	FLOAT_DATA_TYPE("float", "float", Types.FLOAT, Rep.FLOAT, 23, 10) {
		@Override
		protected Object readValueHelper(String cell) {
			return Double.valueOf(cell);
		}
	},
	INTEGER_DATA_TYPE("integer", "integer", Types.INTEGER, Rep.INTEGER, 10, 10) {
		@Override
		protected Object readValueHelper(String cell) {
			return Integer.valueOf(cell);
		}
	},
	JAVA_OBJECT_TYPE("java_object", "java_object", Types.JAVA_OBJECT, Rep.OBJECT, 2147483647, 128 * 1024) {
		@Override
		public Object readValue(String[] values, int index, boolean nullable, ParserRowContext context) {
			final String cell = values[index];
			if (StringUtils.isEmpty(cell)) {
				return "";
			}
			final char firstCharacter = cell.charAt(0);
			if (!isNumberStart(firstCharacter) || firstCharacter == '"' || context.hasQuote(index)) {
				return cell;
			}
			return Double.valueOf(cell);
		}

		private boolean isNumberStart(char character) {
			return Character.isDigit(character) || character != 'N';
		}

		@Override
		protected Object readValueHelper(String cell) {
			return cell.startsWith("\"") ? cell : new BigDecimal(cell);
		}
	},
	LONG_DATA_TYPE("long", "bigint", Types.BIGINT, Rep.LONG, 19, 10) {
		@Override
		protected Object readValueHelper(String cell) {
			return Long.valueOf(cell);
		}
	},
	SHORT_DATA_TYPE("short", "smallint", Types.SMALLINT, Rep.SHORT, 5, 10) {
		@Override
		protected Object readValueHelper(String cell) {
			return Short.valueOf(cell);
		}
	},
	SMALLINT_DATA_TYPE("smallint", "smallint", Types.SMALLINT, Rep.SHORT, 5, 10) {
		@Override
		protected Object readValueHelper(String cell) {
			return Short.valueOf(cell);
		}
	},
	STRING_DATA_TYPE("string", "varchar", Types.VARCHAR, Rep.STRING, 2147483647, 128 * 1024) {
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
			"2016-01-01T00:00:00.000".length(), 10) {
		@Override
		public String getLiteral(boolean isPrefix) {
			return isPrefix ? "TIMESTAMP '" : "'";
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
				if (log.isDebugEnabled()) {
					log.debug("[readValue] " + e.getMessage());
				}
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

	AtsdType(String atsdType, String sqlType, int sqlTypeCode, Rep avaticaType, int maxPrecision, int size) {
		this.originalType = atsdType;
		this.sqlType = sqlType;
		this.sqlTypeCode = sqlTypeCode;
		this.avaticaType = avaticaType;
		this.maxPrecision = maxPrecision;
		this.size = size;
	}

	protected abstract Object readValueHelper(String cell);

	public Object readValue(String[] values, int index, boolean nullable, ParserRowContext context) {
		final String cell = values[index];
		if (StringUtils.isEmpty(cell)) {
			return this == AtsdType.STRING_DATA_TYPE && !nullable ? cell : null;
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
