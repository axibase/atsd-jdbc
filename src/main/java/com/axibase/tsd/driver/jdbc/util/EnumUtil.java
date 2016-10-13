package com.axibase.tsd.driver.jdbc.util;

import com.axibase.tsd.driver.jdbc.enums.*;
import com.axibase.tsd.driver.jdbc.enums.timedatesyntax.*;
import com.axibase.tsd.driver.jdbc.intf.ITimeDateConstant;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.*;


public class EnumUtil {

	private static final Set<String> reservedWordsSql2003 = createSetFromEnum(ReservedWordsSQL2003.values());
	private static final Map<String, AtsdType> atsdNameTypeMapping = createAtsdNameTypeMapping();
	private static final Map<String, AtsdType> columnPrefixAtsdTypeMapping = createColumnPrefixAtsdTypeMapping();
	private static final Map<String, ITimeDateConstant> tokenToTimeDateEnumConstant = initializeTimeDateMap();
	private static final Map<String, Strategy> strategyMap = EnumUtils.getEnumMap(Strategy.class);

	private EnumUtil() {}

	private static Map<String, AtsdType> createAtsdNameTypeMapping() {
		Map<String, AtsdType> mapping = new HashMap<>();
		for (AtsdType type : AtsdType.values()) {
			mapping.put(type.originalType, type);
		}
		return Collections.unmodifiableMap(mapping);
	}

	private static Map<String, AtsdType> createColumnPrefixAtsdTypeMapping() {
		Map<String, AtsdType> mapping = new HashMap<>();
		for (DefaultColumn type : DefaultColumn.values()) {
			mapping.put(type.getColumnNamePrefix(), type.getType());
		}
		return Collections.unmodifiableMap(mapping);
	}

	private static <T extends Enum<T>> Set<String> createSetFromEnum(T[] array ) {
		Set<String> set = new HashSet<>(array.length);
		for (T item : array) {
			set.add(item.name());
		}
		return Collections.unmodifiableSet(set);
	}

	private static boolean isTokenInSet(String token, Set<String> set) {
		return set.contains(token.toUpperCase(Locale.US));
	}

	public static boolean isReservedSqlToken(String token) {
		return isTokenInSet(token, reservedWordsSql2003);
	}

	public static AtsdType getAtsdTypeByOriginalName(String name) {
		AtsdType result = atsdNameTypeMapping.get(name);
		if (result == null) {
			result = AtsdType.STRING_DATA_TYPE; // use string type by default
		}
		return result;
	}

	public static AtsdType getAtsdTypeByColumnName(String columnName) {
		int dotIndex = columnName.indexOf('.');
		final String prefix = dotIndex == -1 ? columnName : columnName.substring(0, dotIndex);
		AtsdType type = columnPrefixAtsdTypeMapping.get(prefix);
		if (type == null) {
			type = AtsdType.STRING_DATA_TYPE;
		}
		return type;
	}

	private static ITimeDateConstant[] buildTimeConstantsArray() {
		EndTime[] endTimeValues = EndTime.values();
		IntervalUnit[] intervalUnitValues = IntervalUnit.values();

		int length = endTimeValues.length + intervalUnitValues.length;
		ITimeDateConstant[] all = new ITimeDateConstant[length];
		System.arraycopy(endTimeValues, 0, all, 0, endTimeValues.length);
		System.arraycopy(intervalUnitValues, 0, all, endTimeValues.length, intervalUnitValues.length);
		return all;
	}

	private static Map<String, ITimeDateConstant> initializeTimeDateMap() {
		Map<String, ITimeDateConstant> map = new HashMap<>();
		for (ITimeDateConstant timeConstant : buildTimeConstantsArray()) {
			map.put(timeConstant.value(), timeConstant);
		}
		for (ITimeDateConstant operator: ArithmeticOperator.values()) {
			map.put(operator.value(), operator);
		}
		return Collections.unmodifiableMap(map);
	}


	public static Strategy getStrategyByName(String name) {
		if (name != null) {
			final Strategy result = strategyMap.get(name.toUpperCase(Locale.US));
			if (result != null) {
				return result;
			}
		}
		return Strategy.STREAM;
	}

	public static ITimeDateConstant getTimeDateConstantByName(String token) {
		final String tokenLowerCase = token.toLowerCase(Locale.US);
		ITimeDateConstant result = tokenToTimeDateEnumConstant.get(tokenLowerCase);
		if (result == null) {
			try {
				result = new NumberConstant(Long.parseLong(token));
			} catch (NumberFormatException e) {
				result = new IsoDateFormat(token);
			}
		}
		return result;
	}

	private static String getPeriodReservedWords() {
		return "PERIOD,PREVIOUS,NEXT,LINEAR,EXTEND,START_TIME,END_TIME,FIRST_VALUE_TIME,CALENDAR";
	}

	public static String getSupportedTimeFunctions() {
		ITimeDateConstant[] timeFunctions = buildTimeConstantsArray();
		StringBuilder buffer = new StringBuilder(timeFunctions[0].toString());
		for (int i = 1; i < timeFunctions.length; ++i) {
			buffer.append(',').append(timeFunctions[i].toString());
		}
		buffer.append("LAST_TIME,DATE_FORMAT,");
		buffer.append(getPeriodReservedWords());
		return buffer.toString();
	}

	public static String getNumericFunctions() {
		return StringUtils.join(NumericFunctions.values(), ',');
	}

	public static String getStringFunctions() {
		return LexerTokens.REGEX.name();
	}

	public static String getSqlKeywords() {
		return LexerTokens.ROW_NUMBER.name();
	}

}
