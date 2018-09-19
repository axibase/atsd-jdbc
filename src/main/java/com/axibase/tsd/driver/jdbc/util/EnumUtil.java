package com.axibase.tsd.driver.jdbc.util;

import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.enums.ReservedWordsSQL2003;
import com.axibase.tsd.driver.jdbc.enums.Strategy;
import com.axibase.tsd.driver.jdbc.enums.timedatesyntax.*;
import com.axibase.tsd.driver.jdbc.intf.ITimeDateConstant;
import lombok.experimental.UtilityClass;
import org.apache.calcite.avatica.Meta;
import org.apache.commons.lang3.EnumUtils;

import javax.annotation.Nullable;
import java.util.*;

import static org.apache.calcite.avatica.Meta.StatementType.*;

@UtilityClass
public class EnumUtil {

	private static final Set<String> reservedWordsSql2003 = createSetFromEnum(ReservedWordsSQL2003.values());
	private static final Map<String, AtsdType> atsdNameTypeMapping = createAtsdNameTypeMapping();
	private static final Map<Integer, AtsdType> sqlAtsdTypesMaping = createSqlAtsdTypesMapping();
	private static final Map<String, ITimeDateConstant> tokenToTimeDateEnumConstant = initializeTimeDateMap();
	private static final Map<String, Strategy> strategyMap = EnumUtils.getEnumMap(Strategy.class);

	private static final Set<Meta.StatementType> SUPPORTED_STATEMENT_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(SELECT, INSERT, UPDATE)));

	private static Map<String, AtsdType> createAtsdNameTypeMapping() {
		Map<String, AtsdType> mapping = new HashMap<>();
		for (AtsdType type : AtsdType.values()) {
			mapping.put(type.originalType, type);
			mapping.put(type.originalType.toUpperCase(Locale.US), type);
		}
		return Collections.unmodifiableMap(mapping);
	}

	private static Map<Integer, AtsdType> createSqlAtsdTypesMapping() {
		Map<Integer, AtsdType> mapping = new HashMap<>();
		for (AtsdType type : AtsdType.values()) {
			mapping.put(type.sqlTypeCode, type);
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

	public static AtsdType getAtsdTypeWithPropertyUrlHint(String serverTypeName, @Nullable String propertyUrl) {
		if ("atsd:datetime".equals(propertyUrl)) {
			return AtsdType.TIMESTAMP_DATA_TYPE; // ATSD may return bigint for datetime column to eliminate parsing operation.
		}
		AtsdType result = atsdNameTypeMapping.get(serverTypeName);
		if (result == null) {
			result = AtsdType.DEFAULT_TYPE;
		}
		return result;
	}

	public static AtsdType getAtsdTypeBySqlType(int typeCode, AtsdType defaultType) {
		AtsdType result = sqlAtsdTypesMaping.get(typeCode);
		if (result == null) {
			result = defaultType;
		}
		return result;
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

	public static Meta.StatementType getStatementTypeByQuery(final String query) {
		if (query == null) {
			return SELECT;
		}
		final String queryKind = new StringTokenizer(query).nextToken().toUpperCase(Locale.US);
		try {
			final Meta.StatementType statementType = Meta.StatementType.valueOf(queryKind);
			if (SUPPORTED_STATEMENT_TYPES.contains(statementType)) {
				return statementType;
			}
		} catch (IllegalArgumentException exc) {
			// pass
		}
		throw new IllegalArgumentException("Unsupported statement type: " + queryKind);
	}

}
