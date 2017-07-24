package com.axibase.tsd.driver.jdbc.enums;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.apache.calcite.avatica.ConnectionProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.calcite.avatica.ConnectionConfigImpl.parse;

public enum AtsdDriverConnectionProperties implements ConnectionProperty {
	secure(DriverConstants.SECURE_PARAM_NAME, DriverConstants.DEFAULT_SECURE_CONNECTION, Type.BOOLEAN),
	trust(DriverConstants.TRUST_PARAM_NAME, DriverConstants.DEFAULT_TRUST_SERVER_CERTIFICATE, Type.BOOLEAN),
	connectTimeout(DriverConstants.CONNECT_TIMEOUT_PARAM, DriverConstants.DEFAULT_CONNECT_TIMEOUT_VALUE, Type.NUMBER),
	readTimeout(DriverConstants.READ_TIMEOUT_PARAM, DriverConstants.DEFAULT_READ_TIMEOUT_VALUE, Type.NUMBER),
	strategy(DriverConstants.STRATEGY_PARAM_NAME, DriverConstants.DEFAULT_STRATEGY, Type.STRING),
	tables(DriverConstants.TABLES_PARAM_NAME, DriverConstants.DEFAULT_TABLES_VALUE, Type.STRING),
	expandTags(DriverConstants.EXPAND_TAGS_PARAM_NAME, DriverConstants.DEFAULT_EXPAND_TAGS_VALUE, Type.BOOLEAN),
	metaColumns(DriverConstants.META_COLUMNS_PARAM_NAME, DriverConstants.DEFAULT_META_COLUMNS_VALUE, Type.BOOLEAN),
	assignColumnNames(DriverConstants.ASSIGN_INNER_COLUMN_NAMES_PARAM, DriverConstants.DEFAULT_ASSIGN_INNER_COLUMN_NAMES_VALUE, Type.BOOLEAN),
	timestamptz(DriverConstants.USE_TIMESTAMP_WITH_TIME_ZONE_PARAM, DriverConstants.DEFAULT_USE_TIMESTAMP_WITH_TIME_ZONE_VALUE, Type.BOOLEAN),
	missingMetric(DriverConstants.ON_MISSING_METRIC_PARAM, DriverConstants.DEFAULT_ON_MISSING_METRIC_VALUE, Type.ENUM);

	private final String name;
	private final Object defaultValue;
	private final Type type;

	AtsdDriverConnectionProperties(String value, Object defaultValue, Type type) {
		this.name = value;
		this.defaultValue = defaultValue;
		this.type = type;
	}

	@Override
	public String camelName() {
		return name;
	}

	@Override
	public Object defaultValue() {
		return defaultValue;
	}

	@Override
	public Type type() {
		return type;
	}

	@Override
	public ConnectionConfigImpl.PropEnv wrap(Properties properties) {
		return new ConnectionConfigImpl.PropEnv(parse(properties, NAME_TO_PROPS), this);
	}

	@Override
	public boolean required() {
		return false;
	}

	@Override
	public Class valueClass() {
		return type.defaultValueClass();
	}

	private static final Map<String, ConnectionProperty> NAME_TO_PROPS = buildMap();

	private static Map<String, ConnectionProperty> buildMap() {
		AtsdDriverConnectionProperties[] values = AtsdDriverConnectionProperties.values();
		Map<String, ConnectionProperty> result = new HashMap<>(values.length * 2);
		for (AtsdDriverConnectionProperties property : values) {
			result.put(property.camelName(), property);
			result.put(property.name(), property);
		}
		return result;
	}

	public static Set<String> propertiesSet() {
		return NAME_TO_PROPS.keySet();
	}

}
