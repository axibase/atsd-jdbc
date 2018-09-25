package com.axibase.tsd.driver.jdbc;

import com.axibase.tsd.driver.jdbc.enums.OnMissingMetricAction;
import lombok.experimental.UtilityClass;

import java.nio.charset.Charset;

@UtilityClass
public final class DriverConstants {
	public static final String DATABASE_PRODUCT_NAME = "Axibase";
	public static final String DATABASE_PRODUCT_VERSION = "Axibase Time Series Database";
	public static final String JDBC_DRIVER_NAME = "ATSD JDBC driver";
	public static final String JDBC_DRIVER_VERSION_DEFAULT = "1.4.4";
	public static final int    DRIVER_VERSION_MAJOR_DEFAULT = 1;
	public static final int    DRIVER_VERSION_MINOR_DEFAULT = 3;

	public static final String PRODUCT_NAME_KEY = "product.name";
	public static final String PRODUCT_VERSION_KEY = "product.version";
	public static final String DRIVER_VERSION_KEY = "driver.version";
	public static final String DRIVER_NAME_KEY = "driver.name";
	public static final String DATABASE_VERSION_MAJOR_KEY = "database.version.major";
	public static final String DATABASE_VERSION_MINOR_KEY = "database.version.minor";
	public static final String DRIVER_VERSION_MAJOR_KEY = "driver.version.major";
	public static final String DRIVER_VERSION_MINOR_KEY = "driver.version.minor";
	public static final String DRIVER_PROPERTIES = "META-INF/axibase-atsd-jdbc.properties";
	public static final String RETRIES_NUMBER = "1";
	public static final boolean JDBC_COMPLIANT = false;
	public static final String CONNECTION_STRING_PARAM_SEPARATOR = ";";

	public static final String CONNECT_URL_PREFIX = "jdbc:atsd://";

	public static final String CONNECT_TIMEOUT_PARAM = "connectTimeout";
	public static final int    DEFAULT_CONNECT_TIMEOUT_VALUE = 5;
	public static final String READ_TIMEOUT_PARAM = "readTimeout";
	public static final int    DEFAULT_READ_TIMEOUT_VALUE = 0;
	public static final String SECURE_PARAM_NAME = "secure";
	public static final boolean DEFAULT_SECURE_CONNECTION = true;
	public static final String TRUST_PARAM_NAME = "trust";
	public static final boolean DEFAULT_TRUST_SERVER_CERTIFICATE = true;
	public static final String TABLES_PARAM_NAME = "tables";
	public static final String DEFAULT_TABLES_VALUE = "%";
	public static final String EXPAND_TAGS_PARAM_NAME = "expandTags";
	public static final boolean DEFAULT_EXPAND_TAGS_VALUE = false;
	public static final String META_COLUMNS_PARAM_NAME = "metaColumns";
	public static final boolean DEFAULT_META_COLUMNS_VALUE = false;
	public static final String ASSIGN_INNER_COLUMN_NAMES_PARAM = "assignColumnNames";
	public static final boolean DEFAULT_ASSIGN_INNER_COLUMN_NAMES_VALUE = false;
	public static final String USE_TIMESTAMP_WITH_TIME_ZONE_PARAM = "timestamptz";
	public static final boolean DEFAULT_USE_TIMESTAMP_WITH_TIME_ZONE_VALUE = true;
	public static final String ON_MISSING_METRIC_PARAM = "missingMetric";
	public static final OnMissingMetricAction DEFAULT_ON_MISSING_METRIC_VALUE = OnMissingMetricAction.WARNING;
	public static final String COMPATIBILITY_PARAM = "compatibility";
	public static final String DEFAULT_COMPATIBILITY_VALUE = null;

	public static final String QUERY_ID_PARAM_NAME = "queryId";
	public static final String Q_PARAM_NAME = "q";
	public static final String STRATEGY_PARAM_NAME = "strategy";
	public static final String DEFAULT_STRATEGY = "stream";
	public static final String FORMAT_PARAM_NAME = "outputFormat";
	public static final String FORMAT_PARAM_VALUE = "csv";
	public static final String METADATA_FORMAT_PARAM_NAME = "metadataFormat";
	public static final String LIMIT_PARAM_NAME = "limit";

	public static final int	MIN_SUPPORTED_ATSD_REVISION = 17285;
	public static final int API_FIND_METRICS_STATISTICS_RENAME_REV = 19437;
	public static final int API_METRIC_STATISTICS_RENAME_REV = 20295;
	public static final String REVISION_LINE = "Revision";

	public static final String CONN_KEEP_ALIVE = "Keep-Alive";
	public static final String AUTHORIZATION_TYPE = "Basic ";
	public static final String CSV_AND_JSON_MIME_TYPE = "text/csv,application/json";
	public static final String PLAIN_AND_JSON_MIME_TYPE = "text/plain,application/json";
	public static final String COMPRESSION_ENCODING = "gzip";
	public static final String DEFAULT_ENCODING = "identity";
	public static final String FORM_URLENCODED_TYPE = "application/x-www-form-urlencoded";
	public static final String USER_AGENT = "ATSD JDBC Client/" + JDBC_DRIVER_VERSION_DEFAULT;

	public static final String DEFAULT_TABLE_NAME = "atsd_series";
	public static final String TEXT_TITLES = "text";

	public static final String DATATYPE_PROPERTY = "datatype";
	public static final String PROPERTY_URL = "propertyUrl";
	public static final String COLUMNS_SCHEME = "columns";
	public static final String INDEX_PROPERTY = "columnIndex";
	public static final String NAME_PROPERTY = "name";
	public static final String PUBLISHER_SECTION = "dc:publisher";
	public static final String SCHEMA_NAME_PROPERTY = "schema:name";
	public static final String TABLE_PROPERTY = "table";
	public static final String TABLE_SCHEMA_SECTION = "tableSchema";
	public static final String TITLE_PROPERTY = "titles";

	public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
}
