/*
* Copyright 2016 Axibase Corporation or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
* https://www.axibase.com/atsd/axibase-apache-2.0.pdf
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import com.axibase.tsd.driver.jdbc.content.*;
import com.axibase.tsd.driver.jdbc.content.json.Metric;
import com.axibase.tsd.driver.jdbc.content.json.Series;
import com.axibase.tsd.driver.jdbc.converter.AtsdSqlConverterFactory;
import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.enums.DefaultColumn;
import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.intf.IDataProvider;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import com.axibase.tsd.driver.jdbc.util.WildcardsUtil;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.axibase.tsd.driver.jdbc.DriverConstants.DEFAULT_CHARSET;
import static com.axibase.tsd.driver.jdbc.DriverConstants.DEFAULT_TABLE_NAME;
import static org.apache.calcite.avatica.Meta.StatementType.SELECT;

public class AtsdMeta extends MetaImpl {
	private static final LoggingFacade log = LoggingFacade.getLogger(AtsdMeta.class);

	public static final ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMATTER = prepareFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	public static final ThreadLocal<SimpleDateFormat> TIMESTAMP_SHORT_FORMATTER = prepareFormatter("yyyy-MM-dd'T'HH:mm:ss'Z'");

	private final AtomicInteger idGenerator = new AtomicInteger(1);
	private final Map<Integer, ContentMetadata> metaCache = new ConcurrentHashMap<>();
	private final Map<Integer, IDataProvider> providerCache = new ConcurrentHashMap<>();
	private final Map<Integer, StatementContext> contextMap = new ConcurrentHashMap<>();
	private final Map<Integer, List<String>> queryPartsMap = new ConcurrentHashMap<>();
	private final AtsdConnectionInfo atsdConnectionInfo;

	public AtsdMeta(final AvaticaConnection conn) {
		super(conn);
		this.connProps.setAutoCommit(true);
		this.connProps.setReadOnly(true);
		this.connProps.setTransactionIsolation(Connection.TRANSACTION_NONE);
		this.connProps.setDirty(false);
		this.atsdConnectionInfo = ((AtsdConnection) conn).getConnectionInfo();
	}

	private static ThreadLocal<SimpleDateFormat> prepareFormatter(final String pattern) {
		return new ThreadLocal<SimpleDateFormat>() {
			@Override
			protected SimpleDateFormat initialValue() {
				SimpleDateFormat sdt = new SimpleDateFormat(pattern, Locale.US);
				sdt.setTimeZone(TimeZone.getTimeZone("UTC"));
				return sdt;
			}
		};
	}

	public StatementContext getContextFromMap(StatementHandle statementHandle) {
		return contextMap.get(statementHandle.id);
	}

	@Override
	@SneakyThrows(SQLException.class)
	public StatementHandle prepare(ConnectionHandle connectionHandle, String query, long maxRowCount) {
		final int statementHandleId = idGenerator.getAndIncrement();
		log.trace("[prepare] handle: {} query: {}", statementHandleId, query);

		if (StringUtils.isBlank(query)) {
			throw new SQLException("Failed to prepare statement with blank query");
		}
		final List<String> queryParts = splitQueryByPlaceholder(query);
		queryPartsMap.put(statementHandleId, queryParts);
		final StatementType statementType = EnumUtil.getStatementTypeByQuery(query);
		Signature signature = new Signature(new ArrayList<ColumnMetaData>(), query, Collections.<AvaticaParameter>emptyList(), null,
				statementType == SELECT ? CursorFactory.LIST : null, statementType);
		return new StatementHandle(connectionHandle.id, statementHandleId, signature);
	}

	public void updatePreparedStatementResultSetMetaData(Signature signature, StatementHandle handle) throws SQLException {
		if (signature.columns.isEmpty()) {
			final String metaEndpoint = Location.SQL_META_ENDPOINT.getUrl(atsdConnectionInfo);
			final ContentDescription contentDescription = new ContentDescription(
					metaEndpoint, atsdConnectionInfo, signature.sql, new StatementContext(handle));
			try (final IContentProtocol protocol = new SdkProtocolImpl(contentDescription)) {
				final List<ColumnMetaData> columnMetaData = ContentMetadata.buildMetadataList(protocol.readContent(0),
						atsdConnectionInfo.catalog(), atsdConnectionInfo.assignColumnNames());
				signature.columns.addAll(columnMetaData);
			} catch (AtsdJsonException e) {
				final Object jsonError = e.getJson().get("error");
				if (jsonError != null) {
					log.error("[updatePreparedStatementResultSetMetaData] error: {}", jsonError);
					throw new SQLException(jsonError.toString());
				} else {
					throw new SQLException(e);
				}
			} catch (AtsdRuntimeException e) {
				log.error("[updatePreparedStatementResultSetMetaData] error: {}", e.getMessage());
				throw new SQLDataException(e);
			} catch (Exception e)  {
				log.error("[updatePreparedStatementResultSetMetaData] error", e);
				throw new SQLException(e);
			}
		}
	}

	@Override
	public ExecuteResult execute(StatementHandle statementHandle, List<TypedValue> parameterValues, long maxRowsCount)
			throws NoSuchStatementException {
		return execute(statementHandle, parameterValues, AvaticaUtils.toSaturatedInt(maxRowsCount));
	}

	@Override
	@SneakyThrows(SQLDataException.class)
	public ExecuteResult execute(StatementHandle statementHandle, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
		if (log.isTraceEnabled()) {
			log.trace("[execute] maxRowsInFirstFrame: {} parameters: {} handle: {}", maxRowsInFirstFrame, parameterValues.size(),
					statementHandle.toString());
		}
		final List<String> queryParts = queryPartsMap.get(statementHandle.id);
		if (queryParts == null) {
			throw new NoSuchStatementException(statementHandle);
		}
		final String query = substitutePlaceholders(queryParts, parameterValues);
        final AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
        final StatementType statementType = statement.getStatementType();
        try {
			IDataProvider provider = createDataProvider(statementHandle, query, statementType);
			final int timeout = getQueryTimeout(statement);
			final ExecuteResult result;
			if (SELECT == statementType) {
				final int maxRows = getMaxRows(statement);
				provider.fetchData(maxRows, timeout);
				final ContentMetadata contentMetadata = createMetadata(query, statementHandle.connectionId, statementHandle.id);
				result = new ExecuteResult(contentMetadata.getList());
			} else {
				String content = AtsdSqlConverterFactory.getConverter(statementType, atsdConnectionInfo.timestampTz()).convertToCommand(query);
				provider.getContentDescription().setPostContent(content);
				long updateCount = provider.sendData(timeout);

				MetaResultSet metaResultSet = MetaResultSet.count(statementHandle.connectionId, statementHandle.id, updateCount);
				List<MetaResultSet> resultSets = Collections.singletonList(metaResultSet);
				result = new ExecuteResult(resultSets);
			}
			return result;
		} catch (SQLDataException e) {
			log.error("[execute] error", e.getMessage());
			throw e;
		} catch (final RuntimeException e) {
			log.error("[execute] error", e);
			throw e;
		} catch (final Exception e) {
			log.error("[execute] error", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	static List<String> splitQueryByPlaceholder(String query) {
		final List<String> queryParts = new ArrayList<>();
		final int length = query.length();
		boolean quoted = false;
		boolean singleQuoted = false;
		int startOfQueryPart = 0;
		for (int i = 0; i < length; i++) {
			char currentChar = query.charAt(i);
			switch (currentChar) {
				case '?':
					if (!quoted && !singleQuoted) {
						queryParts.add(query.substring(startOfQueryPart, i));
						startOfQueryPart = i + 1;
					}
					break;
				case '\'':
					if (!quoted) {
						singleQuoted = !singleQuoted;
					}
					break;
				case '"':
					if (!singleQuoted) {
						quoted = !quoted;
					}
					break;
			}
		}
		queryParts.add(StringUtils.substring(query, startOfQueryPart));
		return queryParts;
	}

	private static String substitutePlaceholders(List<String> queryParts, List<TypedValue> parameterValues) {
		final int parametersSize = parameterValues.size();
		final int queryPartsSize = queryParts.size();
		if (queryPartsSize - 1 != parametersSize) {
			throw new AtsdRuntimeException(String.format("Number of specified values [%d] does not match the number of placeholder occurrences [%d]",
					parametersSize, queryPartsSize - 1));
		}
		if (queryPartsSize == 1) {
			return queryParts.get(0);
		}
		final StringBuilder buffer = new StringBuilder();
		for (int i = 0; i < parametersSize; i++) {
			buffer.append(queryParts.get(i));
			appendTypedValue(parameterValues.get(i), buffer);
		}
		buffer.append(queryParts.get(parametersSize));
		final String result = buffer.toString();
		log.debug("[substitutePlaceholders] {}", result);
		return result;
	}

	private static void appendTypedValue(TypedValue parameterValue, StringBuilder buffer) {
		Object value = parameterValue.value;
        if (value == null) {
            buffer.append("NULL");
            return;
        }
        switch(parameterValue.type) {
			case STRING:
				buffer.append('\'').append(value).append('\'');
				break;
			case JAVA_SQL_TIMESTAMP:
			case JAVA_UTIL_DATE:
				buffer.append('\'').append(TIMESTAMP_FORMATTER.get().format(value)).append('\'');
				break;
			default:
				buffer.append(value);
		}
	}

	private int getMaxRows(Statement statement) {
		int maxRows = 0;
		if (statement != null) {
			try {
				maxRows = statement.getMaxRows();
			} catch (SQLException e) {
				maxRows = 0;
			}
		}
		return maxRows;
	}

	private int getQueryTimeout(Statement statement) {
		int timeout = 0;
		if (statement != null) {
			try {
				timeout = statement.getQueryTimeout();
			} catch (SQLException e) {
				timeout = 0;
			}
		}
		return timeout;
	}

	@Override
	public ExecuteResult prepareAndExecute(StatementHandle statementHandle, String query, long maxRowCount,
										   PrepareCallback callback) throws NoSuchStatementException {
		return prepareAndExecute(statementHandle, query, maxRowCount, 0, callback);
	}

	@Override
	@SneakyThrows(SQLDataException.class)
	public ExecuteResult prepareAndExecute(StatementHandle statementHandle, String query, long maxRowCount,
										   int maxRowsInFrame, PrepareCallback callback) throws NoSuchStatementException {
		final long limit = maxRowCount < 0 ? 0 : maxRowCount;
		log.trace("[prepareAndExecute] handle: {} maxRowCount: {} query: {}", statementHandle, limit, query);
		try {
			final AvaticaStatement statement = (AvaticaStatement) callback.getMonitor();
			final StatementType statementType = statement.getStatementType() == null ? EnumUtil.getStatementTypeByQuery(query) : statement.getStatementType();
			final IDataProvider provider = createDataProvider(statementHandle, query, statementType);
			final long updateCount;
			if (SELECT == statementType) {
				provider.fetchData(limit, statement.getQueryTimeout());
				updateCount = -1;
			} else {
				String content = AtsdSqlConverterFactory.getConverter(statementType, atsdConnectionInfo.timestampTz()).convertToCommand(query);
				provider.getContentDescription().setPostContent(content);
				updateCount = provider.sendData(statement.getQueryTimeout());
			}
			final ContentMetadata contentMetadata = createMetadata(query, statementHandle.connectionId, statementHandle.id);
			synchronized (callback.getMonitor()) {
				callback.clear();
				callback.assign(contentMetadata.getSign(), null, updateCount);
			}
			final ExecuteResult result = new ExecuteResult(contentMetadata.getList());
			callback.execute();
			return result;
		} catch (final AtsdRuntimeException e) {
			log.error("[prepareAndExecute] error", e);
			throw new SQLDataException(e.getMessage(), e);
		} catch (final RuntimeException e) {
			log.error("[prepareAndExecute] error", e);
			throw e;
		} catch (final Exception e) {
			log.error("[prepareAndExecute] error", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle statementHandle, List<String> queries) throws NoSuchStatementException {
        log.trace("[prepareAndExecuteBatch] handle: {} queries: {}", statementHandle.toString(), queries);
		try {
			final AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
			long[] updateCounts = new long[queries.size()];
			int count = 0;
			for (String query : queries) {
				final StatementType statementType = statement.getStatementType() == null ? EnumUtil.getStatementTypeByQuery(query) : statement.getStatementType();
				if (SELECT == statementType) {
					throw new IllegalArgumentException("Invalid statement type: " + statementType);
				}
				final IDataProvider provider = createDataProvider(statementHandle, query, statementType);
				String content = AtsdSqlConverterFactory.getConverter(statementType, atsdConnectionInfo.timestampTz()).convertToCommand(query);
				provider.getContentDescription().setPostContent(content);
				long updateCount = provider.sendData(statement.getQueryTimeout());
				updateCounts[count++] = updateCount;
			}
			final ExecuteBatchResult result = new ExecuteBatchResult(updateCounts);
			return result;
		} catch (final RuntimeException e) {
            log.error("[prepareAndExecuteBatch] error", e);
			throw e;
		} catch (final Exception e) {
            log.error("[prepareAndExecuteBatch] error", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public ExecuteBatchResult executeBatch(StatementHandle statementHandle, List<List<TypedValue>> parameterValueBatch) throws NoSuchStatementException {
		log.trace("[executeBatch] parameters: {} handle: {}", parameterValueBatch.size(), statementHandle.toString());
		final AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
		final StatementType statementType = statement.getStatementType();
		if (SELECT == statementType) {
			throw new IllegalArgumentException("Invalid statement type: " + statementType);
		}
		final String query = getSql(statement);
		final List<List<Object>> preparedValueBatch = prepareValueBatch(parameterValueBatch);
		try {
            IDataProvider provider = createDataProvider(statementHandle, query, statementType);
            final int timeout = getQueryTimeout(statement);
			String content = AtsdSqlConverterFactory.getConverter(statementType, atsdConnectionInfo.timestampTz()).convertBatchToCommands(query, preparedValueBatch);
			provider.getContentDescription().setPostContent(content);
			long updateCount = provider.sendData(timeout);
			ExecuteBatchResult result = new ExecuteBatchResult(generateExecuteBatchResult(parameterValueBatch.size(), updateCount == 0 ? 0 : 1));
			return result;
		} catch (final RuntimeException e) {
			log.error("[executeBatch] error", e);
			throw e;
		} catch (final Exception e) {
            log.error("[executeBatch] error", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public Frame fetch(final StatementHandle statementHandle, long loffset, int fetchMaxRowCount)
			throws NoSuchStatementException, MissingResultsException {
		final int offset = (int) loffset;
		log.trace("[fetch] statement: {} fetchMaxRowCount: {}, offset: {}", statementHandle.id, fetchMaxRowCount, offset);
		IDataProvider provider = providerCache.get(statementHandle.id);
		if (provider == null) {
			throw new MissingResultsException(statementHandle);
		}
		final IStoreStrategy strategy = provider.getStrategy();
		final ContentMetadata contentMetadata = metaCache.get(statementHandle.id);
		if (contentMetadata == null) {
			throw new MissingResultsException(statementHandle);
		}
		try {
			if (offset == 0) {
				final String[] headers = strategy.openToRead(contentMetadata.getMetadataList());
				if (ArrayUtils.isEmpty(headers)) {
					throw new MissingResultsException(statementHandle);
				}
			}
			@SuppressWarnings("unchecked")
			final List<Object> subList = (List) strategy.fetch(offset, fetchMaxRowCount);
			return new Meta.Frame(loffset, subList.size() < fetchMaxRowCount, subList);
		} catch (final AtsdException | IOException e) {
			if (log.isDebugEnabled()) {
				log.debug("[fetch] " + e.getMessage());
			}
			throw new MissingResultsException(statementHandle);
		}

	}

	public void cancelStatement(StatementHandle statementHandle) {
		final IDataProvider provider = providerCache.get(statementHandle.id);
		if (provider != null) {
			provider.cancelQuery();
		}
	}

	@Override
	public void closeStatement(StatementHandle statementHandle) {
		log.debug("[closeStatement] {}->{}", statementHandle.id, statementHandle);
		closeProviderCaches(statementHandle);
		closeProvider(statementHandle);
	}

	private void closeProviderCaches(StatementHandle statementHandle) {
		metaCache.remove(statementHandle.id);
		contextMap.remove(statementHandle.id);
		queryPartsMap.remove(statementHandle.id);
		log.trace("[closeProviderCaches]");
	}

	private void closeProvider(StatementHandle statementHandle) {
		final IDataProvider provider = providerCache.remove(statementHandle.id);
		if (provider != null) {
			try {
				provider.close();
			} catch (final Exception e) {
				log.error("[closeProvider] error", e);
			}
		}
	}

	@Override
	public void closeConnection(ConnectionHandle ch) {
		super.closeConnection(ch);
		metaCache.clear();
		contextMap.clear();
		providerCache.clear();
		log.trace("[closeConnection]");
	}

	@Override
	public boolean syncResults(StatementHandle statementHandle, QueryState state, long offset) throws NoSuchStatementException {
		log.debug("[syncResults] statement: {} offset: {}", statementHandle.id, offset);
		return false;
	}

	@Override
	public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle connectionHandle) {
		return super.getDatabaseProperties(connectionHandle);
	}

	@Override
	public MetaResultSet getTables(ConnectionHandle connectionHandle, String catalog, Pat schemaPattern, Pat tableNamePattern,
								   List<String> typeList) {
        log.debug("[getTables] connection: {} catalog: {} schemaPattern: {} tableNamePattern: {} typeList: {}", connectionHandle.id, catalog, schemaPattern,
                tableNamePattern, typeList);
        if (typeList == null || typeList.contains("TABLE")) {
        	final String pattern = StringUtils.isBlank(schemaPattern.s) ? tableNamePattern.s : schemaPattern.s + '.' + tableNamePattern.s;
			final List<Object> tables = receiveTables(atsdConnectionInfo, pattern);

			if(log.isDebugEnabled()) {
				log.debug("[getTables] count: {}", tables.size());
				StringBuilder sb = new StringBuilder();
				sb.append('[');
				AtsdMetaResultSets.AtsdMetaTable metaTable;
				final int limit = tables.size() > 20 ? 20 : tables.size();
				for (int i=0;i<limit;i++) {
					metaTable = (AtsdMetaResultSets.AtsdMetaTable) tables.get(i);
					if(sb.length() > 1) {
						sb.append(',');
					}
					sb.append(metaTable.tableName);
				}
				sb.append(']');
				log.debug("[getTables] tables: {}", sb.toString());
			}

			return getResultSet(tables, AtsdMetaResultSets.AtsdMetaTable.class);
		}
		return createEmptyResultSet(AtsdMetaResultSets.AtsdMetaTable.class);

	}

	private AtsdMetaResultSets.AtsdMetaTable generateDefaultMetaTable() {
		return new AtsdMetaResultSets.AtsdMetaTable(atsdConnectionInfo.catalog(), atsdConnectionInfo.schema(),
				DEFAULT_TABLE_NAME, "TABLE", "SELECT metric, entity, tags.collector, " +
				"tags.host, datetime, time, value FROM atsd_series WHERE metric = 'gc_time_percent' " +
				"AND entity = 'atsd' AND datetime >= now - 5*MINUTE ORDER BY datetime DESC LIMIT 10");
	}

	private AtsdMetaResultSets.AtsdMetaTable generateMetaTable(String table) {
		return new AtsdMetaResultSets.AtsdMetaTable(atsdConnectionInfo.catalog(), atsdConnectionInfo.schema(),
				table, "TABLE", generateTableRemark(table));
	}

	private String generateTableRemark(String table) {
		StringBuilder buffer = new StringBuilder("SELECT");
		for (DefaultColumn defaultColumn : DefaultColumn.values()) {
			if (atsdConnectionInfo.metaColumns() || !defaultColumn.isMetaColumn()) {
				if (defaultColumn.ordinal() != 0) {
					buffer.append(',');
				}
				buffer.append(' ').append(defaultColumn.getColumnNamePrefix());
			}
		}
		return buffer
				.append(" FROM '")
				.append(table)
				.append("' LIMIT 1")
				.toString();
	}

	private List<Object> receiveTables(AtsdConnectionInfo connectionInfo, String pattern) {
		final List<Object> metricList = new ArrayList<>();
		final List<String> metricMasks = connectionInfo.tables();
		if (!metricMasks.isEmpty()) {
			if (containsAtsdSeriesTable(metricMasks) && WildcardsUtil.wildcardMatch(DEFAULT_TABLE_NAME, pattern)) {
				metricList.add(generateDefaultMetaTable());
			}

			final List<String> metricNames = getAndFilterMetricsFromAtsd(metricMasks, connectionInfo, pattern);
			if (metricNames != Collections.EMPTY_LIST) {
				for (String metricMask : metricMasks) {
					if (!metricNames.contains(metricMask) && isNotPatternExpression(metricMask)) {
						metricNames.add(metricMask);
					}
				}
			}

			for (String metricName : metricNames) {
				metricList.add(generateMetaTable(metricName));
			}
		}

		return metricList;
	}

	private static List<String> getAndFilterMetricsFromAtsd(List<String> metricMasks, AtsdConnectionInfo connectionInfo, String pattern) {
		final String atsdPattern = WildcardsUtil.replaceSqlWildcardsWithAtsd(pattern);
		final String metricsUrl = prepareUrlWithMetricExpression(Location.METRICS_ENDPOINT.getUrl(connectionInfo), metricMasks, atsdPattern);
		try (final IContentProtocol contentProtocol = new SdkProtocolImpl(new ContentDescription(metricsUrl, connectionInfo))) {
			final InputStream metricsInputStream = contentProtocol.readInfo();
			final Metric[] metrics = JsonMappingUtil.mapToMetrics(metricsInputStream);
			List<String> result = new ArrayList<>();
			for (Metric metric : metrics) {
				if (WildcardsUtil.wildcardMatch(metric.getName(), pattern)) {
					result.add(metric.getName());
				}
			}
            return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			return Collections.emptyList();
		}
	}

	private static boolean containsAtsdSeriesTable(List<String> metricMasks) {
		for (String metricMask : metricMasks) {
			if (WildcardsUtil.atsdWildcardMatch(DEFAULT_TABLE_NAME, metricMask)) {
				return true;
			}

		}
		return false;
	}

	@SneakyThrows(UnsupportedEncodingException.class)
	private static String prepareUrlWithMetricExpression(String metricEndpoint, List<String> metricMasks, String tablesFilter) {
		final boolean applyFilter = StringUtils.isNotBlank(tablesFilter);
		final StringBuilder expressionBuilder = new StringBuilder();
		if (applyFilter) {
			expressionBuilder.append('(');
		}
		for (String mask : metricMasks) {
			if (expressionBuilder.length() > 1) {
				expressionBuilder.append(" or ");
			}
			expressionBuilder.append("name");
			if (StringUtils.contains(mask, '*')) {
				expressionBuilder.append(" like ");
			} else {
				expressionBuilder.append('=');
			}
			expressionBuilder.append('\'').append(mask).append('\'');
		}
		if (applyFilter) {
			expressionBuilder.append(") and name like '").append(tablesFilter).append('\'');
		}
		return metricEndpoint + "?expression=" + URLEncoder.encode(expressionBuilder.toString(), DEFAULT_CHARSET.name());

	}

	@Override
	public MetaResultSet getSchemas(ConnectionHandle connectionHandle, String catalog, Pat schemaPattern) {
		log.debug("[getSchemas] connection: {} catalog: {} schemaPattern: {} ", connectionHandle.id, catalog, schemaPattern);
		return createEmptyResultSet(MetaSchema.class);
	}

	@Override
	public MetaResultSet getCatalogs(ConnectionHandle ch) {
        log.debug("[getCatalogs] connection: {}", ch.id);
        final String catalog = atsdConnectionInfo.catalog();
		final Iterable<Object> iterable = catalog == null ? Collections.emptyList() :
				Collections.<Object>singletonList(new MetaCatalog(catalog));
		return getResultSet(iterable, MetaCatalog.class);
	}

	@Override
	public MetaResultSet getTableTypes(ConnectionHandle ch) {
		log.debug("[getTableTypes] connection: {}", ch.id);
		final Iterable<Object> iterable = Arrays.<Object>asList(
				new MetaTableType("TABLE"), new MetaTableType("VIEW"), new MetaTableType("SYSTEM"));
		return getResultSet(iterable, MetaTableType.class);
	}

	@Override
	public MetaResultSet getTypeInfo(ConnectionHandle ch) {
		log.debug("[getTypeInfo] connection: {}", ch.id);
		AtsdType[] atsdTypes = AtsdType.values();
		final List<Object> list = new ArrayList<>(atsdTypes.length);
		for (AtsdType type : atsdTypes) {
			list.add(getTypeInfo(type));
		}
		return getResultSet(list, AtsdMetaResultSets.AtsdMetaTypeInfo.class);
	}

	@Override
	public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
        log.debug("[getColumns] connection: {} catalog: {} schemaPattern: {} tableNamePattern: {} columnNamePattern: {}", ch.id, catalog, schemaPattern,
                tableNamePattern, columnNamePattern);
        final List<String> metricMasks = atsdConnectionInfo.tables();
		if (!metricMasks.isEmpty()) {
			final String colNamePattern = columnNamePattern.s;
			final List<DefaultColumn> columns = filterColumns(colNamePattern, atsdConnectionInfo.metaColumns());

			List<Object> columnData = new ArrayList<>();
			final String pattern = StringUtils.isBlank(schemaPattern.s) ? tableNamePattern.s : schemaPattern.s + '.' + tableNamePattern.s;
			final List<String> tableNames = WildcardsUtil.hasWildcards(pattern) ?
					getAndFilterMetricsFromAtsd(metricMasks, atsdConnectionInfo, pattern):
					Collections.singletonList(pattern);
			for (String tableName : tableNames) {
				int position = 1;
				for (DefaultColumn column : columns) {
					columnData.add(createColumnMetaData(column, tableName, position));
					++position;
				}
				if (DEFAULT_TABLE_NAME.equals(tableName) ||
						(!WildcardsUtil.hasWildcards(colNamePattern) && !colNamePattern.startsWith(TagColumn.PREFIX))) {
					continue;
				}
				Set<String> tags = getTags(tableName);
				if (tags.isEmpty() && StringUtils.startsWith(colNamePattern, TagColumn.PREFIX) && isNotPatternExpression(colNamePattern)) {
					final TagColumn column = new TagColumn(StringUtils.substringAfter(colNamePattern, TagColumn.PREFIX));
					columnData.add(createColumnMetaData(column, tableName, position));
				} else {
					for (String tag : tags) {
						final TagColumn column = new TagColumn(tag);
						if (WildcardsUtil.wildcardMatch(column.getColumnNamePrefix(), colNamePattern)) {
							columnData.add(createColumnMetaData(column, tableName, position));
							++position;
						}
					}
				}
			}

			if (log.isDebugEnabled()) {
				log.debug("[getColumns] count: {}", columnData.size());
				StringBuilder sb = new StringBuilder();
                sb.append('[');
                AtsdMetaResultSets.AtsdMetaColumn metaColumn;
                for (Object column : columnData) {
                    metaColumn = (AtsdMetaResultSets.AtsdMetaColumn) column;
                    if(sb.length() > 1) {
                        sb.append(',');
                    }
                    sb.append("{name=").append(metaColumn.columnName).append(", ").append("type=").append(metaColumn.typeName).append('}');
                }
                sb.append(']');
                log.debug("[getColumns] columns: {}", sb.toString());
            }

			return getResultSet(columnData, AtsdMetaResultSets.AtsdMetaColumn.class);
		}
		return createEmptyResultSet(AtsdMetaResultSets.AtsdMetaColumn.class);
	}

	private static List<DefaultColumn> filterColumns(String columnPattern, boolean showMetaColumns) {
		List<DefaultColumn> result = new ArrayList<>();
		for (DefaultColumn column : DefaultColumn.values()) {
			if ((showMetaColumns || !column.isMetaColumn())
					&& WildcardsUtil.wildcardMatch(column.getColumnNamePrefix(), columnPattern)) {
				result.add(column);
			}
		}
		return result;
	}

	private Set<String> getTags(String metric) {
		if (atsdConnectionInfo.expandTags()) {
			final String seriesUrl = toSeriesEndpoint(atsdConnectionInfo, metric);
			try (final IContentProtocol contentProtocol = new SdkProtocolImpl(new ContentDescription(seriesUrl, atsdConnectionInfo))) {
				final InputStream seriesInputStream = contentProtocol.readInfo();
				final Series[] seriesArray = JsonMappingUtil.mapToSeries(seriesInputStream);
                if (log.isTraceEnabled()) {
                    log.trace("[response] content: {}", Arrays.toString(seriesArray));
                }
                Set<String> tags = new HashSet<>();
				for (Series series : seriesArray) {
					tags.addAll(series.getTags().keySet());
				}
				return tags;
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return Collections.emptySet();
	}

	@SneakyThrows(UnsupportedEncodingException.class)
	private String toSeriesEndpoint(AtsdConnectionInfo connectionInfo, String metric) {
		String encodedMetric = URLEncoder.encode(metric, DriverConstants.DEFAULT_CHARSET.name());
		return Location.METRICS_ENDPOINT.getUrl(connectionInfo) + "/" + encodedMetric + "/series";
	}

	private Object createColumnMetaData(MetadataColumnDefinition column, String table, int ordinal) {
		final AtsdType columnType = column.getType();
		return new AtsdMetaResultSets.AtsdMetaColumn(
				atsdConnectionInfo.catalog(),
				atsdConnectionInfo.schema(),
				table,
				column.getColumnNamePrefix(),
				columnType,
				10,
				column.getNullable(),
				ordinal,
				column.getNullableAsString()
		);
	}

	private MetaResultSet getResultSet(Iterable<Object> iterable, Class<?> clazz) {
		final Field[] fields = clazz.getDeclaredFields();
		final int length = fields.length;
		final List<ColumnMetaData> columns = new ArrayList<>(length);
		final List<String> fieldNames = new ArrayList<>(length);
		int index = 0;
		for (Field field : fields) {
			final String name = AvaticaUtils.camelToUpper(field.getName());
			columns.add(columnMetaData(name, index, field.getType(), getColumnNullability(field)));
			fieldNames.add(name);
			++index;
		}

		if (log.isTraceEnabled()) {
			log.trace("[createResultSet] clazzName: {} fieldNames: {}", clazz.getSimpleName(), fieldNames);
		}
		return createResultSet(Collections.<String, Object>emptyMap(), columns,
				CursorFactory.record(clazz, Arrays.asList(fields), fieldNames), new Frame(0, true, iterable));
	}

	private IDataProvider createDataProvider(StatementHandle statementHandle, String sql, StatementType statementType) throws UnsupportedEncodingException {
		assert connection instanceof AtsdConnection;
		AtsdConnection atsdConnection = (AtsdConnection) connection;
		final StatementContext newContext = new StatementContext(statementHandle);
		contextMap.put(statementHandle.id, newContext);
		try {
			AtsdDatabaseMetaData metaData = atsdConnection.getAtsdDatabaseMetaData();
			newContext.setVersion(metaData.getConnectedAtsdVersion());
			final IDataProvider dataProvider = new DataProvider(atsdConnectionInfo, sql, newContext, statementType);
			providerCache.put(statementHandle.id, dataProvider);
			return dataProvider;
		} catch (SQLException e) {
			log.error("[createDataProvider] Error attempting to get databaseMetadata", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	private ContentMetadata createMetadata(String sql, String connectionId, int statementId)
			throws AtsdException, IOException {
		IDataProvider provider = providerCache.get(statementId);
		final String jsonScheme = provider != null ? provider.getContentDescription().getJsonScheme() : "";
		ContentMetadata contentMetadata = new ContentMetadata(jsonScheme, sql, atsdConnectionInfo.catalog(),
				connectionId, statementId, atsdConnectionInfo.assignColumnNames());
		metaCache.put(statementId, contentMetadata);
		return contentMetadata;
	}

	private static AtsdMetaResultSets.AtsdMetaTypeInfo getTypeInfo(AtsdType type) {
		return new AtsdMetaResultSets.AtsdMetaTypeInfo(type.sqlType, type.sqlTypeCode, type.maxPrecision,
				type.getLiteral(true), type.getLiteral(false),
				(short) DatabaseMetaData.typeNullable, type == AtsdType.STRING_DATA_TYPE,
				(short) DatabaseMetaData.typeSearchable, false, false, false,
				(short) 0, (short) 0, 10);
	}

	// Since Calcite 1.6.0

	@Override
	public void commit(ConnectionHandle ch) {
		if (log.isDebugEnabled()) {
			log.debug("[commit] " + ch.id + "->" + ch.toString());
		}
	}

	@Override
	public void rollback(ConnectionHandle ch) {
		if (log.isDebugEnabled()) {
			log.debug("[rollback] " + ch.id + "->" + ch.toString());
		}
	}

	private static String getSql(AvaticaStatement statement) {
		if (statement instanceof AtsdStatement) {
			return ((AtsdStatement) statement).getSql();
		} else if (statement instanceof AtsdPreparedStatement) {
			return ((AtsdPreparedStatement) statement).getSql();
		}
		throw new IllegalArgumentException("Unsupported statement class: " + statement.getClass().getSimpleName());
	}

	private static List<List<Object>> prepareValueBatch(List<List<TypedValue>> parameterValueBatch) {
		if (parameterValueBatch.isEmpty()) {
			return Collections.emptyList();
		}
		List<List<Object>> result = new ArrayList<>(parameterValueBatch.size());
		for (List<TypedValue> parameterValues : parameterValueBatch) {
			result.add(prepareValues(parameterValues));
		}
		return result;
	}

	private static List<Object> prepareValues(List<TypedValue> parameterValues) {
		if (parameterValues.isEmpty()) {
			return Collections.emptyList();
		}
		List<Object> result = new ArrayList<>(parameterValues.size());
		for (TypedValue parameterValue : parameterValues) {
			Object value = parameterValue.value;

			if (value instanceof Number || value instanceof String) {
				result.add(value);
			} else if (value instanceof Date) {
				result.add(TIMESTAMP_FORMATTER.get().format((Date) value));
			} else {
				result.add(value == null ? null : String.valueOf(value));
			}
		}
		log.debug("[preparedValues] {}", result);
		return result;
	}

	private static long[] generateExecuteBatchResult(int size, long value) {
		long[] result = new long[size];
		Arrays.fill(result, value);
		return result;
	}

	private static boolean isNotPatternExpression(String input) {
		return StringUtils.containsNone(input, WildcardsUtil.WILDCARDS);
	}
}
