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

import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.URLEncoder;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import com.axibase.tsd.driver.jdbc.content.*;
import com.axibase.tsd.driver.jdbc.content.json.Metric;
import com.axibase.tsd.driver.jdbc.content.json.Series;
import com.axibase.tsd.driver.jdbc.converter.AtsdSqlConverterFactory;
import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.enums.DefaultColumn;
import com.axibase.tsd.driver.jdbc.enums.timedatesyntax.EndTime;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.intf.IDataProvider;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import com.axibase.tsd.driver.jdbc.util.TimeDateExpression;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import static org.apache.calcite.avatica.Meta.StatementType.SELECT;

public class AtsdMeta extends MetaImpl {
	private static final LoggingFacade log = LoggingFacade.getLogger(AtsdMeta.class);

	public static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = prepareFormatter("yyyy-MM-dd");
	public static final ThreadLocal<SimpleDateFormat> TIME_FORMATTER = prepareFormatter("HH:mm:ss");
	public static final ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMATTER = prepareFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	public static final ThreadLocal<SimpleDateFormat> TIMESTAMP_SHORT_FORMATTER = prepareFormatter("yyyy-MM-dd'T'HH:mm:ss'Z'");

	private final AtomicInteger idGenerator = new AtomicInteger(1);
	private final Map<Integer, ContentMetadata> metaCache = new ConcurrentHashMap<>();
	private final Map<Integer, IDataProvider> providerCache = new ConcurrentHashMap<>();
	private final Map<Integer, StatementContext> contextMap = new ConcurrentHashMap<>();
	private final String schema;
	private final String catalog;
	private final boolean showMetaColumns;
	private final boolean assignColumnNames;

	public AtsdMeta(final AvaticaConnection conn) {
		super(conn);
		this.connProps.setAutoCommit(true);
		this.connProps.setReadOnly(true);
		this.connProps.setTransactionIsolation(Connection.TRANSACTION_NONE);
		this.connProps.setDirty(false);
		this.schema = null;
		final AtsdConnectionInfo connectionInfo = ((AtsdConnection) conn).getConnectionInfo();
		this.catalog = connectionInfo.catalog();
		this.showMetaColumns = connectionInfo.metaColumns();
		this.assignColumnNames = connectionInfo.assignColumnNames();
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
	public StatementHandle prepare(ConnectionHandle connectionHandle, String query, long maxRowCount) {
		final int id = idGenerator.getAndIncrement();
		log.trace("[prepare] connection: {} query: {}", id, query);
		StatementType statementType = EnumUtil.getStatementTypeByQuery(query);
		List<ColumnMetaData> columnMetaDataList = prepareColumnMetaData(statementType, query);
		Signature signature = new Signature(columnMetaDataList, query, Collections.<AvaticaParameter>emptyList(), null,
				statementType == SELECT ? CursorFactory.LIST : null, statementType);
		return new StatementHandle(connectionHandle.id, id, signature);
	}

	@Override
	public ExecuteResult execute(StatementHandle statementHandle, List<TypedValue> parameterValues, long maxRowsCount)
			throws NoSuchStatementException {
		return execute(statementHandle, parameterValues, AvaticaUtils.toSaturatedInt(maxRowsCount));
	}

	@Override
	public ExecuteResult execute(StatementHandle statementHandle, List<TypedValue> parameterValues, int maxRowsInFirstFrame) throws NoSuchStatementException {
		if (log.isTraceEnabled()) {
			log.trace("[execute] maxRowsInFirstFrame: {} parameters: {} handle: {}", maxRowsInFirstFrame, parameterValues.size(),
					statementHandle.toString());
		}
		final AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
		final String query = substitutePlaceholders(getSql(statement), parameterValues);
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
				String content = AtsdSqlConverterFactory.getConverter(statementType).convertToCommand(query);
				provider.getContentDescription().setPostContent(content);
				long updateCount = provider.sendData(timeout);

				MetaResultSet metaResultSet = MetaResultSet.count(statementHandle.connectionId, statementHandle.id, updateCount);
				List<MetaResultSet> resultSets = Collections.singletonList(metaResultSet);
				result = new ExecuteResult(resultSets);
			}
			return result;
		} catch (final RuntimeException e) {
			log.error("[execute] error", e);
			throw e;
		} catch (final Exception e) {
			log.error("[execute] error", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	private static String substitutePlaceholders(String query, List<TypedValue> parameterValues) {
		if (query.contains("?")) {
			final StringBuilder buffer = new StringBuilder(query.length());
			final String[] parts = query.split("\\?", -1);
			if (parts.length != parameterValues.size() + 1) {
				throw new IndexOutOfBoundsException(
						String.format("Number of specified values [%d] does not match to number of occurrences [%d]",
								parameterValues.size(), parts.length - 1));
			}
			buffer.append(parts[0]);
			int position = 0;
			for (TypedValue parameterValue : parameterValues) {
				++position;
				Object value = parameterValue.value;

				if (value instanceof Number || value instanceof TimeDateExpression || value instanceof EndTime) {
					buffer.append(value);
				} else if (value instanceof String) {
					buffer.append('\'').append((String) value).append('\'');
				} else if (value instanceof java.sql.Date) {
					buffer.append('\'').append(DATE_FORMATTER.get().format((java.sql.Date) value)).append('\'');
				} else if (value instanceof Time) {
					buffer.append('\'').append(TIME_FORMATTER.get().format((Time) value)).append('\'');
				} else if (value instanceof Timestamp) {
					buffer.append('\'').append(TIMESTAMP_FORMATTER.get().format((Timestamp) value)).append('\'');
				}

				buffer.append(parts[position]);
			}

			final String result = buffer.toString();
			log.debug("[substitutePlaceholders] {}", result);
			return result;
		}
		return query;
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
				String content = AtsdSqlConverterFactory.getConverter(statementType).convertToCommand(query);
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
				String content = AtsdSqlConverterFactory.getConverter(statementType).convertToCommand(query);
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
			String content = AtsdSqlConverterFactory.getConverter(statementType).convertBatchToCommands(query, preparedValueBatch);
			provider.getContentDescription().setPostContent(content);
			long updateCount = provider.sendData(timeout);
			ExecuteBatchResult result = new ExecuteBatchResult(new long[] {updateCount});
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
		assert provider != null;
		final ContentDescription contentDescription = provider.getContentDescription();
		final IStoreStrategy strategy = provider.getStrategy();
		final ContentMetadata contentMetadata = metaCache.get(statementHandle.id);
		if (contentMetadata == null) {
			throw new MissingResultsException(statementHandle);
		}
		try {
			if (offset == 0) {
				final String[] headers = strategy.openToRead(contentMetadata.getMetadataList());
				if (headers == null || headers.length == 0) {
					throw new MissingResultsException(statementHandle);
				}
				contentDescription.setHeaders(headers);
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
	public boolean syncResults(StatementHandle sh, QueryState state, long offset) throws NoSuchStatementException {
		log.debug("[syncResults] statement: {} offset: {}", sh.id, offset);
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
		AtsdConnection atsdConnection = (AtsdConnection) connection;
		final AtsdConnectionInfo connectionInfo = atsdConnection.getConnectionInfo();
		if (typeList == null || typeList.contains("TABLE")) {
			final Iterable<Object> iterable = receiveTables(connectionInfo);
			return getResultSet(iterable, AtsdMetaResultSets.AtsdMetaTable.class);
		}
		return createEmptyResultSet(AtsdMetaResultSets.AtsdMetaTable.class);

	}

	private AtsdMetaResultSets.AtsdMetaTable generateDefaultMetaTable() {
		return new AtsdMetaResultSets.AtsdMetaTable(catalog, schema,
				DriverConstants.DEFAULT_TABLE_NAME, "TABLE", "SELECT metric, entity, tags.collector, " +
				"tags.host, datetime, time, value FROM atsd_series WHERE metric = 'gc_time_percent' " +
				"AND entity = 'atsd' AND datetime >= now - 5*MINUTE ORDER BY datetime DESC LIMIT 10");
	}

	private AtsdMetaResultSets.AtsdMetaTable generateMetaTable(String table) {
		return new AtsdMetaResultSets.AtsdMetaTable(catalog, schema, table, "TABLE", generateTableRemark(table));
	}

	private String generateTableRemark(String table) {
		StringBuilder buffer = new StringBuilder("SELECT");
		for (DefaultColumn defaultColumn : DefaultColumn.values()) {
			if (showMetaColumns || !defaultColumn.isMetaColumn()) {
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

	private List<Object> receiveTables(AtsdConnectionInfo connectionInfo) {
		final List<Object> metricList = new ArrayList<>();
		final String tables = connectionInfo.tables();
		if (StringUtils.isNotBlank(tables)) {
			final String metricsUrl = connectionInfo.toEndpoint(DriverConstants.METRICS_ENDPOINT);
			try (final IContentProtocol contentProtocol = new SdkProtocolImpl(new ContentDescription(metricsUrl, connectionInfo))) {
				final InputStream metricsInputStream = contentProtocol.getMetrics(tables);
				final Metric[] metrics = JsonMappingUtil.mapToMetrics(metricsInputStream);
				for (Metric metric : metrics) {
					metricList.add(generateMetaTable(metric.getName()));
				}
			} catch (Exception e) {
				log.error(e.getMessage());
				//don't fill metric tables in case of error
			}
		}
		log.trace("[receiveTables] {}", metricList);
		return metricList;
	}

	@Override
	public MetaResultSet getSchemas(ConnectionHandle ch, String catalog, Pat schemaPattern) {
		log.debug("[getSchemas] connection: {} catalog: {} schemaPattern: {} ", ch.id, catalog, schemaPattern);
		return createEmptyResultSet(MetaSchema.class);
	}

	@Override
	public MetaResultSet getCatalogs(ConnectionHandle ch) {
        log.debug("[getCatalogs] connection: {}", ch.id);
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
		final List<Object> list = new ArrayList<>(atsdTypes.length - 2);
		for (AtsdType type : atsdTypes) {
			if (!(type == AtsdType.LONG_DATA_TYPE || type == AtsdType.SHORT_DATA_TYPE)) {
				list.add(getTypeInfo(type));
			}
		}
		return getResultSet(list, AtsdMetaResultSets.AtsdMetaTypeInfo.class);
	}

	@Override
	public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
		log.debug("[getColumns] connection: {} catalog: {} schemaPattern: {} tableNamePattern: {} columnNamePattern: {}", ch.id, catalog, schemaPattern,
				tableNamePattern, columnNamePattern);
		final String tablePattern = tableNamePattern.s;
		if (tablePattern != null) {
			DefaultColumn[] columns = getDefaultColumns(columnNamePattern == null ? null : columnNamePattern.s);
			List<Object> columnData = new ArrayList<>(columns.length);
			int position;
			for (DefaultColumn column : columns) {
				position = column.ordinal() + 1;
				if (showMetaColumns || !column.isMetaColumn()) {
					columnData.add(createColumnMetaData(column, schema, tablePattern, position));
				}
			}
			if (!DriverConstants.DEFAULT_TABLE_NAME.equals(tablePattern)) {
				position = DefaultColumn.values().length + 1;
				for (String tag : getTags(tablePattern)) {
					columnData.add(createColumnMetaData(new TagColumn(tag), schema, tablePattern, position));
					++position;
				}
			}
			return getResultSet(columnData, AtsdMetaResultSets.AtsdMetaColumn.class);
		}
		return createEmptyResultSet(AtsdMetaResultSets.AtsdMetaColumn.class);
	}

	private static DefaultColumn[] getDefaultColumns(String name) {
		if (StringUtils.isBlank(name)) {
			return DefaultColumn.values();
		}

		DefaultColumn defaultColumn = DefaultColumn.findByName(name);
		return new DefaultColumn[] { defaultColumn };
	}

	private Set<String> getTags(String metric) {
		final AtsdConnectionInfo connectionInfo = ((AtsdConnection) connection).getConnectionInfo();
		if (connectionInfo.expandTags()) {
			final String seriesUrl = toSeriesEndpoint(connectionInfo, metric);
			try (final IContentProtocol contentProtocol = new SdkProtocolImpl(new ContentDescription(seriesUrl, connectionInfo))) {
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

	private String toSeriesEndpoint(AtsdConnectionInfo connectionInfo, String metric) {
		String encodedMetric;
		try {
			encodedMetric = URLEncoder.encode(metric, DriverConstants.DEFAULT_CHARSET.displayName(Locale.US));
		} catch (UnsupportedEncodingException e) {
			log.error("[toSeriesEndpoint] {}", e.getMessage());
			encodedMetric = metric;
		}
		return connectionInfo.toEndpoint(DriverConstants.METRICS_ENDPOINT) + "/" + encodedMetric + "/series";
	}

	private Object createColumnMetaData(MetadataColumnDefinition column, String schema, String table, int ordinal) {
		final AtsdType columnType = column.getType();
		return new AtsdMetaResultSets.AtsdMetaColumn(
				catalog,
				schema,
				table,
				column.getColumnNamePrefix(),
				columnType.sqlTypeCode,
				columnType.sqlType,
				columnType.size,
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
			AtsdConnectionInfo connectionInfo = atsdConnection.getConnectionInfo();
			final IDataProvider dataProvider = new DataProvider(connectionInfo, sql, newContext, statementType);
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
		ContentMetadata contentMetadata = new ContentMetadata(jsonScheme, sql, catalog, connectionId, statementId, assignColumnNames);
		metaCache.put(statementId, contentMetadata);
		return contentMetadata;
	}

	private static AtsdMetaResultSets.AtsdMetaTypeInfo getTypeInfo(AtsdType type) {
		return new AtsdMetaResultSets.AtsdMetaTypeInfo(type.sqlType.toUpperCase(Locale.US), type.sqlTypeCode, type.maxPrecision,
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
			} else if (value instanceof java.sql.Date) {
				result.add(DATE_FORMATTER.get().format((java.sql.Date) value));
			} else if (value instanceof Time) {
				result.add(TIME_FORMATTER.get().format((Time) value));
			} else if (value instanceof Timestamp) {
				result.add(TIMESTAMP_FORMATTER.get().format((Timestamp) value));
			} else {
				result.add(value == null ? null : String.valueOf(value));
			}
		}

		log.debug("[preparedValues] {}", result);
		return result;
	}

	private static List<ColumnMetaData> prepareColumnMetaData(final StatementType statementType, final String query) {
		log.debug("[prepareColumnMetaData] statementType: {} sql: {}", statementType, query);
		if (Meta.StatementType.SELECT == statementType) {
			final int end = StringUtils.indexOfIgnoreCase(query, " from");
			if (end == -1 || StringUtils.contains(query.substring(6, end), '*')) {
				return null;
			}
			Pair<String, List<String>> pair = extractTableNameAndColumnNames(query);
			log.debug("[prepareColumnMetaData] tableName: {} columnNames: {}", pair.getKey(), pair.getValue());
			List<ColumnMetaData> result = new ArrayList<>();
			int index = 0;
			for (String columnName : pair.getValue()) {
				result.add(createColumnMetaData(index++, pair.getKey(), columnName));
			}
			return result;
		} else {
			log.debug("Not yet implemented for statement type: {}", statementType);
			return null;
		}
	}

	private static Pair<String, List<String>> extractTableNameAndColumnNames(String query) {
		query = query.toLowerCase();
		final int begin = StringUtils.indexOfIgnoreCase(query, "select ") + 7;
		final int end = StringUtils.indexOfIgnoreCase(query, " from ");
		String sqlColumns = query.substring(begin, end);
		String[] names = StringUtils.split(sqlColumns, ',');
		List<String> columnNames = new ArrayList<>(names.length);
		for (String name : names) {
			if (name != null) {
				int spaceIndex = name.indexOf(' ');
				if (spaceIndex > 0) {
					name = name.substring(0, spaceIndex);
				}
				columnNames.add(StringUtils.removeAll(name.trim(), "[\"'`]"));
			}
		}
		String tail = query.substring(end + 6);
		int spaceIndex = query.indexOf(' ');
		final String tableName = StringUtils.removeAll(spaceIndex == -1 ? tail.trim() : query.substring(0, spaceIndex), "[\"'`]");
		return new ImmutablePair<>(tableName, columnNames);
	}

	private static ColumnMetaData createColumnMetaData(int position, String tableName, String columnName) {
		DefaultColumn column = DefaultColumn.findByName(columnName);
		final ColumnMetaData.Rep rep = column.getType().rep;
		final ColumnMetaData.AvaticaType avaticaType = ContentMetadata.getAvaticaType(column.getType());
		return new ColumnMetaData(position, false, false, false, false, column.getNullable(), false,
				column.getType().size, columnName, columnName, (String) null, 1,1, tableName, (String) null, avaticaType, true,
				false, false, rep.clazz.getName());
	}

}
