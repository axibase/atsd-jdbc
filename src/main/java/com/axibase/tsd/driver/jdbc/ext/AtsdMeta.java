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
import com.axibase.tsd.driver.jdbc.converter.AtsdCommandConverter;
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
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.calcite.avatica.Meta.StatementType.SELECT;

public class AtsdMeta extends MetaImpl {
	private static final LoggingFacade log = LoggingFacade.getLogger(AtsdMeta.class);

	public static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = prepareFormatter("yyyy-MM-dd");
	public static final ThreadLocal<SimpleDateFormat> TIME_FORMATTER = prepareFormatter("HH:mm:ss");
	public static final ThreadLocal<SimpleDateFormat> TIMESTAMP_FORMATTER = prepareFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	public static final ThreadLocal<SimpleDateFormat> TIMESTAMP_SHORT_FORMATTER = prepareFormatter("yyyy-MM-dd'T'HH:mm:ss'Z'");

	private static final Set<StatementType> SUPPORTED_STATEMENT_TYPES = Collections.unmodifiableSet(new HashSet<StatementType>() {
		{
			add(StatementType.SELECT);
			add(StatementType.INSERT);
			add(StatementType.UPDATE);
		}
	});

	private final AtomicInteger idGenerator = new AtomicInteger(1);
	private final Map<Integer, ContentMetadata> metaCache = new ConcurrentHashMap<>();
	private final Map<Integer, IDataProvider> providerCache = new ConcurrentHashMap<>();
	private final Map<Integer, StatementContext> contextMap = new ConcurrentHashMap<>();
	private final ReentrantLock lock = new ReentrantLock();
	private final String schema;
	private final String catalog;

	public AtsdMeta(final AvaticaConnection conn) {
		super(conn);
		this.connProps.setAutoCommit(true);
		this.connProps.setReadOnly(true);
		this.connProps.setTransactionIsolation(Connection.TRANSACTION_NONE);
		this.connProps.setDirty(false);
		this.schema = null;
		this.catalog = ((AtsdConnection) conn).getConnectionInfo().catalog();
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
		try {
			lock.lockInterruptibly();
		} catch (InterruptedException e) {
			if (log.isDebugEnabled()) {
				log.debug("[prepare] " + e.getMessage());
			}
			Thread.currentThread().interrupt();
		}
		final int id = idGenerator.getAndIncrement();
		if (log.isTraceEnabled()) {
			log.trace("[prepare] locked: {} handle: {} query: {}", lock.getHoldCount(), id, query);
		}

		final StatementType statementType = getStatementTypeByQuery(query);
		Signature signature = new Signature(null, query, Collections.<AvaticaParameter>emptyList(), null,
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
		IDataProvider provider = null;
		try {
			provider = initProvider(statementHandle, query, statement.getStatementType());
		} catch (IOException e) {
			if (log.isDebugEnabled()) {
				log.debug("[execute]" + e.getMessage());
			}
		}
		assert provider != null;
		try {
			final int timeout = getQueryTimeout(statement);
			final ExecuteResult result;
			if (SELECT == statement.getStatementType()) {
				final int maxRows = getMaxRows(statement);
				provider.fetchData(maxRows, timeout);
				final ContentMetadata contentMetadata = findMetadata(query, statementHandle.connectionId, statementHandle.id);
				result = new ExecuteResult(contentMetadata.getList());
			} else {
				AtsdCommandConverter converter = new AtsdCommandConverter();
				String content = converter.convertSqlToCommand(query);
				provider.getContentDescription().setPostContent(content);
				long updateCount = provider.sendData(timeout);

				MetaResultSet metaResultSet = MetaResultSet.count(statementHandle.connectionId, statementHandle.id, updateCount);
				List<MetaResultSet> resultSets = Collections.singletonList(metaResultSet);
				result = new ExecuteResult(resultSets);
			}
			return result;
		} catch (final RuntimeException e) {
			if (log.isErrorEnabled()) {
				log.error("[execute] error", e);
			}
			throw e;
		} catch (final Exception e) {
			if (log.isErrorEnabled()) {
				log.error("[execute] error", e);
			}
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	private static String substitutePlaceholders(String query, List<TypedValue> parameterValues) {
		if (query.contains("?")) {
			final StringBuilder buffer = new StringBuilder(query.length());
			final String[] parts = query.split("\\?", -1);
			if (parts.length != parameterValues.size() + 1) {
				throw new IndexOutOfBoundsException(
						String.format("Number of specified values [%d] does not match to number of occurences [%d]",
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
			if (log.isDebugEnabled()) {
				log.debug("[substitutePlaceholders] " + result);
			}
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
		long limit = maxRowCount < 0 ? 0 : maxRowCount;
		try {
			lock.lockInterruptibly();
		} catch (InterruptedException e) {
			if (log.isDebugEnabled()) {
				log.debug("[prepareAndExecute] " + e.getMessage());
			}
			Thread.currentThread().interrupt();
		}
		if (log.isTraceEnabled()) {
			log.trace("[prepareAndExecute] locked: {} maxRowCount: {} handle: {} query: {}", lock.getHoldCount(),
					limit, statementHandle.toString(), query);
		}
		try {
			final AvaticaStatement statement = (AvaticaStatement) callback.getMonitor();
			final IDataProvider provider = initProvider(statementHandle, query, statement.getStatementType());
			final long updateCount;
			if (SELECT == statement.getStatementType()) {
				provider.fetchData(limit, statement.getQueryTimeout());
				updateCount = -1;
			} else {
				AtsdCommandConverter converter = new AtsdCommandConverter();
				String content = converter.convertSqlToCommand(query);
				provider.getContentDescription().setPostContent(content);
				updateCount = provider.sendData(statement.getQueryTimeout());
			}
			final ContentMetadata contentMetadata = findMetadata(query, statementHandle.connectionId, statementHandle.id);
			synchronized (callback.getMonitor()) {
				// callback.clear();
				callback.assign(contentMetadata.getSign(), null, updateCount);
			}
			final ExecuteResult result = new ExecuteResult(contentMetadata.getList());
			callback.execute();
			return result;
		} catch (final RuntimeException e) {
			if (log.isErrorEnabled()) {
				log.error("[prepareAndExecute] error", e);
			}
			throw e;
		} catch (final Exception e) {
			if (log.isErrorEnabled()) {
				log.error("[prepareAndExecute] error", e);
			}
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle statementHandle, List<String> queries) throws NoSuchStatementException {
		try {
			lock.lockInterruptibly();
		} catch (InterruptedException e) {
			if (log.isDebugEnabled()) {
				log.debug("[prepareAndExecuteBatch] " + e.getMessage());
			}
			Thread.currentThread().interrupt();
		}
		if (log.isTraceEnabled()) {
			log.trace("[prepareAndExecuteBatch] locked: {} handle: {} queries: {}", lock.getHoldCount(),
					statementHandle.toString(), queries);
		}
		try {
			final AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
			long[] updateCounts = new long[queries.size()];
			int count = 0;
			for (String query : queries) {
				final StatementType statementType = statement.getStatementType() == null ? getStatementTypeByQuery(query) : statement.getStatementType();
				if (SELECT == statementType) {
					throw new IllegalArgumentException("Invalid statement type: " + statementType);
				}
				final IDataProvider provider = initProvider(statementHandle, query, statementType);
				AtsdCommandConverter converter = new AtsdCommandConverter();
				String content = converter.convertSqlToCommand(query);
				provider.getContentDescription().setPostContent(content);
				long updateCount = provider.sendData(statement.getQueryTimeout());
				updateCounts[count++] = updateCount;
			}
			final ExecuteBatchResult result = new ExecuteBatchResult(updateCounts);
			return result;
		} catch (final RuntimeException e) {
			if (log.isErrorEnabled()) {
				log.error("[prepareAndExecuteBatch] error", e);
			}
			throw e;
		} catch (final Exception e) {
			if (log.isErrorEnabled()) {
				log.error("[prepareAndExecuteBatch] error", e);
			}
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public ExecuteBatchResult executeBatch(StatementHandle statementHandle, List<List<TypedValue>> parameterValueBatch) throws NoSuchStatementException {
		if (log.isTraceEnabled()) {
			log.trace("[executeBatch] parameters: {} handle: {}", parameterValueBatch.size(),
					statementHandle.toString());
		}
		final AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
		final StatementType statementType = statement.getStatementType();
		if (SELECT == statementType) {
			throw new IllegalArgumentException("Invalid statement type: " + statementType);
		}
		final String query = getSql(statement);
		final List<List<Object>> preparedValueBatch = prepareValueBatch(parameterValueBatch);
		IDataProvider provider = null;
		try {
			provider = initProvider(statementHandle, query, statement.getStatementType());
		} catch (IOException e) {
			if (log.isDebugEnabled()) {
				log.debug("[executeBatch]" + e.getMessage());
			}
		}
		assert provider != null;
		try {
			final int timeout = getQueryTimeout(statement);
			AtsdCommandConverter converter = new AtsdCommandConverter();
			String content = converter.convertBatchToCommands(query, preparedValueBatch);
			provider.getContentDescription().setPostContent(content);
			long updateCount = provider.sendData(timeout);
			ExecuteBatchResult result = new ExecuteBatchResult(new long[] {updateCount});
			return result;
		} catch (final RuntimeException e) {
			if (log.isErrorEnabled()) {
				log.error("[executeBatch] error", e);
			}
			throw e;
		} catch (final Exception e) {
			if (log.isErrorEnabled()) {
				log.error("[executeBatch] error", e);
			}
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public Frame fetch(final StatementHandle statementHandle, long loffset, int fetchMaxRowCount)
			throws NoSuchStatementException, MissingResultsException {
		final int offset = (int) loffset;
		if (log.isTraceEnabled()) {
			log.trace("[fetch] fetchMaxRowCount: {} offset: {}", fetchMaxRowCount, offset);
		}
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
		if (log.isDebugEnabled()) {
			log.debug("[closeStatement] " + statementHandle.id + "->" + statementHandle.toString());
		}
		closeProviderCaches(statementHandle);
		closeProvider(statementHandle);
		if (lock.isHeldByCurrentThread()) {
			lock.unlock();
			if (log.isTraceEnabled()) {
				log.trace("[unlocked]");
			}
		}
		if (log.isTraceEnabled()) {
			log.trace("[closedStatement]");
		}
	}

	private void closeProviderCaches(StatementHandle statementHandle) {
		if (!metaCache.isEmpty()) {
			metaCache.remove(statementHandle.id);
		}
		if (!contextMap.isEmpty()) {
			contextMap.remove(statementHandle.id);
		}
		if (log.isTraceEnabled()) {
			log.trace("[closeProviderCaches]");
		}

	}

	private void closeProvider(StatementHandle statementHandle) {
		if (!providerCache.isEmpty()) {
			final IDataProvider provider = providerCache.remove(statementHandle.id);
			if (provider != null) {
				try {
					provider.close();
				} catch (final Exception e) {
					if (log.isDebugEnabled()) {
						log.debug("[closeProvider] " + e.getMessage());
					}
				}
			}
		}
	}

	public void closeConnection() {
		closeCaches();
		if (lock.isHeldByCurrentThread()) {
			lock.unlock();
			if (log.isTraceEnabled()) {
				log.trace("[unlocked]");
			}
		}
		if (log.isTraceEnabled()) {
			log.trace("[closed]");
		}
	}

	private void closeCaches() {
		if (!metaCache.isEmpty()) {
			metaCache.clear();
		}
		if (!contextMap.isEmpty()) {
			contextMap.clear();
		}
		if (!providerCache.isEmpty()) {
			providerCache.clear();
		}
	}

	@Override
	public boolean syncResults(StatementHandle sh, QueryState state, long offset) throws NoSuchStatementException {
		if (log.isDebugEnabled()) {
			log.debug("[syncResults] " + offset);
		}
		return false;
	}

	@Override
	public Map<DatabaseProperty, Object> getDatabaseProperties(ConnectionHandle connectionHandle) {
		return super.getDatabaseProperties(connectionHandle);
	}

	@Override
	public MetaResultSet getTables(ConnectionHandle connectionHandle, String catalog, Pat schemaPattern, Pat tableNamePattern,
								   List<String> typeList) {
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
		return new AtsdMetaResultSets.AtsdMetaTable(catalog, schema,
				table, "TABLE", "SELECT metric, entity, tags, datetime, time, value" +
				" FROM '" + table + "' WHERE datetime >= now - 1*HOUR ORDER BY datetime DESC LIMIT 10");
	}

	private List<Object> receiveTables(AtsdConnectionInfo connectionInfo) {
		final List<Object> metricList = new ArrayList<>();
		metricList.add(generateDefaultMetaTable());
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
			}
		}
		return metricList;
	}

	@Override
	public MetaResultSet getSchemas(ConnectionHandle connectionHandle, String catalog, Pat schemaPattern) {
		return createEmptyResultSet(MetaSchema.class);
	}

	@Override
	public MetaResultSet getCatalogs(ConnectionHandle ch) {
		final Iterable<Object> iterable = Collections.<Object>singletonList(
				new MetaCatalog(catalog));
		return getResultSet(iterable, MetaCatalog.class);
	}

	@Override
	public MetaResultSet getTableTypes(ConnectionHandle ch) {
		final Iterable<Object> iterable = Arrays.<Object>asList(
				new MetaTableType("TABLE"), new MetaTableType("VIEW"), new MetaTableType("SYSTEM"));
		return getResultSet(iterable, MetaTableType.class);
	}

	@Override
	public MetaResultSet getTypeInfo(ConnectionHandle ch) {
		AtsdType[] atsdTypes = AtsdType.values();
		final List<Object> list = new ArrayList<>(atsdTypes.length - 2);
		for (AtsdType type : atsdTypes) {
			if (!(type == AtsdType.LONG_DATA_TYPE || type == AtsdType.SHORT_DATA_TYPE)) {
				list.add(getTypeInfo(type));
			}
		}
		return getResultSet(list, MetaTypeInfo.class);
	}

	@Override
	public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
		final String tablePattern = tableNamePattern.s;
		if (tablePattern != null) {
			DefaultColumn[] columns = DefaultColumn.values();
			List<Object> columnData = new ArrayList<>(columns.length);
			int position = 1;
			for (DefaultColumn column : columns) {
				columnData.add(createColumnMetaData(column, schema, tablePattern, position));
				++position;
			}
			if (!DriverConstants.DEFAULT_TABLE_NAME.equals(tablePattern)) {
				for (String tag : getTags(tablePattern)) {
					columnData.add(createColumnMetaData(new TagColumn(tag), schema, tablePattern, position));
					++position;
				}
			}
			return getResultSet(columnData, AtsdMetaResultSets.AtsdMetaColumn.class);
		}
		return createEmptyResultSet(AtsdMetaResultSets.AtsdMetaColumn.class);
	}

	private Set<String> getTags(String metric) {
		final AtsdConnectionInfo connectionInfo = ((AtsdConnection) connection).getConnectionInfo();
		if (connectionInfo.expandTags()) {
			final String seriesUrl = toSeriesEndpoint(connectionInfo, metric);
			try (final IContentProtocol contentProtocol = new SdkProtocolImpl(new ContentDescription(seriesUrl, connectionInfo))) {
				final InputStream seriesInputStream = contentProtocol.readInfo();
				final Series[] seriesArray = JsonMappingUtil.mapToSeries(seriesInputStream);
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
				null,
				10,
				column.getNullable(),
				0,
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

		return createResultSet(Collections.<String, Object>emptyMap(), columns,
				CursorFactory.record(clazz, Arrays.asList(fields), fieldNames), new Frame(0, true, iterable));
	}

	private IDataProvider initProvider(StatementHandle statementHandle, String sql, StatementType statementType) throws UnsupportedEncodingException {
		assert connection instanceof AtsdConnection;
		AtsdConnection atsdConnection = (AtsdConnection) connection;
		final StatementContext newContext = new StatementContext(statementHandle);
		contextMap.put(statementHandle.id, newContext);
		try {
			AtsdDatabaseMetaData metaData = (AtsdDatabaseMetaData) connection.getMetaData();
			newContext.setVersion(metaData.getDatabaseMajorVersion());
			AtsdConnectionInfo connectionInfo = atsdConnection.getConnectionInfo();
			final IDataProvider dataProvider = new DataProvider(connectionInfo, sql, newContext, statementType);
			providerCache.put(statementHandle.id, dataProvider);
			return dataProvider;
		} catch (SQLException e) {
			log.error("[initProvider] Error attempting to get databaseMetadata", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	private ContentMetadata findMetadata(String sql, String connectionId, int statementId)
			throws AtsdException, IOException {
		ContentMetadata contentMetadata = metaCache.get(statementId);
		if (contentMetadata == null) {
			IDataProvider provider = providerCache.get(statementId);
			final String jsonScheme = provider != null ? provider.getContentDescription().getJsonScheme() : "";
			contentMetadata = new ContentMetadata(jsonScheme, sql, catalog, connectionId, statementId);
			metaCache.put(statementId, contentMetadata);
		}
		return contentMetadata;
	}

	private static MetaTypeInfo getTypeInfo(AtsdType type) {
		return new MetaTypeInfo(type.sqlType.toUpperCase(Locale.US), type.sqlTypeCode, type.maxPrecision,
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

	private static StatementType getStatementTypeByQuery(final String query) {
		final String queryKind = query.substring(0, query.indexOf(' ')).toUpperCase();
		final StatementType statementType;
		try {
			statementType = StatementType.valueOf(queryKind);
		} catch (IllegalArgumentException exc) {
			throw new IllegalArgumentException("Illegal statement type: " + queryKind);
		}
		if (SUPPORTED_STATEMENT_TYPES.contains(statementType)) {
			return statementType;
		}
		throw new IllegalArgumentException("Unsupported statement type: " + queryKind);
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

		if (log.isDebugEnabled()) {
			log.debug("[preparedValues] " + result);
		}
		return result;
	}

}
