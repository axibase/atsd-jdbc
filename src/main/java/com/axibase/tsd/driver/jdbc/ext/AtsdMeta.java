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

import com.axibase.tsd.driver.jdbc.content.*;
import com.axibase.tsd.driver.jdbc.content.json.Metric;
import com.axibase.tsd.driver.jdbc.content.json.Series;
import com.axibase.tsd.driver.jdbc.converter.AtsdSqlConverter;
import com.axibase.tsd.driver.jdbc.converter.AtsdSqlConverterFactory;
import com.axibase.tsd.driver.jdbc.enums.*;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.intf.IDataProvider;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.util.DbMetadataUtils;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import com.axibase.tsd.driver.jdbc.util.WildcardsUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Date;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.axibase.tsd.driver.jdbc.DriverConstants.*;
import static org.apache.calcite.avatica.Meta.StatementType.SELECT;

public class AtsdMeta extends MetaImpl {
	private static final LoggingFacade log = LoggingFacade.getLogger(AtsdMeta.class);

	public static final FastDateFormat TIMESTAMP_PRINTER = prepareFormatter("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

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

	private static FastDateFormat prepareFormatter(final String pattern) {
		return FastDateFormat.getInstance(pattern, TimeZone.getTimeZone("UTC"), Locale.US);
	}

	StatementContext getContextFromMap(StatementHandle statementHandle) {
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
		final String normalizedQuery = normalizeQuery(query);
		final StatementType statementType = EnumUtil.getStatementTypeByQuery(normalizedQuery);
		final Signature signature;
		if (statementType == SELECT) {
			queryPartsMap.put(statementHandleId, splitQueryByPlaceholder(query));
			signature = new Signature(new ArrayList<ColumnMetaData>(), query, Collections.<AvaticaParameter>emptyList(), null,
					CursorFactory.LIST, statementType);
		} else {
			queryPartsMap.put(statementHandleId, splitQueryByPlaceholder(normalizedQuery));
			signature = new Signature(new ArrayList<ColumnMetaData>(), query, Collections.<AvaticaParameter>emptyList(), null,
					null, statementType);
		}
		return new StatementHandle(connectionHandle.id, statementHandleId, signature);
	}

	public void updatePreparedStatementResultSetMetaData(Signature signature, StatementHandle handle) throws SQLException {
		if (signature.columns.isEmpty()) {
			final String metaEndpoint = Location.SQL_META_ENDPOINT.getUrl(atsdConnectionInfo);
			final ContentDescription contentDescription = new ContentDescription(
					metaEndpoint, atsdConnectionInfo, signature.sql, new StatementContext(handle, false));
			try (final IContentProtocol protocol = new SdkProtocolImpl(contentDescription)) {
				final List<ColumnMetaData> columnMetaData = ContentMetadata.buildMetadataList(protocol.readContent(0),
						atsdConnectionInfo.catalog(), atsdConnectionInfo.assignColumnNames(), atsdConnectionInfo.odbc2Compatibility());
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
	@SneakyThrows({SQLDataException.class, SQLFeatureNotSupportedException.class})
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
        if (statement == null) {
        	throw new NoSuchStatementException(statementHandle);
		}
        final StatementType statementType = statement.getStatementType();
        try {
			final int timeoutMillis = statement.getQueryTimeout() * 1000;
			final ExecuteResult result;
			if (SELECT == statementType) {
				final boolean encodeTags = isEncodeTags(statement);
				final IDataProvider provider = createDataProvider(statementHandle, query, statementType, encodeTags);
				final int maxRows = statement.getMaxRows();
				provider.fetchData(maxRows, timeoutMillis);
				final ContentMetadata contentMetadata = createMetadata(query, statementHandle.connectionId, statementHandle.id);
				result = new ExecuteResult(contentMetadata.getList());
			} else {
				IDataProvider provider = createDataProvider(statementHandle, query, statementType, false);
				List<String> content = convertToCommands(statementType, query);
				provider.getContentDescription().setPostContent(StringUtils.join(content,'\n'));
				long updateCount = provider.sendData(timeoutMillis);

				MetaResultSet metaResultSet = MetaResultSet.count(statementHandle.connectionId, statementHandle.id, updateCount);
				List<MetaResultSet> resultSets = Collections.singletonList(metaResultSet);
				result = new ExecuteResult(resultSets);
			}
			return result;
		} catch (SQLDataException | SQLFeatureNotSupportedException e) {
			log.error("[execute] error {}", e.getMessage());
			throw e;
		} catch (final RuntimeException e) {
			log.error("[execute] error", e);
			throw e;
		} catch (final Exception e) {
			log.error("[execute] error", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	public static boolean isEncodeTags(AvaticaStatement statement) {
		if (statement instanceof AtsdPreparedStatement) {
			return ((AtsdPreparedStatement) statement).isTagsEncoding();
		} else if (statement instanceof AtsdStatement) {
			return ((AtsdStatement) statement).isTagsEncoding();
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private List<String> convertToCommands(StatementType statementType, String query) throws SQLException {
		return AtsdSqlConverterFactory.getConverter(statementType, atsdConnectionInfo.timestampTz()).convertToCommands(query);
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
				buffer.append('\'').append(TIMESTAMP_PRINTER.format(value)).append('\'');
				break;
			case OBJECT:
				appendObjectValue(value, buffer);
				break;
			default:
				buffer.append(value);
		}
	}

	private static void appendObjectValue(Object value, StringBuilder buffer) {
		if (value instanceof String) {
			buffer.append('\'').append(value).append('\'');
		} else if (value instanceof Date) {
			buffer.append('\'').append(TIMESTAMP_PRINTER.format((Date) value)).append('\'');
		} else {
			buffer.append(value);
		}
	}

	@Override
	@Deprecated
	public ExecuteResult prepareAndExecute(StatementHandle statementHandle, String query, long maxRowCount,
										   PrepareCallback callback) {
		return prepareAndExecute(statementHandle, query, maxRowCount, -1, callback);
	}

	@Override
	@SneakyThrows({SQLDataException.class, SQLFeatureNotSupportedException.class})
	public ExecuteResult prepareAndExecute(StatementHandle statementHandle, String query, long maxRowCount,
										   int maxRowsInFrame, PrepareCallback callback) {
		final long limit = Math.max(0, maxRowCount);
		log.trace("[prepareAndExecute] handle: {} maxRowCount: {} query: {}", statementHandle, limit, query);
		try {
			final AvaticaStatement statement = (AvaticaStatement) callback.getMonitor();
			final String normalizedQuery = normalizeQuery(query);
			final StatementType statementType = EnumUtil.getStatementTypeByQuery(normalizedQuery);
			final long updateCount;
			if (SELECT == statementType) {
				final boolean encodeTags = isEncodeTags(statement);
				final IDataProvider provider = createDataProvider(statementHandle, query, statementType, encodeTags);
				provider.fetchData(limit, statement.getQueryTimeout());
				updateCount = -1;
			} else {
				final List<String> content = convertToCommands(statementType, normalizedQuery);
				final IDataProvider provider = createDataProvider(statementHandle, query, statementType, false);
				provider.getContentDescription().setPostContent(StringUtils.join(content,'\n'));
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
		} catch (SQLDataException | SQLFeatureNotSupportedException e) {
			log.error("[prepareAndExecute] error {}", e.getMessage());
			throw e;
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
	@SneakyThrows({SQLDataException.class, SQLFeatureNotSupportedException.class})
	public ExecuteBatchResult prepareAndExecuteBatch(StatementHandle statementHandle, List<String> queries) throws NoSuchStatementException {
        log.trace("[prepareAndExecuteBatch] handle: {} queries: {}", statementHandle.toString(), queries);
		try {
			final AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
			long[] updateCounts = new long[queries.size()];
			int count = 0;
			for (String query : queries) {
				final String normalizedQuery = normalizeQuery(query);
				final StatementType statementType = EnumUtil.getStatementTypeByQuery(normalizedQuery);
				if (SELECT == statementType) {
					throw new IllegalArgumentException("Batch SELECT statements are not supported");
				}
				final List<String> content = convertToCommands(statementType, normalizedQuery);
				final IDataProvider provider = createDataProvider(statementHandle, query, statementType,false);
				provider.getContentDescription().setPostContent(StringUtils.join(content,'\n'));
				long updateCount = provider.sendData(statement.getQueryTimeout());
				updateCounts[count++] = updateCount;
			}
			return new ExecuteBatchResult(updateCounts);
		} catch (SQLDataException | SQLFeatureNotSupportedException e) {
			log.error("[prepareAndExecuteBatch] error {}", e.getMessage());
			throw e;
		} catch (final RuntimeException e) {
            log.error("[prepareAndExecuteBatch] error", e);
			throw e;
		} catch (final Exception e) {
            log.error("[prepareAndExecuteBatch] error", e);
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	@SneakyThrows({SQLDataException.class, SQLFeatureNotSupportedException.class})
	public ExecuteBatchResult executeBatch(StatementHandle statementHandle, List<List<TypedValue>> parameterValueBatch) throws NoSuchStatementException {
		log.trace("[executeBatch] parameters: {} handle: {}", parameterValueBatch.size(), statementHandle.toString());
		final AvaticaStatement statement = connection.statementMap.get(statementHandle.id);
		final StatementType statementType = statement.getStatementType();
		if (SELECT == statementType) {
			throw new IllegalArgumentException("Invalid statement type: " + statementType);
		}
		final String query = ((AtsdPreparedStatement) statement).getSql();
		final List<List<Object>> preparedValueBatch = prepareValueBatch(parameterValueBatch);
		try {
            IDataProvider provider = createDataProvider(statementHandle, query, statementType,false);
            final int timeoutMillis = statement.getQueryTimeout();
            final AtsdSqlConverter converter = AtsdSqlConverterFactory.getConverter(statementType, atsdConnectionInfo.timestampTz());
			@SuppressWarnings("unchecked")
			List<String> content = converter.convertBatchToCommands(query, preparedValueBatch);
			provider.getContentDescription().setPostContent(StringUtils.join(content,'\n'));
			long updateCount = provider.sendData(timeoutMillis);
			long[] updateCounts = updateCount == 0 ? new long[parameterValueBatch.size()] : converter.getCommandCounts();
			return new ExecuteBatchResult(updateCounts);
		} catch (SQLDataException | SQLFeatureNotSupportedException e) {
			log.error("[executeBatch] error {}", e.getMessage());
			throw e;
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

	static String normalizeQuery(String query) {
		final int length = query.length();
		int start = -1;
		for (int i = 0; i < length; i++) {
			final char currentChar = query.charAt(i);
			if (!Character.isWhitespace(currentChar)) {
				if (currentChar == '-') {
					final int nextLine = query.indexOf('\n', i);
					i = nextLine != -1 ? nextLine : length;
				} else {
					start = i;
					break;
				}
			}
		}
		if (start == -1) {
			return null;
		} else if (start == 0) {
			return query;
		} else {
			return query.substring(start);
		}
	}

	void cancelStatement(StatementHandle statementHandle) {
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
				log.debug("[getTables] tables: {}", buildTablesStringForDebug(tables));
			}

			return getResultSet(tables, AtsdMetaResultSets.AtsdMetaTable.class);
		}
		return createEmptyResultSet(AtsdMetaResultSets.AtsdMetaTable.class);
	}

	private static String buildTablesStringForDebug(List<Object> tables) {
		StringBuilder buffer = new StringBuilder();
		buffer.append('[');
		AtsdMetaResultSets.AtsdMetaTable metaTable;
		final int maxTablesShow = 20;
		final int limit = tables.size() > maxTablesShow ? maxTablesShow : tables.size();
		for (int i = 0;i < limit; i++) {
			metaTable = (AtsdMetaResultSets.AtsdMetaTable) tables.get(i);
			if(buffer.length() > 1) {
				buffer.append(',');
			}
			buffer.append(metaTable.tableName);
		}
		buffer.append(']');
		return buffer.toString();
	}

	private AtsdMetaResultSets.AtsdMetaTable generateMetaTable(String table, String remark) {
		return new AtsdMetaResultSets.AtsdMetaTable(atsdConnectionInfo.catalog(), atsdConnectionInfo.schema(),
				table, "TABLE", remark);
	}

	private String generateTableRemark(String table, boolean addCreationTime) {
		StringBuilder buffer = new StringBuilder("SELECT");
        for (DefaultColumn column : DefaultColumn.values()) {
            if (column.ordinal() != 0) {
                buffer.append(',');
            }
            buffer.append(' ').append(column.getColumnNamePrefix());
        }
		if (atsdConnectionInfo.metaColumns()) {
		    appendMetaColumns(buffer, EntityColumn.values(addCreationTime));
		    appendMetaColumns(buffer, MetricColumn.values(addCreationTime));
		}
		return buffer.append(" FROM \"").append(table).append("\" LIMIT 1").toString();
	}

	/**
	 * Append metric and entity metadata columns to series requests. This method must not be used while processing meta tables.
	 * @param buffer StringBuilder to append to
	 * @param values columns to append
	 */
	private static void appendMetaColumns(StringBuilder buffer, MetadataColumnDefinition[] values) {
        for (MetadataColumnDefinition column : values) {
            buffer.append(", ").append(column.getColumnNamePrefix());
        }
    }

    private boolean useShortColumnNamesInMetaTables() {
		return getAtsdRevision() >= META_COLUMNS_SHORT_REVISION;
	}

	private boolean shouldAddCreationTimeMetaColumns() {
		return getAtsdRevision() >= META_COLUMNS_CREATION_TIME_REVISION;
	}

	private List<Object> receiveTables(AtsdConnectionInfo connectionInfo, String pattern) {
		final List<Object> metricList = new ArrayList<>();
		final List<String> metricMasks = connectionInfo.tables();
		final boolean metadataColumnsShort = useShortColumnNamesInMetaTables();
		for (DefaultTable defaultTable : DefaultTable.values()) {
			for (String metricMask : metricMasks) {
				if (WildcardsUtil.wildcardMatch(defaultTable.tableName, metricMask)
						&& WildcardsUtil.wildcardMatch(defaultTable.tableName, pattern)) {
					metricList.add(generateMetaTable(defaultTable.tableName, defaultTable.getRemark(metadataColumnsShort)));
				}
			}
		}

		final Map<String, AtsdType> metricNamesToTypes = getAndFilterMetricsFromAtsd(metricMasks, pattern, false);
        if (metricNamesToTypes != Collections.<String, AtsdType>emptyMap()) { // some query was performed
            for (String metricMask : metricMasks) {
                if (!WildcardsUtil.hasWildcards(metricMask) && !DefaultTable.isDefaultTable(metricMask)) {
					metricNamesToTypes.put(WildcardsUtil.wildcardToTableName(metricMask), AtsdType.DEFAULT_VALUE_TYPE);
                }
            }
        }

        final boolean addCreationTime = shouldAddCreationTimeMetaColumns();
        for (String metricName : metricNamesToTypes.keySet()) {
            metricList.add(generateMetaTable(metricName, generateTableRemark(metricName, addCreationTime)));
        }

		return metricList;
	}

	private Map<String, AtsdType> getAndFilterMetricsFromAtsd(List<String> metricMasks, String pattern, boolean underscoreAsLiteral) {
		final Collection<MetricLocation> metricLocation = prepareGetMetricUrls(metricMasks, pattern, underscoreAsLiteral);
		if (metricLocation.isEmpty()) { // no limits set
			return Collections.emptyMap();
		}
		final Map<String, AtsdType> result = new LinkedHashMap<>();
		final int atsdRevision = getAtsdRevision();
		for (MetricLocation location : metricLocation) {
			final String metricsUrl = location.toEndpointUrl(atsdConnectionInfo, atsdRevision);
			try (final IContentProtocol contentProtocol = new SdkProtocolImpl(new ContentDescription(metricsUrl, atsdConnectionInfo))) {
				final InputStream metricsInputStream = contentProtocol.readInfo();
				final List<Metric> metrics = JsonMappingUtil.mapToMetrics(metricsInputStream, location.isReturnSingleElement());
				for (Metric metric : metrics) {
					if (WildcardsUtil.wildcardMatch(metric.getName(), pattern)) {
						result.put(metric.getName(), EnumUtil.getAtsdTypeWithPropertyUrlHint(metric.getDataType(), null));
					}
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return result;
	}

	private AtsdConnection getAtsdConnection() {
		return (AtsdConnection) connection;
	}

	private int getAtsdRevision() {
		return getAtsdConnection().getMetaData().getDatabaseMajorVersion();
	}

	/**
	 * Prepare URL to retrieve metrics
	 * @param metricMasks filter specified in `tables` connection string parameter
	 * @param tableFilter filter specified in method parameter
	 * @param underscoreAsLiteral treat underscore as not a metacharacter
	 * @return MetricLocation
	 */
	@Nonnull
	static Collection<MetricLocation> prepareGetMetricUrls(List<String> metricMasks, String tableFilter, boolean underscoreAsLiteral) {
		if (WildcardsUtil.isRetrieveAllPattern(tableFilter) || tableFilter.isEmpty()) {
			if (metricMasks.isEmpty()) {
				return Collections.emptyList();
			} else {
				return buildPatternDisjunction(metricMasks, underscoreAsLiteral);
			}
		} else  {
			return Collections.singletonList(buildAtsdPatternUrl(tableFilter, underscoreAsLiteral));
		}
	}

	private static boolean isWithWildcards(String sqlPattern, String atsdPattern) {
		return (StringUtils.isEmpty(sqlPattern) || !StringUtils.equals(sqlPattern, atsdPattern))
				&& WildcardsUtil.hasAtsdWildcards(atsdPattern);
	}

	private static MetricLocation buildAtsdPatternUrl(String sqlPattern, boolean underscoreAsLiteral) {
		final String atsdPattern = WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping(sqlPattern, underscoreAsLiteral);
		if (!isWithWildcards(sqlPattern, atsdPattern)) { // no wildcards
			return new MetricLocation(true, atsdPattern);
		}
		return new MetricLocation(false, "name like '" + atsdPattern + "'");
	}

	private static Collection<MetricLocation> buildPatternDisjunction(List<String> patterns, boolean underscoreAsLiteral) {
		final Collection<String> preprocessedPatterns = new ArrayList<>(patterns.size());
		boolean hasWildcard = false;
		for (String pattern : patterns) {
			final String preprocessed = WildcardsUtil.replaceSqlWildcardsWithAtsdUseEscaping(pattern, underscoreAsLiteral);
			preprocessedPatterns.add(preprocessed);
			if (!hasWildcard && isWithWildcards(pattern, preprocessed)) {
				hasWildcard = true;
			}
		}
		if (hasWildcard) {
			StringBuilder buffer = new StringBuilder();
			for (String mask : preprocessedPatterns) {
				if (buffer.length() > 1) {
					buffer.append(" or ");
				}
				buffer.append("name like '").append(mask).append('\'');
			}
			return Collections.singletonList(new MetricLocation(false, buffer.toString()));
		}
		final Collection<MetricLocation> result = new ArrayList<>(patterns.size());
		for (String metric : preprocessedPatterns) {
			result.add(new MetricLocation(true, metric));
		}
		return result;
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

	private List<String> includedMetaTables(List<String> metricMasks, String tablePattern) {
		final List<DefaultTable> availableDefaultTables = getAtsdRevision() >= META_COLUMNS_REVISION ?
				Arrays.asList(DefaultTable.values()) : Collections.singletonList(DefaultTable.ATSD_SERIES);
		final List<String> result = new ArrayList<>();
		for (DefaultTable table : availableDefaultTables) {
			for (String metricMask : metricMasks) {
				if (WildcardsUtil.wildcardMatch(table.tableName, metricMask) &&
						WildcardsUtil.wildcardMatch(table.tableName, tablePattern)) {
					result.add(table.tableName);
					break;
				}
			}
		}
		return result;
	}

	@Override
	public MetaResultSet getColumns(ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern) {
        log.debug("[getColumns] connection: {} catalog: {} schemaPattern: {} tableNamePattern: {} columnNamePattern: {}", ch.id, catalog, schemaPattern,
                tableNamePattern, columnNamePattern);
        final List<String> metricMasks = atsdConnectionInfo.tables();
		if (!metricMasks.isEmpty()) {
			final String colNamePattern = columnNamePattern.s;
			final boolean addCreationTime = shouldAddCreationTimeMetaColumns();
			final List<MetadataColumnDefinition> columns = filterColumns(colNamePattern, atsdConnectionInfo.metaColumns(), addCreationTime);
			final List<Object> columnData = new ArrayList<>();
			final boolean underscoreAsLiteral = atsdConnectionInfo.disableUnderscoreInColumns();
			final String pattern = StringUtils.isBlank(schemaPattern.s) ? tableNamePattern.s : schemaPattern.s + '.' + tableNamePattern.s;
			final Map<String, AtsdType> tableNamesAndValueTypes = getAndFilterMetricsFromAtsd(metricMasks, pattern, underscoreAsLiteral);
			if (tableNamesAndValueTypes.isEmpty() && !WildcardsUtil.hasWildcards(pattern)) {
				tableNamesAndValueTypes.put(pattern, AtsdType.DEFAULT_VALUE_TYPE);
			}
			for (String metaTable : includedMetaTables(metricMasks, pattern)) {
				tableNamesAndValueTypes.put(metaTable, AtsdType.DEFAULT_VALUE_TYPE);
			}
			for (String metricMask : metricMasks) {
				if (!WildcardsUtil.hasWildcards(metricMask) && !tableNamesAndValueTypes.containsKey(metricMask)) {
					tableNamesAndValueTypes.put(WildcardsUtil.wildcardToTableName(metricMask), AtsdType.DEFAULT_VALUE_TYPE);
				}
			}
			final boolean odbcCompatible = atsdConnectionInfo.odbc2Compatibility();
			final boolean useShortNames = useShortColumnNamesInMetaTables();
			for (Map.Entry<String, AtsdType> entry : tableNamesAndValueTypes.entrySet()) {
				final String tableName = entry.getKey();
				final AtsdType metricValueType = entry.getValue();
				final List<MetadataColumnDefinition> columnDefinitions;
				if (DefaultTable.ATSD_METRIC.tableName.equals(tableName)) {
					columnDefinitions = filterMetaTablesColumns(colNamePattern, MetricColumn.values(addCreationTime), useShortNames);
				} else if (DefaultTable.ATSD_ENTITY.tableName.equals(tableName)) {
					columnDefinitions = filterMetaTablesColumns(colNamePattern, EntityColumn.values(addCreationTime), useShortNames);
				} else {
					columnDefinitions = columns;
				}
				int position = 1;
				final boolean cutMetaPrefix = useShortNames && columnDefinitions != columns;
				for (MetadataColumnDefinition column : columnDefinitions) {
					columnData.add(createColumnMetaData(column, tableName, metricValueType, position, odbcCompatible, cutMetaPrefix));
					++position;
				}
				appendFilteredTagsColumns(tableName, colNamePattern, columnData, odbcCompatible, metricValueType);
			}

			if (log.isDebugEnabled()) {
				log.debug("[getColumns] count: {}", columnData.size());
				log.debug("[getColumns] columns: {}", buildColumnsStringForDebug(columnData));
			}
			return getResultSet(columnData, AtsdMetaResultSets.AtsdMetaColumn.class);
		}
		return createEmptyResultSet(AtsdMetaResultSets.AtsdMetaColumn.class);
	}

	private void appendFilteredTagsColumns(String tableName, String colNamePattern, List<Object> columnData,
										   boolean odbcCompatible, AtsdType metricValueType) {
        if (DefaultTable.isDefaultTable(tableName) || !maybeTagColumnPattern(colNamePattern)) {
            return;
        }
        final boolean useShortNames = false; // don't use in non-default tables
        final Set<String> tags = getTags(tableName);
        int position = columnData.size() + 1;
        if (tags.isEmpty() && StringUtils.startsWith(colNamePattern, TagColumn.PREFIX) && !WildcardsUtil.hasWildcards(colNamePattern)) {
            final TagColumn column = new TagColumn(StringUtils.substringAfter(colNamePattern, TagColumn.PREFIX));
            columnData.add(createColumnMetaData(column, tableName, metricValueType, position, odbcCompatible, useShortNames));
        } else {
            for (String tag : tags) {
                final TagColumn column = new TagColumn(tag);
                if (WildcardsUtil.wildcardMatch(column.getColumnNamePrefix(), colNamePattern)) {
                    columnData.add(createColumnMetaData(column, tableName, metricValueType, position, odbcCompatible, useShortNames));
                    ++position;
                }
            }
        }
    }

	private static String buildColumnsStringForDebug(List<Object> columnData) {
		StringBuilder buffer = new StringBuilder();
		buffer.append('[');
		AtsdMetaResultSets.AtsdMetaColumn metaColumn;
		for (Object column : columnData) {
			metaColumn = (AtsdMetaResultSets.AtsdMetaColumn) column;
			if(buffer.length() > 1) {
				buffer.append(',');
			}
			buffer.append("{name=").append(metaColumn.columnName).append(", ").append("type=").append(metaColumn.typeName).append('}');
		}
		buffer.append(']');
		return buffer.toString();
	}

	private static boolean maybeTagColumnPattern(String pattern) {
		return WildcardsUtil.hasWildcards(pattern) || pattern.startsWith(TagColumn.PREFIX)
                || pattern.startsWith(EntityColumn.TAGS.getColumnNamePrefix())
                || pattern.startsWith(MetricColumn.TAGS.getColumnNamePrefix());
	}

	private static List<MetadataColumnDefinition> filterColumns(String columnPattern, boolean showMetaColumns, boolean addCreationTime) {
		List<MetadataColumnDefinition> result = new ArrayList<>();
		for (DefaultColumn column : DefaultColumn.values()) {
			filterColumn(columnPattern, column.getColumnNamePrefix(), column, result);
		}
		if (showMetaColumns) {
			for (EntityColumn column : EntityColumn.values(addCreationTime)) {
				filterColumn(columnPattern, column.getColumnNamePrefix(), column, result);
			}
			for (MetricColumn column : MetricColumn.values(addCreationTime)) {
				filterColumn(columnPattern, column.getColumnNamePrefix(), column, result);
			}
		}
		return result;
	}

	private static List<MetadataColumnDefinition> filterMetaTablesColumns(String columnPattern, MetadataColumnDefinition[] unfiltered, boolean useShortNames) {
		List<MetadataColumnDefinition> result = new ArrayList<>();
		for (MetadataColumnDefinition columnDefinition : unfiltered) {
			final String columnNamePrefix = useShortNames ? columnDefinition.getShortColumnNamePrefix() : columnDefinition.getColumnNamePrefix();
			filterColumn(columnPattern, columnNamePrefix, columnDefinition, result);
		}
		return result;
	}

	private static void filterColumn(String columnPattern, String columnNamePrefix, MetadataColumnDefinition column,
									 final List<MetadataColumnDefinition> columns) {
		if (WildcardsUtil.wildcardMatch(columnNamePrefix, columnPattern)) {
			columns.add(column);
		}
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

	private String toSeriesEndpoint(AtsdConnectionInfo connectionInfo, String metric) {
		final String encodedMetric = DbMetadataUtils.urlEncode(metric);
		final String baseMetricUrl = Location.METRICS_ENDPOINT.getUrl(connectionInfo);
		return baseMetricUrl + "/" + encodedMetric + "/series";
	}

	private AtsdMetaResultSets.AtsdMetaColumn createColumnMetaData(MetadataColumnDefinition column, String table, AtsdType valueType,
																   int ordinal, boolean odbcCompatible, boolean useShortNames) {
		final AtsdType columnType = column.getType(valueType).getCompatibleType(odbcCompatible);
		return new AtsdMetaResultSets.AtsdMetaColumn(
				odbcCompatible,
				atsdConnectionInfo.catalog(),
				atsdConnectionInfo.schema(),
				table,
				useShortNames ? column.getShortColumnNamePrefix() : column.getColumnNamePrefix(),
				columnType,
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

	private IDataProvider createDataProvider(StatementHandle statementHandle, String sql, StatementType statementType, boolean encodeTags) throws UnsupportedEncodingException {
		final StatementContext newContext = new StatementContext(statementHandle, encodeTags);
		contextMap.put(statementHandle.id, newContext);
		final IDataProvider dataProvider = new DataProvider(atsdConnectionInfo, sql, newContext, statementType);
		providerCache.put(statementHandle.id, dataProvider);
		return dataProvider;
	}

	private ContentMetadata createMetadata(String sql, String connectionId, int statementId)
			throws AtsdException, IOException {
		IDataProvider provider = providerCache.get(statementId);
		final String jsonScheme = provider != null ? provider.getContentDescription().getJsonScheme() : "";
		ContentMetadata contentMetadata = new ContentMetadata(jsonScheme, sql, atsdConnectionInfo.catalog(),
				connectionId, statementId, atsdConnectionInfo.assignColumnNames(), atsdConnectionInfo.odbc2Compatibility());
		metaCache.put(statementId, contentMetadata);
		return contentMetadata;
	}

	private AtsdMetaResultSets.AtsdMetaTypeInfo getTypeInfo(AtsdType atsdType) {
		return new AtsdMetaResultSets.AtsdMetaTypeInfo(atsdConnectionInfo.odbc2Compatibility(), atsdType, DatabaseMetaData.typeNullable,
				DatabaseMetaData.typeSearchable, false, false, false, 0,  0);
	}

	// Since Calcite 1.6.0

	@Override
	public void commit(ConnectionHandle handle) {
		log.debug("[commit] {} -> {}", handle.id, handle);
	}

	@Override
	public void rollback(ConnectionHandle handle) {
		log.debug("[rollback] {} -> {}", handle.id, handle);
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
				result.add(TIMESTAMP_PRINTER.format((Date) value));
			} else {
				result.add(value == null ? null : String.valueOf(value));
			}
		}
		log.debug("[preparedValues] {}", result);
		return result;
	}

	@Override
	public StatementHandle createStatement(ConnectionHandle connectionHandle) {
		return new StatementHandle(connectionHandle.id, idGenerator.getAndIncrement(), null);
	}

	@Getter
	@AllArgsConstructor
	static class MetricLocation {
		private final boolean returnSingleElement;
		private final String expression;

		private String toEndpointUrl(AtsdConnectionInfo connectionInfo, int atsdRevision) {
			final String encodedExpression = DbMetadataUtils.urlEncode(expression);
			final String addInsertTimeParam = addInsertTimeParamName(atsdRevision);
			final String base = Location.METRICS_ENDPOINT.getUrl(connectionInfo);
			if (returnSingleElement) {
				return base + "/" + encodedExpression + "?" + addInsertTimeParam + "=false";
			} else {
				return base + "?" + addInsertTimeParam + "=false&expression=" + encodedExpression;
			}
		}

		private String addInsertTimeParamName(int revision) {
			final int threshold = returnSingleElement ? API_METRIC_STATISTICS_RENAME_REV : API_FIND_METRICS_STATISTICS_RENAME_REV;
			return revision < threshold ? "statistics" : "addInsertTime";
		}
	}
}
