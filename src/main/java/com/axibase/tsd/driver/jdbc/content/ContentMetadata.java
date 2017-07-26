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
package com.axibase.tsd.driver.jdbc.content;

import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.ext.AtsdJsonException;
import com.axibase.tsd.driver.jdbc.ext.AtsdMetaResultSets;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import lombok.Getter;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.com.fasterxml.jackson.core.JsonParser;
import org.apache.calcite.avatica.com.fasterxml.jackson.core.JsonToken;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.driver.jdbc.DriverConstants.*;

@SuppressWarnings("unchecked")
@Getter
public class ContentMetadata {
	private static final LoggingFacade logger = LoggingFacade.getLogger(ContentMetadata.class);

	private final Signature sign;
	private final List<MetaResultSet> list;
	private final List<ColumnMetaData> metadataList;

	public ContentMetadata(String scheme, String sql, String catalog, String connectionId, int statementId, boolean assignColumnNames, boolean odbcCompatible)
			throws AtsdException, IOException {
		metadataList = StringUtils.isNotEmpty(scheme) ? buildMetadataList(scheme, catalog, assignColumnNames, odbcCompatible)
				: Collections.<ColumnMetaData>emptyList();
		sign = new Signature(metadataList, sql, Collections.<AvaticaParameter>emptyList(), null, CursorFactory.LIST,
				StatementType.SELECT);
		list = Collections.unmodifiableList(
				Collections.singletonList(MetaResultSet.create(connectionId, statementId, false, sign, null)));
	}

	public static List<ColumnMetaData> buildMetadataList(InputStream jsonInputStream, String catalog, boolean assignColumnNames, boolean odbcCompatible)
			throws IOException, AtsdException {
		return buildMetadataList(new InputStreamReader(jsonInputStream), catalog, assignColumnNames, odbcCompatible);
	}

	public static List<ColumnMetaData> buildMetadataList(String json, String catalog, boolean assignColumnNames, boolean odbcCompatible)
			throws IOException, AtsdException {
		return buildMetadataList(new StringReader(json), catalog, assignColumnNames, odbcCompatible);
	}

	private static List<ColumnMetaData> buildMetadataList(Reader jsonReader, String catalog, boolean assignColumnNames, boolean odbcCompatible)
			throws IOException, AtsdException {
		final Map<String, Object> jsonObject = getJsonScheme(jsonReader);
		if (jsonObject == null) {
			throw new AtsdException("Wrong metadata content");
		}
		final Map<String, Object> publisher = (Map<String, Object>) jsonObject.get(PUBLISHER_SECTION);
		if (publisher == null) {
			throw new AtsdJsonException("Wrong metadata publisher", jsonObject);
		}
		final String schema = null;
		final Map<String, Object> tableSchema = (Map<String, Object>) jsonObject.get(TABLE_SCHEMA_SECTION);
		if (tableSchema == null) {
			throw new AtsdJsonException("Wrong table schema", jsonObject);
		}
		final List<Object> columns = (List<Object>) tableSchema.get(COLUMNS_SCHEME);
		if (columns == null) {
			throw new AtsdJsonException("Wrong columns schema", jsonObject);
		}
		final int size = columns.size();
		ColumnMetaData[] sortedByOrdinal = new ColumnMetaData[size];
		int index = 0;
		for (final Object obj : columns) {
			final ColumnMetaData cmd = getColumnMetaData(schema, catalog, index, obj, assignColumnNames, odbcCompatible);
			sortedByOrdinal[cmd.ordinal] = cmd;
			++index;
		}
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Schema is processed. %s headers are found.", size));
		}
		return Collections.unmodifiableList(Arrays.asList(sortedByOrdinal));
	}

	private static ColumnMetaData getColumnMetaData(String schema, String catalog, int ind, final Object obj,
													boolean assignColumnNames, boolean odbcCompatible) {
		final Map<String, Object> property = (Map<String, Object>) obj;
		final Integer index = (Integer) property.get(INDEX_PROPERTY);
		final int columnIndex = index != null ? index - 1 : ind;
		final String name = (String) property.get(NAME_PROPERTY);
		final String title = (String) property.get(TITLE_PROPERTY);
		final String table = (String) property.get(TABLE_PROPERTY);
		final String datatype = property.get(DATATYPE_PROPERTY).toString(); // may be represented as a json object (hashmap)
		final String propertyUrl = (String) property.get(PROPERTY_URL);
		final AtsdType atsdType = EnumUtil.getAtsdTypeByOriginalName(datatype);
		final boolean nullable = atsdType == AtsdType.JAVA_OBJECT_TYPE || (atsdType == AtsdType.STRING_DATA_TYPE
				&& (StringUtils.endsWithIgnoreCase(propertyUrl, "Tag") || TEXT_TITLES.equals(title)));
		return new ColumnMetaDataBuilder(assignColumnNames, odbcCompatible)
				.withColumnIndex(columnIndex)
				.withSchema(schema)
				.withCatalog(catalog)
				.withTable(table)
				.withName(name)
				.withLabel(title)
				.withAtsdType(atsdType)
				.withNullable(nullable ? 1 : 0)
				.build();
	}

	private static Map<String, Object> getJsonScheme(Reader jsonReader) throws IOException {
		try (final JsonParser parser = JsonMappingUtil.getParser(jsonReader)) {
			final JsonToken token = parser.nextToken();
			Class<?> type;
			if (token == JsonToken.START_OBJECT) {
				type = Map.class;
			} else if (token == JsonToken.START_ARRAY) {
				type = List.class;
			} else {
				type = String.class;
			}
			return (Map<String, Object>) parser.readValueAs(type);
		}
	}

	public static class ColumnMetaDataBuilder {
		private final boolean assignColumnNames;
		private final boolean odbcCompatible;
		private String name;
		private String label;
		private String table;
		private AtsdType atsdType;

		private int columnIndex;
		private int nullable;
		private String schema;
		private String catalog;

		public ColumnMetaDataBuilder(boolean assignColumnNames, boolean odbcCompatible) {
			this.assignColumnNames = assignColumnNames;
			this.odbcCompatible = odbcCompatible;
		}

		public ColumnMetaDataBuilder withName(String name) {
			this.name = name;
			return this;
		}

		public ColumnMetaDataBuilder withLabel(String label) {
			this.label = label;
			return this;
		}

		public ColumnMetaDataBuilder withTable(String table) {
			this.table = table;
			return this;
		}

		public ColumnMetaDataBuilder withAtsdType(AtsdType atsdType) {
			this.atsdType = atsdType;
			return this;
		}

		public ColumnMetaDataBuilder withColumnIndex(int columnIndex) {
			this.columnIndex = columnIndex;
			return this;
		}

		public ColumnMetaDataBuilder withSchema(String schema) {
			this.schema = schema;
			return this;
		}

		public ColumnMetaDataBuilder withCatalog(String catalog) {
			this.catalog = catalog;
			return this;
		}

		public ColumnMetaDataBuilder withNullable(int nullable) {
			this.nullable = nullable;
			return this;
		}

		public ColumnMetaData build() {
			final AtsdType type = atsdType.getCompatibleType(odbcCompatible);
			final ColumnMetaData.AvaticaType internalType = type.getAvaticaType(false);
			final ColumnMetaData.AvaticaType exposedType = type.getAvaticaType(odbcCompatible);
			return new AtsdMetaResultSets.AtsdColumnMetaData(columnIndex, false, false, false,
					false, nullable, false, type.size, label, assignColumnNames ? name : label,
					getValueNotNull(schema), type.maxPrecision, type.scale, table, getValueNotNull(catalog), internalType,
					true, false,false, internalType.rep.clazz.getCanonicalName(), exposedType);
		}

		private AtsdType substituteForOdbcCompatibility(AtsdType type) {
			return odbcCompatible && type == AtsdType.BIGINT_DATA_TYPE ? AtsdType.DOUBLE_DATA_TYPE : type;
		}

		private String getValueNotNull(String value) {
			return value == null ? "" : value;
		}
	}

}
