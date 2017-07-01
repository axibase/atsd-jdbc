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
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.com.fasterxml.jackson.core.JsonParseException;
import org.apache.calcite.avatica.com.fasterxml.jackson.core.JsonParser;
import org.apache.calcite.avatica.com.fasterxml.jackson.core.JsonToken;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.axibase.tsd.driver.jdbc.DriverConstants.*;

@SuppressWarnings("unchecked")
public class ContentMetadata {
	private static final LoggingFacade logger = LoggingFacade.getLogger(ContentMetadata.class);

	private final Signature sign;
	private final List<MetaResultSet> list;
	private final List<ColumnMetaData> metadataList;

	public ContentMetadata(String scheme, String sql, String catalog, String connectionId, int statementId)
			throws AtsdException, IOException {
		metadataList = StringUtils.isNotEmpty(scheme) ? buildMetadataList(scheme, catalog)
				: Collections.<ColumnMetaData>emptyList();
		sign = new Signature(metadataList, sql, Collections.<AvaticaParameter>emptyList(), null, CursorFactory.LIST,
				StatementType.SELECT);
		list = Collections.unmodifiableList(
				Collections.singletonList(MetaResultSet.create(connectionId, statementId, false, sign, null)));
	}

	public Signature getSign() {
		return sign;
	}

	public List<MetaResultSet> getList() {
		return list;
	}

	public List<ColumnMetaData> getMetadataList() {
		return metadataList;
	}

	static List<ColumnMetaData> buildMetadataList(String json, String catalog)
			throws JsonParseException, MalformedURLException, IOException, AtsdException {
		final Map<String, Object> jsonObject = getJsonScheme(json);
		if (jsonObject == null) {
			throw new AtsdException("Wrong metadata content");
		}
		final Map<String, Object> publisher = (Map<String, Object>) jsonObject.get(PUBLISHER_SECTION);
		if (publisher == null) {
			throw new AtsdException("Wrong metadata publisher");
		}
		final String schema = (String) publisher.get(SCHEMA_NAME_PROPERTY);
		if (schema == null) {
			throw new AtsdException("Wrong metadata schema");
		}
		final Map<String, Object> tableSchema = (Map<String, Object>) jsonObject.get(TABLE_SCHEMA_SECTION);
		if (tableSchema == null) {
			throw new AtsdException("Wrong table schema");
		}
		final List<Object> columns = (List<Object>) tableSchema.get(COLUMNS_SCHEME);
		if (columns == null) {
			throw new AtsdException("Wrong columns schema");
		}
		final int size = columns.size();
		ColumnMetaData[] sortedByOrdinal = new ColumnMetaData[size];
		int index = 0;
		for (final Object obj : columns) {
			final ColumnMetaData cmd = getColumnMetaData(schema, catalog, index, obj);
			sortedByOrdinal[cmd.ordinal] = cmd;
			++index;
		}
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Schema is processed. %s headers are found.", size));
		}
		return Collections.unmodifiableList(Arrays.asList(sortedByOrdinal));
	}

	private static ColumnMetaData getColumnMetaData(String schema, String catalog, int ind, final Object obj) {
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
		return new ColumnMetaDataBuilder()
				.withColumnIndex(columnIndex)
				.withSchema(schema)
				.withCatalog(catalog)
				.withTable(table)
				.withName(name)
				.withTitle(title)
				.withAtsdType(atsdType)
				.withNullable(nullable ? 1 : 0)
				.build();
	}

	private static Map<String, Object> getJsonScheme(String json) throws IOException {
		try (final JsonParser parser = JsonMappingUtil.getParser(json)) {
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

	public static ColumnMetaData.AvaticaType getAvaticaType(AtsdType type) {
		return new ColumnMetaData.AvaticaType(type.sqlTypeCode, type.sqlType, type.avaticaType);
	}

	public static class ColumnMetaDataBuilder {
		private String name;
		private String title;
		private String table;
		private AtsdType atsdType;

		private int columnIndex;
		private int nullable;
		private String schema;
		private String catalog;

		public ColumnMetaDataBuilder withName(String name) {
			this.name = name;
			return this;
		}

		public ColumnMetaDataBuilder withTitle(String title) {
			this.title = title;
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
			final ColumnMetaData.AvaticaType atype = getAvaticaType(atsdType);
			return new ColumnMetaData(columnIndex, false, false, false,
					false, nullable, false, atsdType.size, name, title, schema, 1, 1, table, catalog, atype,
					true, false, false, atype.rep.clazz.getCanonicalName());
		}
	}

}
