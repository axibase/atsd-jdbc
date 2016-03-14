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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.commons.lang3.StringUtils;

import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;

@SuppressWarnings("unchecked")
public class ContentMetadata {
	private static final LoggingFacade logger = LoggingFacade.getLogger(ContentMetadata.class);

	private final Signature sign;
	private final List<MetaResultSet> list;
	private final List<ColumnMetaData> metadataList;

	public ContentMetadata(String scheme, String sql, String connectionId, int statementId)
			throws AtsdException, IOException {
		metadataList = !StringUtils.isEmpty(scheme) ? buildMetadataList(scheme)
				: Collections.<ColumnMetaData> emptyList();
		sign = new Signature(metadataList, sql, Collections.<AvaticaParameter> emptyList(), null, CursorFactory.LIST,
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

	static List<ColumnMetaData> buildMetadataList(String json)
			throws JsonParseException, MalformedURLException, IOException, AtsdException {
		final Object jsonObject = getJsonScheme(json);
		if (!(jsonObject instanceof Map))
			throw new AtsdException("Wrong metadata content");
		final Map<String, Object> map = (Map<String, Object>) jsonObject;
		final Map<String, Object> publisher = (Map<String, Object>) map.get(PUBLISHER_SECTION);
		if (publisher == null)
			throw new AtsdException("Wrong metadata publisher");
		String schema = (String) publisher.get(SCHEMA_NAME_PROPERTY);
		if (schema == null)
			throw new AtsdException("Wrong metadata schema");
		final Map<String, Object> tableSchema = (Map<String, Object>) map.get(TABLE_SCHEMA_SECTION);
		if (tableSchema == null)
			throw new AtsdException("Wrong table schema");
		final List<Object> columns = (List<Object>) tableSchema.get(COLUMNS_SCHEME);
		if (columns == null)
			throw new AtsdException("Wrong columns schema");
		final List<ColumnMetaData> metadataList = new ArrayList<>();
		int ind = 1;
		for (final Object obj : columns) {
			final Map<String, Object> property = (Map<String, Object>) obj;
			String name = (String) property.get(NAME_PROPERTY);
			String title = (String) property.get(TITLE_PROPERTY);
			String table = (String) property.get(TABLE_PROPERTY);
			String datatype = (String) property.get(DATATYPE_PROPERTY);
			Integer index = (Integer) property.get(INDEX_PROPERTY);
			final ColumnMetaData.AvaticaType atype = getAvaticaType(datatype);
			final ColumnMetaData cmd = new ColumnMetaData(index != null ? index.intValue() : ind++, false, false, false, false, 0, false, 10, name, title,
					schema, 1, 1, table, DEFAULT_CATALOG_NAME, atype, true, false, false,
					atype.rep.clazz.getCanonicalName());
			metadataList.add(cmd);
		}
		if(logger.isDebugEnabled())
			logger.debug("Schema is processed: " + metadataList.size());
		return Collections.unmodifiableList(metadataList);
	}

	private static Map<String, Object> getJsonScheme(String json) throws IOException {
		final MappingJsonFactory jsonFactory = new MappingJsonFactory();
		try (final InputStream is = new ByteArrayInputStream(json.getBytes(Charset.defaultCharset()));
				final JsonParser parser = jsonFactory.createParser(is);) {
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

	private static ColumnMetaData.AvaticaType getAvaticaType(String datatype) {
		int metaType;
		final Rep rep;
		switch (datatype) {
		case STRING_DATA_TYPE:
			metaType = Types.VARCHAR;
			rep = ColumnMetaData.Rep.STRING;
			break;
		case SHORT_DATA_TYPE:
			metaType = Types.SMALLINT;
			rep = ColumnMetaData.Rep.SHORT;
			break;
		case INTEGER_DATA_TYPE:
			metaType = Types.INTEGER;
			rep = ColumnMetaData.Rep.INTEGER;
			break;
		case LONG_DATA_TYPE:
			metaType = Types.BIGINT;
			rep = ColumnMetaData.Rep.LONG;
			break;
		case FLOAT_DATA_TYPE:
			metaType = Types.FLOAT;
			rep = ColumnMetaData.Rep.FLOAT;
			break;
		case DOUBLE_DATA_TYPE:
			metaType = Types.DOUBLE;
			rep = ColumnMetaData.Rep.DOUBLE;
			break;
		case TIME_STAMP_DATA_TYPE:
			metaType = Types.TIMESTAMP;
			rep = ColumnMetaData.Rep.JAVA_SQL_TIMESTAMP;
			break;
		default:
			metaType = Types.VARCHAR;
			rep = ColumnMetaData.Rep.STRING;
		}
		return new ColumnMetaData.AvaticaType(metaType, datatype, rep);
	}

	private static final String TIME_STAMP_DATA_TYPE = "xsd:dateTimeStamp";
	private static final String DOUBLE_DATA_TYPE = "double";
	private static final String FLOAT_DATA_TYPE = "float";
	private static final String LONG_DATA_TYPE = "long";
	private static final String INTEGER_DATA_TYPE = "integer";
	private static final String SHORT_DATA_TYPE = "short";
	private static final String STRING_DATA_TYPE = "string";
	private static final String COLUMNS_SCHEME = "columns";
	private static final String DEFAULT_CATALOG_NAME = "axiCatalog";
	private static final String DATATYPE_PROPERTY = "datatype";
	private static final String INDEX_PROPERTY = "columnIndex";
	private static final String TABLE_PROPERTY = "table";
	private static final String TITLE_PROPERTY = "titles";
	private static final String NAME_PROPERTY = "name";
	private static final String SCHEMA_NAME_PROPERTY = "schema:name";
	private static final String TABLE_SCHEMA_SECTION = "tableSchema";
	private static final String PUBLISHER_SECTION = "dc:publisher";
}
