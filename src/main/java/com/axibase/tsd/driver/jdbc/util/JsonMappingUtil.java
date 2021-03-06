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
package com.axibase.tsd.driver.jdbc.util;

import com.axibase.tsd.driver.jdbc.content.json.*;
import lombok.experimental.UtilityClass;
import org.apache.calcite.avatica.com.fasterxml.jackson.core.JsonParser;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.MappingJsonFactory;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.type.CollectionType;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

@UtilityClass
public class JsonMappingUtil {
	private static final ObjectMapper MAPPER = prepareObjectMapper();
	private static final ObjectReader READER = MAPPER.reader();
	private static final MappingJsonFactory JSON_FACTORY = new MappingJsonFactory(MAPPER);

	private static ObjectMapper prepareObjectMapper() {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return objectMapper;
	}

	public static Comments mapToComments(String json) throws IOException {
		return READER.forType(Comments.class).readValue(json);
	}

	public static QueryDescription[] mapToQueryDescriptionArray(InputStream jsonIs) throws IOException {
		return READER.forType(QueryDescription[].class).readValue(jsonIs);
	}

	public static String deserializeErrorObject(String errorJson) throws IOException {
		final GeneralError errorObject = READER.forType(GeneralError.class).readValue(errorJson);
		return errorObject == null ? null : errorObject.getError();
	}

	public static Version mapToVersion(InputStream jsonIs) throws IOException {
		return READER.forType(Version.class).readValue(jsonIs);
	}

	public static List<Metric> mapToMetrics(InputStream inputStream, boolean single) throws IOException {
		if (single) {
			final Metric metric = READER.forType(Metric.class).readValue(inputStream);
			return Collections.singletonList(metric);
		}
		final JsonParser parser = READER.getFactory().createParser(inputStream);
        final CollectionType collectionType = MAPPER.getTypeFactory().constructCollectionType(ArrayList.class, Metric.class);
        return READER.readValue(parser, collectionType);
	}

	public static Series[] mapToSeries(InputStream inputStream) throws IOException {
		return READER.forType(Series[].class).readValue(inputStream);
	}

	public static SendCommandResult mapToSendCommandResult(InputStream inputStream) throws IOException {
		return READER.forType(SendCommandResult.class).readValue(inputStream);
	}

	public static TreeMap<String, String> mapToTags(String jsonTags) throws IOException {
		return READER.forType(TreeMap.class).readValue(jsonTags);
	}

	public static JsonParser getParser(Reader reader) throws IOException {
		return JSON_FACTORY.createParser(reader);
	}
}
