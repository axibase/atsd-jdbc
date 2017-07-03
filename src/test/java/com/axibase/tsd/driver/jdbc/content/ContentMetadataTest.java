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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Scanner;

import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import org.apache.calcite.avatica.ColumnMetaData;
import org.junit.Test;

import static org.junit.Assert.*;

public class ContentMetadataTest {
	private static final String CONTEXT_START = "{\n   \"@context\":[";
	private static final String MCN_JSON_SCHEMA = "/json/mpstat_cpu_busy.jsonld";

	@Test
	public final void testGetMetadataList() {
		checkMetadataList(MCN_JSON_SCHEMA, 3);
	}

	private void checkMetadataList(String schema, int expectedSize) {
		try (final InputStream is = this.getClass().getResourceAsStream(schema);
			 final Scanner scanner = new Scanner(is)) {
			scanner.useDelimiter("\\A");
			String json = scanner.hasNext() ? scanner.next() : "";
			assertTrue(json != null && json.length() != 0 && json.startsWith(CONTEXT_START));
			final List<ColumnMetaData> metadataList = ContentMetadata.buildMetadataList(json, null, false);
			assertEquals(expectedSize, metadataList.size());
			final String[] expectedColumnNames = {"datetime", "value", "entity"};
			for (int i = 0; i < metadataList.size(); i++) {
				assertEquals(expectedColumnNames[i], metadataList.get(i).columnName);
			}
		} catch (final IOException | AtsdException e) {
			fail(e.getMessage());
		}
	}

}