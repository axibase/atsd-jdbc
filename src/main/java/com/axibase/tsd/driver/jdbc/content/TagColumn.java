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
import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;

public class TagColumn implements MetadataColumnDefinition {

	public static final String PREFIX = "tags.";

	private final String tagName;

	public TagColumn(String tagName) {
		this.tagName = tagName;
	}

	@Override
	public AtsdType getType(AtsdType metricType) {
		return AtsdType.STRING_DATA_TYPE;
	}

	@Override
	public String getColumnNamePrefix() {
		return PREFIX + "'" + tagName + "'";
	}

	@Override
	public int getNullable() {
		return 1;
	}

	@Override
	public String getNullableAsString() {
		return "TRUE";
	}

	@Override
	public boolean isMetaColumn() {
		return false;
	}
}
