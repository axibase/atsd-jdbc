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
package com.axibase.tsd.driver.jdbc.enums;

import com.axibase.tsd.driver.jdbc.intf.MetadataColumnDefinition;
import com.axibase.tsd.driver.jdbc.util.AtsdColumn;
import lombok.AccessLevel;
import lombok.Getter;

@Getter
public enum DefaultColumn implements MetadataColumnDefinition {
	TIME(AtsdColumn.TIME, AtsdType.BIGINT_DATA_TYPE, 0, false),
	DATETIME(AtsdColumn.DATETIME, AtsdType.TIMESTAMP_DATA_TYPE, 0, false),
	VALUE(AtsdColumn.VALUE, null, 0, false) {
		@Override
		public AtsdType getType(AtsdType metricType) {
			return metricType;
		}
	},
	TEXT(AtsdColumn.TEXT, AtsdType.STRING_DATA_TYPE, 1, false),
	METRIC(AtsdColumn.METRIC, AtsdType.STRING_DATA_TYPE, 0, false),
	ENTITY(AtsdColumn.ENTITY, AtsdType.STRING_DATA_TYPE, 0, false),
	TAGS(AtsdColumn.TAGS, AtsdType.STRING_DATA_TYPE, 1, false);

	private final String columnNamePrefix;
	@Getter(AccessLevel.NONE)
	private final AtsdType type;
	private final int nullable;
	private final boolean metaColumn;

	DefaultColumn(String prefix, AtsdType type, int nullable, boolean metaColumn) {
		this.columnNamePrefix = prefix;
		this.type = type;
		this.nullable = nullable;
		this.metaColumn = metaColumn;
	}

    @Override
    public String getNullableAsString() {
		return NULLABLE_AS_STRING[nullable];
	}

    @Override
    public AtsdType getType(AtsdType metricType) {
		return type;
	}

	@Override
	public String getShortColumnNamePrefix() {
		return columnNamePrefix;
	}

}
