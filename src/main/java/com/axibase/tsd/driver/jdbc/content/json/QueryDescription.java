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
package com.axibase.tsd.driver.jdbc.content.json;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ToString
public class QueryDescription {
	@JsonProperty("queryId")
	private String queryId;
	@JsonProperty("atsdQueryId")
	private String atsdQueryId;
	@JsonProperty("status")
	private String status;
	@JsonProperty("user")
	private String user;
	@JsonProperty("sql")
	private String sql;
	@JsonIgnore
	private final Map<String, Object> additionalProperties = new HashMap<>();
}
