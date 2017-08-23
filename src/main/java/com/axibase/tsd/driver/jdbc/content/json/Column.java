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
import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.JsonPropertyOrder;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "columnIndex",
    "name",
    "titles",
    "datatype",
    "table",
    "propertyUrl",
    "dc:description"
})
public class Column {
    @JsonProperty("columnIndex")
    private int columnIndex;
    @JsonProperty("name")
    private String name;
    @JsonProperty("titles")
    private String titles;
    @JsonProperty("datatype")
    private String datatype;
    @JsonProperty("table")
    private String table;
    @JsonProperty("propertyUrl")
    private String propertyUrl;
    @JsonProperty("dc:description")
    private String dcDescription;
}
