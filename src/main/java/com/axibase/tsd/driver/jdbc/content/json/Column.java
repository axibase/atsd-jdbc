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

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.*;


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
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("columnIndex")
    public int getColumnIndex() {
        return columnIndex;
    }

    @JsonProperty("columnIndex")
    public void setColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
    }

    public Column withColumnIndex(int columnIndex) {
        this.columnIndex = columnIndex;
        return this;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    public Column withName(String name) {
        this.name = name;
        return this;
    }

    @JsonProperty("titles")
    public String getTitles() {
        return titles;
    }

    @JsonProperty("titles")
    public void setTitles(String titles) {
        this.titles = titles;
    }

    public Column withTitles(String titles) {
        this.titles = titles;
        return this;
    }

    @JsonProperty("datatype")
    public String getDatatype() {
        return datatype;
    }

    @JsonProperty("datatype")
    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public Column withDatatype(String datatype) {
        this.datatype = datatype;
        return this;
    }

    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    @JsonProperty("table")
    public void setTable(String table) {
        this.table = table;
    }

    public Column withTable(String table) {
        this.table = table;
        return this;
    }

    @JsonProperty("propertyUrl")
    public String getPropertyUrl() {
        return propertyUrl;
    }

    @JsonProperty("propertyUrl")
    public void setPropertyUrl(String propertyUrl) {
        this.propertyUrl = propertyUrl;
    }

    public Column withPropertyUrl(String propertyUrl) {
        this.propertyUrl = propertyUrl;
        return this;
    }

    @JsonProperty("dc:description")
    public String getDcDescription() {
        return dcDescription;
    }

    @JsonProperty("dc:description")
    public void setDcDescription(String dcDescription) {
        this.dcDescription = dcDescription;
    }

    public Column withDcDescription(String dcDescription) {
        this.dcDescription = dcDescription;
        return this;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    public Column withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

}
