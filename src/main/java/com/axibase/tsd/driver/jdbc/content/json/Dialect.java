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

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "commentPrefix",
    "delimiter",
    "doubleQuote",
    "quoteChar",
    "headerRowCount",
    "encoding",
    "header",
    "lineTerminators",
    "skipBlankRows",
    "skipColumns",
    "skipRows",
    "skipInitialSpace",
    "trim",
    "@type"
})
public class Dialect {
    @JsonProperty("commentPrefix")
    private String commentPrefix;
    @JsonProperty("delimiter")
    private String delimiter;
    @JsonProperty("doubleQuote")
    private boolean doubleQuote;
    @JsonProperty("quoteChar")
    private String quoteChar;
    @JsonProperty("headerRowCount")
    private int headerRowCount;
    @JsonProperty("encoding")
    private String encoding;
    @JsonProperty("header")
    private boolean header;
    @JsonProperty("lineTerminators")
    private List<String> lineTerminators = new ArrayList<String>();
    @JsonProperty("skipBlankRows")
    private boolean skipBlankRows;
    @JsonProperty("skipColumns")
    private int skipColumns;
    @JsonProperty("skipRows")
    private int skipRows;
    @JsonProperty("skipInitialSpace")
    private boolean skipInitialSpace;
    @JsonProperty("trim")
    private boolean trim;
    @JsonProperty("@type")
    private String Type;
}
