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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.*;


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
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("commentPrefix")
    public String getCommentPrefix() {
        return commentPrefix;
    }

    @JsonProperty("commentPrefix")
    public void setCommentPrefix(String commentPrefix) {
        this.commentPrefix = commentPrefix;
    }

    public Dialect withCommentPrefix(String commentPrefix) {
        this.commentPrefix = commentPrefix;
        return this;
    }

    @JsonProperty("delimiter")
    public String getDelimiter() {
        return delimiter;
    }

    @JsonProperty("delimiter")
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public Dialect withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    @JsonProperty("doubleQuote")
    public boolean isDoubleQuote() {
        return doubleQuote;
    }

    @JsonProperty("doubleQuote")
    public void setDoubleQuote(boolean doubleQuote) {
        this.doubleQuote = doubleQuote;
    }

    public Dialect withDoubleQuote(boolean doubleQuote) {
        this.doubleQuote = doubleQuote;
        return this;
    }

    @JsonProperty("quoteChar")
    public String getQuoteChar() {
        return quoteChar;
    }

    @JsonProperty("quoteChar")
    public void setQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
    }

    public Dialect withQuoteChar(String quoteChar) {
        this.quoteChar = quoteChar;
        return this;
    }

    @JsonProperty("headerRowCount")
    public int getHeaderRowCount() {
        return headerRowCount;
    }

    @JsonProperty("headerRowCount")
    public void setHeaderRowCount(int headerRowCount) {
        this.headerRowCount = headerRowCount;
    }

    public Dialect withHeaderRowCount(int headerRowCount) {
        this.headerRowCount = headerRowCount;
        return this;
    }

    @JsonProperty("encoding")
    public String getEncoding() {
        return encoding;
    }

    @JsonProperty("encoding")
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public Dialect withEncoding(String encoding) {
        this.encoding = encoding;
        return this;
    }

    @JsonProperty("header")
    public boolean isHeader() {
        return header;
    }

    @JsonProperty("header")
    public void setHeader(boolean header) {
        this.header = header;
    }

    public Dialect withHeader(boolean header) {
        this.header = header;
        return this;
    }

    @JsonProperty("lineTerminators")
    public List<String> getLineTerminators() {
        return lineTerminators;
    }

    @JsonProperty("lineTerminators")
    public void setLineTerminators(List<String> lineTerminators) {
        this.lineTerminators = lineTerminators;
    }

    public Dialect withLineTerminators(List<String> lineTerminators) {
        this.lineTerminators = lineTerminators;
        return this;
    }

    @JsonProperty("skipBlankRows")
    public boolean isSkipBlankRows() {
        return skipBlankRows;
    }

    @JsonProperty("skipBlankRows")
    public void setSkipBlankRows(boolean skipBlankRows) {
        this.skipBlankRows = skipBlankRows;
    }

    public Dialect withSkipBlankRows(boolean skipBlankRows) {
        this.skipBlankRows = skipBlankRows;
        return this;
    }

    @JsonProperty("skipColumns")
    public int getSkipColumns() {
        return skipColumns;
    }

    @JsonProperty("skipColumns")
    public void setSkipColumns(int skipColumns) {
        this.skipColumns = skipColumns;
    }

    public Dialect withSkipColumns(int skipColumns) {
        this.skipColumns = skipColumns;
        return this;
    }

    @JsonProperty("skipRows")
    public int getSkipRows() {
        return skipRows;
    }

    @JsonProperty("skipRows")
    public void setSkipRows(int skipRows) {
        this.skipRows = skipRows;
    }

    public Dialect withSkipRows(int skipRows) {
        this.skipRows = skipRows;
        return this;
    }

    @JsonProperty("skipInitialSpace")
    public boolean isSkipInitialSpace() {
        return skipInitialSpace;
    }

    @JsonProperty("skipInitialSpace")
    public void setSkipInitialSpace(boolean skipInitialSpace) {
        this.skipInitialSpace = skipInitialSpace;
    }

    public Dialect withSkipInitialSpace(boolean skipInitialSpace) {
        this.skipInitialSpace = skipInitialSpace;
        return this;
    }

    @JsonProperty("trim")
    public boolean isTrim() {
        return trim;
    }

    @JsonProperty("trim")
    public void setTrim(boolean trim) {
        this.trim = trim;
    }

    public Dialect withTrim(boolean trim) {
        this.trim = trim;
        return this;
    }

    @JsonProperty("@type")
    public String getType() {
        return Type;
    }

    @JsonProperty("@type")
    public void setType(String Type) {
        this.Type = Type;
    }

    public Dialect withType(String Type) {
        this.Type = Type;
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

    public Dialect withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

}
