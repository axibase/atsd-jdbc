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
    "@context",
    "dc:created",
    "dc:publisher",
    "dc:title",
    "rdfs:comment",
    "@type",
    "url",
    "tableSchema",
    "dialect"
})
public class MetadataSchema {
    @JsonProperty("@context")
    private List<String> Context = new ArrayList<>();
    @JsonProperty("dc:created")
    private DcCreated dcCreated;
    @JsonProperty("dc:publisher")
    private DcPublisher dcPublisher;
    @JsonProperty("dc:title")
    private String dcTitle;
    @JsonProperty("rdfs:comment")
    private String rdfsComment;
    @JsonProperty("@type")
    private String Type;
    @JsonProperty("url")
    private String url;
    @JsonProperty("tableSchema")
    private TableSchema tableSchema;
    @JsonProperty("dialect")
    private Dialect dialect;
}
