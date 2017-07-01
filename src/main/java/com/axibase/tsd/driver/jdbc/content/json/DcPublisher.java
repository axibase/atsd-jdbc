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
    "schema:name",
    "schema:url"
})
public class DcPublisher {

    @JsonProperty("schema:name")
    private String schemaName;
    @JsonProperty("schema:url")
    private SchemaUrl schemaUrl;
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<String, Object>();
    @JsonProperty("schema:name")
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty("schema:name")
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public DcPublisher withSchemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    @JsonProperty("schema:url")
    public SchemaUrl getSchemaUrl() {
        return schemaUrl;
    }
    
    @JsonProperty("schema:url")
    public void setSchemaUrl(SchemaUrl schemaUrl) {
        this.schemaUrl = schemaUrl;
    }

    public DcPublisher withSchemaUrl(SchemaUrl schemaUrl) {
        this.schemaUrl = schemaUrl;
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

    public DcPublisher withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

}
