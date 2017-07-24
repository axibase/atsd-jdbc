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
    "@value",
    "@type"
})
public class DcCreated {

    @JsonProperty("@value")
    private String Value;
    @JsonProperty("@type")
    private String Type;
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("@value")
    public String getValue() {
        return Value;
    }

    @JsonProperty("@value")
    public void setValue(String Value) {
        this.Value = Value;
    }

    public DcCreated withValue(String Value) {
        this.Value = Value;
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

    public DcCreated withType(String Type) {
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

    public DcCreated withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

}
