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
    "revisionNumber",
    "buildNumber",
    "buildId"
})
public class BuildInfo {

    @JsonProperty("revisionNumber")
    private String revisionNumber;
    @JsonProperty("buildNumber")
    private String buildNumber;
    @JsonProperty("buildId")
    private String buildId;
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public BuildInfo() {
    }

    @JsonProperty("revisionNumber")
    public String getRevisionNumber() {
        return revisionNumber;
    }

    @JsonProperty("revisionNumber")
    public void setRevisionNumber(String revisionNumber) {
        this.revisionNumber = revisionNumber;
    }

    @JsonProperty("buildNumber")
    public String getBuildNumber() {
        return buildNumber;
    }

    @JsonProperty("buildNumber")
    public void setBuildNumber(String buildNumber) {
        this.buildNumber = buildNumber;
    }

    @JsonProperty("buildId")
    public String getBuildId() {
        return buildId;
    }

    @JsonProperty("buildId")
    public void setBuildId(String buildId) {
        this.buildId = buildId;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((buildId == null) ? 0 : buildId.hashCode());
		result = prime * result + ((revisionNumber == null) ? 0 : revisionNumber.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BuildInfo other = (BuildInfo) obj;
		if (buildId == null) {
			if (other.buildId != null)
				return false;
		} else if (!buildId.equals(other.buildId))
			return false;
		if (revisionNumber == null) {
			if (other.revisionNumber != null)
				return false;
		} else if (!revisionNumber.equals(other.revisionNumber))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "BuildInfo [revisionNumber=" + revisionNumber + ", buildNumber=" + buildNumber + ", buildId=" + buildId
				+ "]";
	}

}
