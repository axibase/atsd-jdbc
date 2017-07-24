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
    "buildInfo",
    "license"
})
public class Version {

    @JsonProperty("buildInfo")
    private BuildInfo buildInfo;
    @JsonProperty("license")
    private License license;
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<String, Object>();

    public Version() {}

    public Version(BuildInfo buildInfo, License license) {
        this.buildInfo = buildInfo;
        this.license = license;
    }

    @JsonProperty("buildInfo")
    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    @JsonProperty("buildInfo")
    public void setBuildInfo(BuildInfo buildInfo) {
        this.buildInfo = buildInfo;
    }

    @JsonProperty("license")
    public License getLicense() {
        return license;
    }

    @JsonProperty("license")
    public void setLicense(License license) {
        this.license = license;
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
		result = prime * result + ((buildInfo == null) ? 0 : buildInfo.hashCode());
		result = prime * result + ((license == null) ? 0 : license.hashCode());
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
		Version other = (Version) obj;
		if (buildInfo == null) {
			if (other.buildInfo != null)
				return false;
		} else if (!buildInfo.equals(other.buildInfo))
			return false;
		if (license == null) {
			if (other.license != null)
				return false;
		} else if (!license.equals(other.license))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Version [buildInfo=" + buildInfo + ", license=" + license + "]";
	}

}
