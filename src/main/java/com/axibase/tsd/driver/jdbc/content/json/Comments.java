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
@JsonPropertyOrder({ "errors", "warnings" })
public class Comments {

	@JsonProperty("errors")
	private List<ErrorSection> errors = new ArrayList<ErrorSection>();
	@JsonProperty("warnings")
	private List<WarningSection> warnings = new ArrayList<WarningSection>();
	@JsonIgnore
	private final Map<String, Object> additionalProperties = new HashMap<String, Object>();

	@JsonProperty("errors")
	public List<ErrorSection> getErrors() {
		return errors;
	}

	@JsonProperty("errors")
	public void setErrors(List<ErrorSection> errors) {
		this.errors = errors;
	}

	public Comments withErrors(List<ErrorSection> errors) {
		this.errors = errors;
		return this;
	}

	@JsonProperty("warnings")
	public List<WarningSection> getWarnings() {
		return warnings;
	}

	@JsonProperty("warnings")
	public void setWarnings(List<WarningSection> warnings) {
		this.warnings = warnings;
	}

	public Comments withWarnings(List<WarningSection> warnings) {
		this.warnings = warnings;
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

	public Comments withAdditionalProperty(String name, Object value) {
		this.additionalProperties.put(name, value);
		return this;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((errors == null) ? 0 : errors.hashCode());
		result = prime * result + ((warnings == null) ? 0 : warnings.hashCode());
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
		Comments other = (Comments) obj;
		if (errors == null) {
			if (other.errors != null)
				return false;
		} else if (!errors.equals(other.errors))
			return false;
		if (warnings == null) {
			if (other.warnings != null)
				return false;
		} else if (!warnings.equals(other.warnings))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Comments [errors=" + errors + ", warnings=" + warnings + "]";
	}

}