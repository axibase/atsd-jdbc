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

import java.io.IOException;
import java.io.InputStream;

import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import org.apache.calcite.avatica.com.fasterxml.jackson.annotation.*;


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
		"error"
})
public class GeneralError {
	@JsonProperty("error")
	private String error;

	@JsonProperty("error")
	public String getError() {
		return error;
	}

	@JsonProperty("error")
	public void setError(String error) {
		this.error = error;
	}

	public static String errorFromInputStream(InputStream inputStream) throws IOException {
		final GeneralError errorObject = JsonMappingUtil.mapToGeneralError(inputStream);
		if (errorObject == null) {
			return null;
		}
		return errorObject.getError();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		if (error != null) {
			result = prime + error.hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof GeneralError)) {
			return false;
		}
		final GeneralError other = (GeneralError) o;
		if (this.error == null) {
			return other.error == null;
		}
		return this.error.equals(other.error);
	}

	@Override
	public String toString() {
		return "GeneralError [error=" + error + "]";
	}
}
