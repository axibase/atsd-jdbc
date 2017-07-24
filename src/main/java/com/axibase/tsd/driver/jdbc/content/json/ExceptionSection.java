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
    "methodName",
    "fileName",
    "lineNumber",
    "className",
    "nativeMethod"
})
public class ExceptionSection {

    @JsonProperty("methodName")
    private String methodName;
    @JsonProperty("fileName")
    private String fileName;
    @JsonProperty("lineNumber")
    private Integer lineNumber;
    @JsonProperty("className")
    private String className;
    @JsonProperty("nativeMethod")
    private Boolean nativeMethod;
    @JsonIgnore
    private final Map<String, Object> additionalProperties = new HashMap<String, Object>();
	
    @JsonProperty("methodName")
    public String getMethodName() {
        return methodName;
    }
	
    @JsonProperty("methodName")
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public ExceptionSection withMethodName(String methodName) {
        this.methodName = methodName;
        return this;
    }
	
    @JsonProperty("fileName")
    public String getFileName() {
        return fileName;
    }
	
    @JsonProperty("fileName")
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public ExceptionSection withFileName(String fileName) {
        this.fileName = fileName;
        return this;
    }
	
    @JsonProperty("lineNumber")
    public Integer getLineNumber() {
        return lineNumber;
    }
	
    @JsonProperty("lineNumber")
    public void setLineNumber(Integer lineNumber) {
        this.lineNumber = lineNumber;
    }

    public ExceptionSection withLineNumber(Integer lineNumber) {
        this.lineNumber = lineNumber;
        return this;
    }

    @JsonProperty("className")
    public String getClassName() {
        return className;
    }

    @JsonProperty("className")
    public void setClassName(String className) {
        this.className = className;
    }

    public ExceptionSection withClassName(String className) {
        this.className = className;
        return this;
    }

    @JsonProperty("nativeMethod")
    public Boolean getNativeMethod() {
        return nativeMethod;
    }

    @JsonProperty("nativeMethod")
    public void setNativeMethod(Boolean nativeMethod) {
        this.nativeMethod = nativeMethod;
    }

    public ExceptionSection withNativeMethod(Boolean nativeMethod) {
        this.nativeMethod = nativeMethod;
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

    public ExceptionSection withAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
        return this;
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fileName == null) ? 0 : fileName.hashCode());
		result = prime * result + ((lineNumber == null) ? 0 : lineNumber.hashCode());
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
		ExceptionSection other = (ExceptionSection) obj;
		if (fileName == null) {
			if (other.fileName != null)
				return false;
		} else if (!fileName.equals(other.fileName))
			return false;
		if (lineNumber == null) {
			if (other.lineNumber != null)
				return false;
		} else if (!lineNumber.equals(other.lineNumber))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ExceptionSection [methodName=" + methodName + ", fileName=" + fileName + ", lineNumber=" + lineNumber
				+ ", className=" + className + ", nativeMethod=" + nativeMethod + "]";
	}

}
