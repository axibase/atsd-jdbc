/*
* Copyright 2017 Axibase Corporation or its affiliates. All Rights Reserved.
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
package com.axibase.tsd.driver.jdbc.ext;

import lombok.Getter;

import java.util.Map;

@Getter
public class AtsdJsonException extends AtsdException {
	private final Map<String, Object> json;

	public AtsdJsonException(String message, Map<String, Object> json) {
		super(message);
		this.json = json;
	}
}
