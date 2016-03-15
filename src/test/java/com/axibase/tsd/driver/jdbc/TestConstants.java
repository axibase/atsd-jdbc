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
package com.axibase.tsd.driver.jdbc;

public interface TestConstants {
	static final String SELECT_TVE_CLAUSE = "SELECT time, value, entity FROM ";
	static final String SELECT_DVE_CLAUSE = "SELECT datetime, value, entity FROM ";
	static final String SELECT_ALL_CLAUSE = "SELECT * FROM ";
	static final String SELECT_LIMIT_1000 = " LIMIT 1000";
	static final String SELECT_LIMIT_100000 = " LIMIT 100000";
	static final String WHERE_CLAUSE = " WHERE entity = ?";
	static final String JDBC_ATDS_URL_PREFIX = "jdbc:axibase:atsd:";
	static final String PARAM_SEPARATOR = ";";
	static final String STRATEGY_FILE_PARAMETER = "strategy=file";
	static final String STRATEGY_STREAM_PARAMETER = "strategy=stream";
	static final String TRUST_PARAMETER = "trustServerCertificate=true";
	static final String UNTRUST_PARAMETER = "trustServerCertificate=false";
	static final String TRUST_PARAMETER_IN_QUERY = PARAM_SEPARATOR + TRUST_PARAMETER + PARAM_SEPARATOR;
	static final String UNTRUST_PARAMETER_IN_QUERY = PARAM_SEPARATOR + UNTRUST_PARAMETER + PARAM_SEPARATOR;
}