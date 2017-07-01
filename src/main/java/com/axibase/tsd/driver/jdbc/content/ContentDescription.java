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
package com.axibase.tsd.driver.jdbc.content;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.axibase.tsd.driver.jdbc.enums.MetadataFormat;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import org.apache.calcite.avatica.org.apache.http.HttpHeaders;
import org.apache.commons.lang3.StringUtils;

import static com.axibase.tsd.driver.jdbc.DriverConstants.*;

public class ContentDescription {
	private static final LoggingFacade logger = LoggingFacade.getLogger(ContentDescription.class);

	private String host;
	private String query;
	private String login;
	private String password;
	private String postContent = "";
	private final Map<String, String> requestHeaders = new HashMap<>();
	private long contentLength;
	private String[] headers;
	private String jsonScheme;
	private final String metadataFormat;
	private long maxRowsCount;
	private final String queryId;
	private final boolean supportsCancel;
	private final AtsdConnectionInfo atsdConnectionInfo;

	public ContentDescription(String host, AtsdConnectionInfo atsdConnectionInfo) {
		this(host, atsdConnectionInfo, "", 0, "");
	}

	ContentDescription(String host, AtsdConnectionInfo atsdConnectionInfo, String query, StatementContext context) {
		this(host , atsdConnectionInfo, query, context.getVersion(), context.getQueryId());
	}

	public ContentDescription(AtsdConnectionInfo atsdConnectionInfo, String query, StatementContext context) {
		this(atsdConnectionInfo.host() , atsdConnectionInfo, query, context.getVersion(), context.getQueryId());
	}

	private ContentDescription(String host, AtsdConnectionInfo atsdConnectionInfo, String query, int atsdVersion, String queryId) {
		this.host = host;
		this.query = query;
		this.login = atsdConnectionInfo.user();
		this.password = atsdConnectionInfo.password();
		this.metadataFormat = atsdVersion < ATSD_VERSION_SUPPORTING_BODY_METADATA ?
				MetadataFormat.HEADER.name() : MetadataFormat.EMBED.name();
		this.atsdConnectionInfo = atsdConnectionInfo;
		this.queryId = queryId;
		this.supportsCancel = atsdVersion >= ATSD_VERSION_SUPPORTS_CANCEL_QUERIES;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getEncodedQuery() {
		try {
			return URLEncoder.encode(query, DEFAULT_CHARSET.name());
		} catch (UnsupportedEncodingException e) {
			logger.error(e.getMessage());
			return query;
		}
	}

	public String getLogin() {
		return login;
	}

	public void setLogin(String login) {
		this.login = login;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getJsonScheme() {
		return jsonScheme != null ? jsonScheme : "";
	}

	public void setJsonScheme(String jsonScheme) {
		this.jsonScheme = jsonScheme;
	}

	public String[] getHeaders() {
		return headers;
	}

	public void setHeaders(String[] headers) {
		this.headers = headers;
	}

	public long getContentLength() {
		return contentLength;
	}

	public void setContentLength(long contentLength) {
		this.contentLength = contentLength;
	}

	public void setMaxRowsCount(long maxRowsCount) {
		this.maxRowsCount = maxRowsCount;
	}

	public String getPostContent() {
		return postContent;
	}

	public void setPostContent(String postContent) {
		this.postContent = postContent;
	}

	public void initSelectContent() {
		if (StringUtils.isEmpty(query)) {
			return;
		}
		StringBuilder buffer = new StringBuilder();
		if (supportsCancel) {
			buffer.append(QUERY_ID_PARAM_NAME).append('=').append(queryId).append('&');
		}
		this.postContent = buffer
				.append(Q_PARAM_NAME).append('=').append(getEncodedQuery()).append('&')
				.append(FORMAT_PARAM_NAME).append('=').append(FORMAT_PARAM_VALUE).append('&')
				.append(METADATA_FORMAT_PARAM_NAME).append('=').append(metadataFormat).append('&')
				.append(LIMIT_PARAM_NAME).append('=').append(maxRowsCount)
				.toString();
	}

	public void addRequestHeadersForDataFetching() {
		addRequestHeader(HttpHeaders.ACCEPT, CSV_AND_JSON_MIME_TYPE);
		addRequestHeader(HttpHeaders.CONTENT_TYPE, FORM_URLENCODED_TYPE);
	}

	public String getCancelQueryUrl() {
		return host + CANCEL_METHOD + '?' + QUERY_ID_PARAM_NAME + '=' + queryId;
	}

	public String getMetadataFormat() {
		return metadataFormat;
	}

	public Map<String, String> getQueryParamsAsMap() {
		if (StringUtils.isEmpty(query)) {
			return Collections.emptyMap();
		}
		Map<String, String> map = new HashMap<>();
		map.put(Q_PARAM_NAME, query);
		map.put(FORMAT_PARAM_NAME, FORMAT_PARAM_VALUE);
		map.put(METADATA_FORMAT_PARAM_NAME, metadataFormat);
		map.put(LIMIT_PARAM_NAME, Long.toString(maxRowsCount));
		return map;
	}

	public boolean isSsl() {
		return StringUtils.startsWithIgnoreCase(host, "https://");
	}

	public boolean isTrusted() {
		return atsdConnectionInfo.trustCertificate();
	}

	public int getConnectTimeout() {
		return atsdConnectionInfo.connectTimeout();
	}

	public int getReadTimeout() {
		return atsdConnectionInfo.readTimeout();
	}

	public String getStrategyName() {
		return atsdConnectionInfo.strategy();
	}

	public String getQueryId() {
		return queryId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((login == null) ? 0 : login.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
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
		ContentDescription other = (ContentDescription) obj;
		if (login == null) {
			if (other.login != null)
				return false;
		} else if (!login.equals(other.login))
			return false;
		if (password == null) {
			if (other.password != null)
				return false;
		} else if (!password.equals(other.password))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "ContentDescription [host=" + this.getHost() + ", postContent=" + postContent + ", login=" + login
				+ ", headers=" + Arrays.toString(headers) + ", jsonScheme=" + jsonScheme + ", contentLength="
				+ contentLength + "]";
	}

	public void addRequestHeader(String name, String value) {
		if (StringUtils.isEmpty(name) || StringUtils.isEmpty(value)) {
			return;
		}
		requestHeaders.put(name, value);
	}

	public Map<String, String> getRequestHeaders() {
		return requestHeaders;
	}

}
