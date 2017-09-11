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

import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.enums.MetadataFormat;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.DbMetadataUtils;
import lombok.Data;
import org.apache.calcite.avatica.org.apache.http.HttpHeaders;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.axibase.tsd.driver.jdbc.DriverConstants.*;

@Data
public class ContentDescription {
	private static final LoggingFacade logger = LoggingFacade.getLogger(ContentDescription.class);

	private String endpoint;
	private String query;
    private String postContent = "";
    private final Map<String, String> requestHeaders = new HashMap<>();
    private String[] headers;
	private String jsonScheme;
	private MetadataFormat metadataFormat;
	private long maxRowsCount;
	private final String queryId;
	private final AtsdConnectionInfo info;

	public ContentDescription(String endpoint, AtsdConnectionInfo atsdConnectionInfo) {
		this(endpoint, atsdConnectionInfo, "", "");
	}

	public ContentDescription(String endpoint, AtsdConnectionInfo atsdConnectionInfo, String query, StatementContext context) {
		this(endpoint, atsdConnectionInfo, query, context.getQueryId());
	}

	private ContentDescription(String endpoint, AtsdConnectionInfo atsdConnectionInfo, String query, String queryId) {
		this.endpoint = endpoint;
		this.query = query;
		this.metadataFormat = MetadataFormat.EMBED;
		this.info = atsdConnectionInfo;
		this.queryId = queryId;
	}

	public String getEncodedQuery() {
		return DbMetadataUtils.urlEncode(query);
	}

	public void initDataFetchingContent() {
		if (StringUtils.isEmpty(query)) {
            return;
        } else if (endpoint.endsWith(Location.SQL_META_ENDPOINT.getEndpoint())) {
			final String encodedQuery = getEncodedQuery();
			this.postContent = Q_PARAM_NAME + '=' + encodedQuery;
        } else {
			final String encodedQuery = getEncodedQuery();
			this.postContent = QUERY_ID_PARAM_NAME + '=' + queryId + '&' +
                    Q_PARAM_NAME + '=' + encodedQuery + '&' +
                    FORMAT_PARAM_NAME + '=' + FORMAT_PARAM_VALUE + '&' +
                    METADATA_FORMAT_PARAM_NAME + '=' + metadataFormat + '&' +
                    LIMIT_PARAM_NAME + '=' + maxRowsCount;
        }
    }

	public void addRequestHeadersForDataFetching() {
		addRequestHeader(HttpHeaders.ACCEPT, CSV_AND_JSON_MIME_TYPE);
		addRequestHeader(HttpHeaders.CONTENT_TYPE, FORM_URLENCODED_TYPE);
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
