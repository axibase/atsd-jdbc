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
import com.axibase.tsd.driver.jdbc.enums.SqlStatementType;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.ext.AtsdException;
import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import com.axibase.tsd.driver.jdbc.intf.IContentProtocol;
import com.axibase.tsd.driver.jdbc.intf.IDataProvider;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.protocol.ProtocolFactory;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import com.axibase.tsd.driver.jdbc.strategies.StrategyFactory;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DataProvider implements IDataProvider {
	private static final LoggingFacade logger = LoggingFacade.getLogger(DataProvider.class);
	@Getter
	private final ContentDescription contentDescription;
	private final IContentProtocol contentProtocol;
	private final StatementContext context;
	private final SqlStatementType statementType;
	@Getter
	private IStoreStrategy strategy;
	private final AtomicBoolean isHoldingConnection = new AtomicBoolean();

	public DataProvider(AtsdConnectionInfo connectionInfo, String query, StatementContext context,
						SqlStatementType statementType) {
		this.statementType = statementType;
		final String endpoint;
		if (context.isEncodeTags()) {
			endpoint = Location.SQL_ENDPOINT.getUrl(connectionInfo) + "?encodeTags=true";
		} else {
			endpoint = Location.SQL_ENDPOINT.getUrl(connectionInfo);
		}
		this.contentDescription = new ContentDescription(endpoint, connectionInfo, query, context);
		logger.trace("Endpoint: {}", contentDescription.getEndpoint());
		this.contentProtocol = ProtocolFactory.create(SdkProtocolImpl.class, contentDescription);
		this.context = context;
	}

	@Override
	public long executeQuery(long maxLimit, int timeoutMillis) throws AtsdException, GeneralSecurityException, IOException {
		contentDescription.setMaxRowsCount(maxLimit);
		this.isHoldingConnection.set(true);
		final InputStream is = contentProtocol.readContent(timeoutMillis);
		this.isHoldingConnection.set(false);
		if (statementType == SqlStatementType.SELECT) {
			this.strategy = defineStrategy();
			if (this.strategy != null) {
				this.strategy.store(is);
			}
			return -1L;
		} else {
			return JsonMappingUtil.mapToDataModificationResult(is).countUpdated();
		}
	}

	@Override
	@Deprecated
	public long sendData(int timeoutMillis) throws AtsdException, GeneralSecurityException, IOException {
		this.isHoldingConnection.set(false);
		return contentProtocol.writeContent(timeoutMillis);
	}

	@Override
	public void cancelQuery() {
		if (this.contentProtocol == null) {
			throw new IllegalStateException("Cannot cancel query: contentProtocol is not created yet");
		}
		logger.trace("[cancelQuery] sending cancel queryId={}", context.getQueryId());
		try {
			this.contentProtocol.cancelQuery();
		} catch (Exception e) {
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
		this.isHoldingConnection.set(false);
	}

	@Override
	public void close() throws Exception {
		if (this.isHoldingConnection.get()) {
			cancelQuery();
		}
		if (this.strategy != null) {
			this.strategy.close();
		}

		logger.trace("[DataProvider#close]");
	}

	private IStoreStrategy defineStrategy() {
		final AtsdConnectionInfo info = this.contentDescription.getInfo();
		return StrategyFactory.create(StrategyFactory.findClassByName(info.strategy()), this.context, info.missingMetric());
	}

}
