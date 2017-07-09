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

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.avatica.Meta;

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

import static com.axibase.tsd.driver.jdbc.DriverConstants.SQL_ENDPOINT;
import static com.axibase.tsd.driver.jdbc.DriverConstants.COMMAND_ENDPOINT;

public class DataProvider implements IDataProvider {
	private static final LoggingFacade logger = LoggingFacade.getLogger(DataProvider.class);
	private final ContentDescription contentDescription;
	private final IContentProtocol contentProtocol;
	private final StatementContext context;
	private IStoreStrategy strategy;
	private AtomicBoolean isHoldingConnection = new AtomicBoolean();

	public DataProvider(AtsdConnectionInfo connectionInfo, String query, StatementContext context, Meta.StatementType statementType) {
		switch (statementType) {
			case SELECT: {
				final String commandUrl = connectionInfo.toEndpoint(SQL_ENDPOINT);
				this.contentDescription = new ContentDescription(commandUrl, connectionInfo, query, context);
				this.contentDescription.initSelectContent();
				break;
			}
			case INSERT:
			case UPDATE: {
				final String commandUrl = connectionInfo.toEndpoint(COMMAND_ENDPOINT);
				this.contentDescription = new ContentDescription(commandUrl, connectionInfo, query, context);
				break;
			}
			default: throw new IllegalArgumentException("Unsupported statement type: " + statementType);
		}
		logger.trace("Endpoint: {}", contentDescription.getEndpoint());
		this.contentProtocol = ProtocolFactory.create(SdkProtocolImpl.class, contentDescription);
		this.context = context;
	}

	@Override
	public ContentDescription getContentDescription() {
		return this.contentDescription;
	}

	@Override
	public IStoreStrategy getStrategy() {
		return this.strategy;
	}

	@Override
	public void fetchData(long maxLimit, int timeout) throws AtsdException, GeneralSecurityException, IOException {
		contentDescription.setMaxRowsCount(maxLimit);
		this.isHoldingConnection.set(true);
		final InputStream is = contentProtocol.readContent(timeout);
		this.isHoldingConnection.set(false);
		this.strategy = defineStrategy();
		if (this.strategy != null) {
			this.strategy.store(is);
		}
	}

	@Override
	public long sendData(int timeout) throws AtsdException, GeneralSecurityException, IOException {
		this.isHoldingConnection.set(false);
		final long writeCount = contentProtocol.writeContent(timeout);
		return writeCount;
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

	private void closeWithRuntimeException() {
		try {
			this.contentProtocol.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
		return StrategyFactory.create(StrategyFactory.findClassByName(this.contentDescription.getInfo().strategy()), this.context);
	}

}
