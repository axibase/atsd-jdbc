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
package com.axibase.tsd.driver.jdbc.strategies;

import com.axibase.tsd.driver.jdbc.content.StatementContext;
import com.axibase.tsd.driver.jdbc.enums.OnMissingMetricAction;
import com.axibase.tsd.driver.jdbc.enums.Strategy;
import com.axibase.tsd.driver.jdbc.intf.IConsumer;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import org.apache.calcite.avatica.ColumnMetaData;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractStrategy implements IStoreStrategy {
	private static final LoggingFacade logger = LoggingFacade.getLogger(AbstractStrategy.class);

	protected final IConsumer consumer;
	protected final StrategyStatus status;
	protected long position;
	protected InputStream inputStream;

	protected AbstractStrategy(StatementContext context, Strategy strategy, OnMissingMetricAction action) {
		this.status = new StrategyStatus();
		this.status.setInProgress(true);
		this.position = 0;
		this.consumer = new Consumer(context, status, strategy.getSource(), action);
	}

	@Override
	public StatementContext getContext() {
		return consumer.getContext();
	}

	@Override
	public List<List<Object>> fetch(long from, int limit) throws IOException {
		final List<List<Object>> list = new ArrayList<>();
		int size = 0;
		for (Object[] next : consumer) {
			if (position < from) {
				if (logger.isTraceEnabled()) {
					logger.trace("[fetch] position less from: " + position + "->" + from);
				}
				position++;
			} else {
				list.add(Arrays.asList(next));
				++size;
				if (size == limit) {
					break;
				}
			}
		}
		consumer.fillComments();
		if (logger.isTraceEnabled()) {
			logger.trace("[fetch] sublist size: " + size);
		}
		position = from + size;
		if (logger.isTraceEnabled()) {
			logger.trace("[fetch] updated position: " + position);
		}
		status.increaseProcessed(size);
		return Collections.unmodifiableList(list);
	}

	@Override
	public String[] openToRead(List<ColumnMetaData> metadataList) throws IOException {
		if (logger.isTraceEnabled()) {
			logger.trace("[openToRead] {}", status.getSyncLatch().getCount());
		}
		try {
			status.getSyncLatch().await();
		} catch (InterruptedException e) {
			logger.debug("[openToRead] {}", e.getMessage());
			Thread.currentThread().interrupt();
		}
		final String[] header = consumer.open(inputStream, metadataList);
		consumer.fillComments();
		return header;
	}

	@Override
	public void store(InputStream inputStream) throws IOException {
		if (logger.isTraceEnabled()) {
			logger.trace("[store] " + inputStream.hashCode() + " -> " + inputStream.available());
		}
		this.inputStream = inputStream;

		final CountDownLatch syncLatch = status.getSyncLatch();
		if (syncLatch.getCount() != 0) {
			syncLatch.countDown();
		}
	}

	@Override
	public void close() throws Exception {
		status.setInProgress(false);
		if (consumer != null) {
			consumer.close();
		}
		if (inputStream != null) {
			inputStream.close();
		}
		if (logger.isDebugEnabled()) {
			logger.debug("[close] processed " + status.getProcessed());
		}
	}
}
