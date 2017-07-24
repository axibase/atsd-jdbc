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
package com.axibase.tsd.driver.jdbc.strategies.storage;

import com.axibase.tsd.driver.jdbc.content.StatementContext;
import com.axibase.tsd.driver.jdbc.enums.OnMissingMetricAction;
import com.axibase.tsd.driver.jdbc.enums.Strategy;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.strategies.AbstractStrategy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.concurrent.CountDownLatch;

public class FileStoreStrategy extends AbstractStrategy {
	private static final LoggingFacade logger = LoggingFacade.getLogger(FileStoreStrategy.class);
	private static final String TMP_FILE_PREFIX = "atsd-driver";

	private Path tmp;

	public FileStoreStrategy(StatementContext context, OnMissingMetricAction action) {
		super(context, Strategy.FILE, action);
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (logger.isTraceEnabled()) {
			logger.trace("[close] " + status.getSyncLatch().getCount());
		}
		if (tmp != null) {
			final File file = tmp.toFile();
			if (file.exists()) {
				boolean deleted = file.delete();
				if (logger.isDebugEnabled()) {
					logger.debug("[close] File {} is deleted {} ", file.toString(), deleted);
				}
			}
		}
	}

	@Override
	public void store(InputStream inputStream) throws IOException {
		tmp = Files.createTempFile(TMP_FILE_PREFIX, null);
		if (logger.isDebugEnabled()) {
			logger.debug("[store] " + tmp.toRealPath());
		}
		Files.copy(inputStream, tmp, StandardCopyOption.REPLACE_EXISTING);
		this.inputStream = Files.newInputStream(tmp, StandardOpenOption.READ);
		final CountDownLatch syncLatch = status.getSyncLatch();
		if (syncLatch.getCount() != 0) {
			syncLatch.countDown();
		}
	}
}
