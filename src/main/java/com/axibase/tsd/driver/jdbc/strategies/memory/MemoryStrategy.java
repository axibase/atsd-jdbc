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
package com.axibase.tsd.driver.jdbc.strategies.memory;

import com.axibase.tsd.driver.jdbc.content.StatementContext;
import com.axibase.tsd.driver.jdbc.enums.OnMissingMetricAction;
import com.axibase.tsd.driver.jdbc.enums.Strategy;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.strategies.AbstractStrategy;

import java.io.*;

public class MemoryStrategy extends AbstractStrategy {
	private static final LoggingFacade logger = LoggingFacade.getLogger(MemoryStrategy.class);
	private static final int BUFFER_SIZE = 16 * 1024;

	public MemoryStrategy(StatementContext context, OnMissingMetricAction action) {
		super(context, Strategy.MEMORY, action);
	}

	@Override
	public void store(InputStream inputStream) throws IOException {
		super.store(new ByteArrayInputStream(inputStreamToByteArray(inputStream)));
		inputStream.close();
	}

	private static byte[] inputStreamToByteArray(InputStream inputStream) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		final int length = BUFFER_SIZE;
		byte[] buffer = new byte[length];
		int read;
		while ((read = inputStream.read(buffer, 0, length)) != -1) {
			byteArrayOutputStream.write(buffer, 0, read);
		}
		byteArrayOutputStream.flush();
		return byteArrayOutputStream.toByteArray();
	}
}
