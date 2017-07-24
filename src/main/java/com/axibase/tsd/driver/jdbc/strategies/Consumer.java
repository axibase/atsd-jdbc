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
import com.axibase.tsd.driver.jdbc.content.json.*;
import com.axibase.tsd.driver.jdbc.enums.OnMissingMetricAction;
import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import com.axibase.tsd.driver.jdbc.intf.IConsumer;
import com.axibase.tsd.driver.jdbc.logging.LoggingFacade;
import com.axibase.tsd.driver.jdbc.util.JsonMappingUtil;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.axibase.tsd.driver.jdbc.util.ExceptionsUtil.isMetricNotFoundException;


public class Consumer implements IConsumer {
	private static final LoggingFacade logger = LoggingFacade.getLogger(Consumer.class);

	protected final StrategyStatus status;
	protected final StatementContext context;
	protected final OnMissingMetricAction onMissingMetricAction;
	protected final String source;
	protected RowIterator iterator;

	public Consumer(final StatementContext context, final StrategyStatus status, String source, OnMissingMetricAction action) {
		this.context = context;
		this.status = status;
		this.source = source;
		this.onMissingMetricAction = action;
	}

	@Override
	public StatementContext getContext() {
		return context;
	}

	@Override
	public void close() throws IOException {
		if (logger.isTraceEnabled()) {
			logger.trace("[close]");
		}
		if (iterator != null) {
			iterator.close();
		}
	}

	@Override
	public String[] open(InputStream inputStream, List<ColumnMetaData> columnMetadataList) throws IOException {
		iterator = RowIterator.newDefaultIterator(inputStream, columnMetadataList, context.getVersion());
		return iterator.getHeader();
	}

	@Override
	public Iterator<Object[]> iterator(){
		if (iterator == null) {
			assert source != null;
			throw new IllegalStateException(source + " has not opened yet");
		}
		return iterator;
	}

	@Override
	public void fillComments() {
		final CharSequence comments = iterator.getCommentSection();
		if (StringUtils.isBlank(comments)) {
			return;
		}
		final String json = comments.toString();
		if (logger.isTraceEnabled()) {
			logger.trace(json);
		}
		try {
			final Comments commentsObject = JsonMappingUtil.mapToComments(json);
			fillWarnings(commentsObject.getWarnings());
			fillErrors(commentsObject.getErrors());
		} catch (IOException e){
			if (logger.isDebugEnabled()) {
				logger.debug("Wrong error format: {}", e.getMessage());
			}
		}
	}

	private void addWarning(AtsdExceptionRepresentation section) {
		SQLWarning sqlw = new SQLWarning(section.getMessage(), section.getState());
		List<StackTraceElement> list = getStackTrace(section);
		sqlw.setStackTrace(list.toArray(new StackTraceElement[list.size()]));
		context.addWarning(sqlw);
	}

	private SQLException addError(AtsdExceptionRepresentation section) {
		SQLException sqlException = new SQLException(section.getMessage(), section.getState());
		List<StackTraceElement> list = getStackTrace(section);
		sqlException.setStackTrace(list.toArray(new StackTraceElement[list.size()]));
		context.addException(sqlException);
		return sqlException;
	}

	private void fillWarnings(List<WarningSection> warningSections) {
		if (warningSections != null) {
			for (AtsdExceptionRepresentation section : warningSections) {
				addWarning(section);
			}
		}
	}

	private void fillErrors(List<ErrorSection> errorSections) {
		SQLException sqlException = null;
		if (errorSections != null) {
			for (ErrorSection section : errorSections) {
				if (isMetricNotFoundException(section.getMessage())) {
					if (onMissingMetricAction == OnMissingMetricAction.ERROR) {
						sqlException = addError(section);
					} else if (onMissingMetricAction == OnMissingMetricAction.WARNING) {
						addWarning(section);
					}
				} else {
					sqlException = addError(section);
				}
			}
		}
		if (sqlException != null) {
			throw new AtsdRuntimeException(sqlException.getMessage(), sqlException);
		}
	}

	private static List<StackTraceElement> getStackTrace(AtsdExceptionRepresentation section) {
		final List<ExceptionSection> exceptions = section.getException();
		final List<StackTraceElement> list = new ArrayList<>(exceptions.size());
		for (ExceptionSection exc : exceptions) {
			list.add(new StackTraceElement(exc.getClassName(), exc.getMethodName(), exc.getFileName(),
					exc.getLineNumber()));
		}
		return list;
	}


}
