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

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import com.axibase.tsd.driver.jdbc.content.UnivocityParserRowContext;
import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import com.axibase.tsd.driver.jdbc.intf.ParserRowContext;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.avatica.ColumnMetaData;

import static com.axibase.tsd.driver.jdbc.DriverConstants.DEFAULT_CHARSET;

public class RowIterator implements Iterator<Object[]>, AutoCloseable {
	private static final char COMMENT_SIGN = '#';

	private CsvParser decoratedParser;
	private final Reader decoratedReader;
	private ParserRowContext rowContext;
	private String commentSection;
	private Object[] nextRow;
	private String[] header;
	private AtsdType[] columnTypes;
	private boolean[] nullable;
	private AtsdRowProcessor processor;

	private RowIterator(Reader reader, List<ColumnMetaData> columnMetadata, CsvParserSettings settings) {
		this.decoratedReader = reader;
		try {
			final int firstSymbol = reader.read();
			if (firstSymbol == COMMENT_SIGN) {
				fillCommentSectionWithReaderContent(reader);
			} else if (firstSymbol != -1) {
				fillFromMetadata(columnMetadata);
				this.nextRow = this.header;
				this.processor = new AtsdRowProcessor();
				settings.setProcessor(this.processor);
				this.decoratedParser = new CsvParser(settings);
				this.decoratedParser.beginParsing(reader);
				this.rowContext = new UnivocityParserRowContext(this.decoratedParser.getContext(), this.header.length);
				next();
			}
		} catch (IOException e) {
			throw new AtsdRuntimeException(e.getMessage(), e);
		}
	}

	public static RowIterator newDefaultIterator(InputStream inputStream, List<ColumnMetaData> metadata, int version) {
		Reader reader = new InputStreamReader(inputStream, DEFAULT_CHARSET);
		return newDefaultIterator(reader, metadata, version);
	}

	public static RowIterator newDefaultIterator(Reader reader, List<ColumnMetaData> metadata, int version) {
		return new RowIterator(reader, metadata, prepareParserSettings(version));
	}

	protected final class AtsdRowProcessor implements RowProcessor {
		private Object[] parsed;

		Object[] getParsed() {
			return parsed;
		}

		@Override
		public void processStarted(ParsingContext context) {
			// do nothing
		}

		@Override
		public void rowProcessed(String[] row, ParsingContext context) {
			parsed = parseValues(row);
		}

		@Override
		public void processEnded(ParsingContext context) {
			// do nothing
		}
	}

	private static CsvParserSettings prepareParserSettings(int version) {
		final CsvParserSettings settings = new CsvParserSettings();
		settings.setInputBufferSize(16 * 1024);
		settings.setReadInputOnSeparateThread(false);
		settings.setCommentCollectionEnabled(false);
		settings.setEmptyValue("");
		settings.setNullValue(version >= DriverConstants.ATSD_VERSION_DIFFERS_NULL_AND_EMPTY ? null : "");
		settings.setNumberOfRowsToSkip(1);
		return settings;
	}

	private void fillFromMetadata(List<ColumnMetaData> columnMetadata) {
		final int length = columnMetadata.size();
		this.header = new String[length];
		this.columnTypes = new AtsdType[length];
		this.nullable = new boolean[length];
		int i = 0;
		for (ColumnMetaData metaData : columnMetadata) {
			this.header[i] = metaData.columnName;
			this.columnTypes[i] = EnumUtil.getAtsdTypeBySqlType(metaData.type.id);
			this.nullable[i] = metaData.nullable == 1;
			++i;
		}
	}

	private void fillCommentSectionWithReaderContent(Reader reader) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(reader);
		StringBuilder buffer = new StringBuilder();
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			buffer.append(line.charAt(0) == COMMENT_SIGN ? line.substring(1) : line);
		}
		this.commentSection = buffer.toString();
	}

	private void fillCommentSectionWithParsedComments() {
		final String comments = decoratedParser.getContext().currentParsedContent();
		if (comments == null) {
			return;
		}
		int startIndex = comments.indexOf(COMMENT_SIGN) + 1;
		if (startIndex > 0) {
			final int length = comments.length();
			StringBuilder buffer = new StringBuilder(length - startIndex);
			while (startIndex != -1) {
				final int endIndex = comments.indexOf("\n#", startIndex);
				final String substring;
				if (endIndex == -1) {
					substring = comments.substring(startIndex);
					startIndex = -1;
				} else {
					substring = comments.substring(startIndex, endIndex);
					startIndex = endIndex + 2;
				}
				buffer.append(substring);
			}
			commentSection = buffer.toString();
		}
	}

	@Override
	public boolean hasNext() {
		return nextRow != null;
	}

	@Override
	public Object[] next() {
		if (nextRow == null) {
			throw new NoSuchElementException();
		}
		final Object[] result = nextRow;
		if (decoratedParser.parseNext() == null) {
			fillCommentSectionWithParsedComments();
			nextRow = null;
		} else {
			nextRow = processor.getParsed();
		}
		return result;
	}

	private Object[] parseValues(String[] values) {
		final int length = columnTypes.length;
		if (columnTypes.length != values.length) {
			throw new AtsdRuntimeException("Parsed number of columns doesn't match to header on row=" + this.rowContext.getLine());
		}
		Object[] parsed = new Object[length];
		for (int i = 0; i != length; ++i) {
			parsed[i] = columnTypes[i].readValue(values, i, nullable[i], this.rowContext);
		}
		return parsed;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("This iterator is read-only");
	}

	@Override
	public void close() throws IOException {
		if (this.decoratedParser != null) {
			this.decoratedParser.stopParsing();
		}
		if (this.decoratedReader != null) {
			this.decoratedReader.close();
		}
	}

	public CharSequence getCommentSection() {
		return commentSection;
	}

	public String[] getHeader() {
		return header;
	}
}
