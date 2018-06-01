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

import com.axibase.tsd.driver.jdbc.content.UnivocityParserRowContext;
import com.axibase.tsd.driver.jdbc.enums.AtsdType;
import com.axibase.tsd.driver.jdbc.ext.AtsdRuntimeException;
import com.axibase.tsd.driver.jdbc.intf.ParserRowContext;
import com.axibase.tsd.driver.jdbc.util.EnumUtil;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.Getter;
import org.apache.calcite.avatica.ColumnMetaData;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.axibase.tsd.driver.jdbc.DriverConstants.DEFAULT_CHARSET;

public class RowIterator implements Iterator<Object[]>, AutoCloseable {
	private static final char COMMENT_SYMBOL = '#';
	private static final Object[] EMPTY_LINE = {null};

	private final CsvParser csvParser;
	private final Reader decoratedReader;
	private final ParserRowContext rowContext;
	private final AtsdType[] columnTypes;
	private final boolean[] nullable;
	private final AtsdRowProcessor processor;

	private Object[] nextRow;
	@Getter
	private final String[] header;
	@Getter
	private String commentSection;

	private RowIterator(Reader reader, String commentSection) {
		this.decoratedReader = reader;
		this.commentSection = commentSection;
		this.header = null;
		this.columnTypes = null;
		this.nullable = null;
		this.processor = null;
		this.csvParser = null;
		this.rowContext = null;
	}

	private RowIterator(Reader reader, List<ColumnMetaData> columnMetadata) {
		this.decoratedReader = reader;
		final int length = columnMetadata.size();
		this.header = new String[length];
		this.columnTypes = new AtsdType[length];
		this.nullable = new boolean[length];
		fillArraysFromMetadata(columnMetadata);

		this.nextRow = this.header;
		this.processor = new AtsdRowProcessor();
		this.csvParser = new CsvParser(prepareParserSettings(this.processor));
		this.csvParser.beginParsing(reader);
		this.rowContext = new UnivocityParserRowContext(csvParser.getContext(), length);
	}

	private void fillArraysFromMetadata(List<ColumnMetaData> columnMetaData) {
		int i = 0;
		for (ColumnMetaData metaData : columnMetaData) {
			this.header[i] = metaData.columnName;
			this.columnTypes[i] = EnumUtil.getAtsdTypeBySqlType(metaData.type.id, AtsdType.DEFAULT_TYPE);
			this.nullable[i] = metaData.nullable == 1;
			++i;
		}
	}

	public static RowIterator newDefaultIterator(InputStream inputStream, List<ColumnMetaData> metadata) throws IOException {
		Reader reader = new InputStreamReader(inputStream, DEFAULT_CHARSET);
		return newDefaultIterator(reader, metadata);
	}

	public static RowIterator newDefaultIterator(Reader reader, List<ColumnMetaData> metadata) throws IOException {
		final RowIterator rowIterator;
		final int firstSymbol = reader.read();
		if (firstSymbol == COMMENT_SYMBOL) {
			rowIterator = new RowIterator(reader, readCommentSection(reader));
		} else if (firstSymbol == -1) {
			rowIterator = new RowIterator(reader, (String) null);
		} else {
			rowIterator = new RowIterator(reader, metadata);
			rowIterator.next();
		}
		return rowIterator;
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
			parsed = row.length == 0 ? EMPTY_LINE : parseValues(row);
		}

		@Override
		public void processEnded(ParsingContext context) {
			// do nothing
		}
	}

	static CsvParserSettings prepareParserSettings(RowProcessor rowProcessor) {
		final CsvParserSettings settings = new CsvParserSettings();
		settings.setInputBufferSize(16 * 1024);
		settings.setReadInputOnSeparateThread(false);
		settings.setCommentCollectionEnabled(false);
		settings.setEmptyValue("");
		settings.setNullValue(null);
		settings.setNumberOfRowsToSkip(1);
		settings.setSkipEmptyLines(false);
		settings.setProcessor(rowProcessor);
		return settings;
	}

	private static String readCommentSection(Reader reader) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(reader);
		StringBuilder buffer = new StringBuilder();
		String line;
		while ((line = bufferedReader.readLine()) != null) {
			buffer.append(line.charAt(0) == COMMENT_SYMBOL ? line.substring(1) : line);
		}
		return buffer.toString();
	}

	private void fillCommentSectionWithParsedComments() {
		final String comments = csvParser.getContext().currentParsedContent();
		if (comments == null) {
			return;
		}
		int startIndex = comments.indexOf(COMMENT_SYMBOL) + 1;
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
		if (csvParser.parseNext() == null) {
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
		if (this.csvParser != null) {
			this.csvParser.stopParsing();
		}
		if (this.decoratedReader != null) {
			this.decoratedReader.close();
		}
	}
}
