package com.axibase.tsd.driver.jdbc.content;

import com.axibase.tsd.driver.jdbc.TestUtil;
import com.axibase.tsd.driver.jdbc.intf.ParserRowContext;
import com.axibase.tsd.driver.jdbc.strategies.Trojan;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.calcite.avatica.ColumnMetaData;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.StringReader;
import java.util.List;

import static com.axibase.tsd.driver.jdbc.content.UnivocityParserRowContext.lastCharForColumn;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class UnivocityParserRowContextTest {
	private static final String CSV = TestUtil.resourceToString("/csv/csvWithQuotes.csv", UnivocityParserRowContext.class);
	private static final List<ColumnMetaData> metadata = TestUtil.prepareMetadata("/csv/csvWithQuotes.csv", UnivocityParserRowContext.class);
	private static final CsvParserSettings settings = Trojan.preparesSettings();
	private static final String FIRST_LINE_AFTER_HEADER = "entity-1,24.4,\"hello\nworld\"\n";

	private CsvParser parser;
	private ParserRowContext rowContext;

	@Before
	public void init() {
		parser = new CsvParser(settings);
		parser.beginParsing(new StringReader(CSV));
		rowContext = PowerMockito.spy(new UnivocityParserRowContext(parser.getContext(), metadata.size()));
		parser.parseNext();
	}

	@Test
	public void testGetLine() throws Exception {
		assertThat(rowContext.getLine(), is(2L));
	}

	@Test
	public void testGetRowSource() throws Exception {
		assertThat(rowContext.getRowSource(), is(FIRST_LINE_AFTER_HEADER));
		rowContext.getLine();
		// assure that row is cached because parser may clear buffer
		assertThat(rowContext.getRowSource(), is(FIRST_LINE_AFTER_HEADER));
	}

	@Test
	public void testGetColumnSource() throws Exception {
		assertThat(rowContext.getColumnSource(0), is("entity-1"));
		assertThat(rowContext.getColumnSource(1), is("24.4"));
		assertThat(rowContext.getColumnSource(2), is("\"hello\nworld\""));
	}

	@Test
	public void testGetColumnSourceOnLineWithEmptyText() throws Exception {
		parser.parseNext();
		parser.parseNext();
		assertThat(rowContext.getColumnSource(0), is("entity-3"));
		assertThat(rowContext.getColumnSource(1), is("26.1"));
		assertThat(rowContext.getColumnSource(2), is(""));
	}

	@Test
	public void testHasQuote() throws Exception {
		assertThat(rowContext.hasQuote(0), is(false));
		assertThat(rowContext.hasQuote(1), is(false));
		assertThat(rowContext.hasQuote(2), is(true));
	}

	@Test
	public void testLastCharForColumn() throws Exception {
		final int[] indexes = new int[3];
		assertThat(lastCharForColumn(0, indexes, "entity-1,24.4,\"hello\nworld\"\n"), is('1'));
		assertThat(lastCharForColumn(1, indexes, "entity-1,24.4,\"hello\nworld\"\n"), is('4'));
		assertThat(lastCharForColumn(2, indexes, "entity-1,24.4,\"hello\nworld\"\n"), is('"'));
		assertThat(lastCharForColumn(0, indexes, ",second,third"), is('\0'));
		assertThat(lastCharForColumn(2, indexes, "secondBeforeLast,beforeLast,"), is('\0'));
	}

}