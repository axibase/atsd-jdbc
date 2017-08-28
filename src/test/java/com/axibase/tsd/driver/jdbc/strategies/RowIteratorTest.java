package com.axibase.tsd.driver.jdbc.strategies;

import com.axibase.tsd.driver.jdbc.TestUtil;
import org.junit.Test;

import java.io.IOException;
import java.io.StringReader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RowIteratorTest {
	@Test
	public void testEmptyLines() throws IOException {
		String header = "\"tags.absent\"";
		final String content = header + "\n\n\n\n";
		final RowIterator rowIterator = RowIterator.newDefaultIterator(new StringReader(content), TestUtil.prepareMetadata(header));
		int rowNumber = 0;
		while (rowIterator.hasNext()) {
			++ rowNumber;
			final Object[] row = rowIterator.next();
			assertThat(row.length, is(1));
			assertThat(row[0], is((Object) null));
		}
		assertThat(rowNumber, is(3));
	}
}
