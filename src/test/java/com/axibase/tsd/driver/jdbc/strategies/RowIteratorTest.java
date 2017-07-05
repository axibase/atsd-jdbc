package com.axibase.tsd.driver.jdbc.strategies;

import java.io.StringReader;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import com.axibase.tsd.driver.jdbc.TestUtil;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RowIteratorTest {
	@Test
	public void testEmptyLines() {
		String header = "\"tags.absent\"";
		final String content = header + "\n\n\n\n";
		final RowIterator rowIterator = RowIterator.newDefaultIterator(new StringReader(content), TestUtil.prepareMetadata(header),
				DriverConstants.MIN_SUPPORTED_ATSD_REVISION);
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
