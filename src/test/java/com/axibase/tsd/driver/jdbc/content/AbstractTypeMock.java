package com.axibase.tsd.driver.jdbc.content;

import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import org.junit.Test;

public abstract class AbstractTypeMock extends AbstractFetchTest {
	@Test
	public void testType() throws Exception {
		try (final IStoreStrategy storeStrategy = getStrategyObject()) {
			fetch(storeStrategy, String.format("/csv/%s.csv", getTable()), 1);
		}
	}

	protected abstract String getTable();
}
