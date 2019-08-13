package com.axibase.tsd.driver.jdbc.content;

import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import org.junit.Test;

public class DecimalTypeMock extends AbstractTypeMock {
	private static final String TYPE_MOCK_TABLE = "jdbc.driver.test.metric.decimal";
	private static final String TYPE_MOCK_TABLE_200 = TYPE_MOCK_TABLE + ".200";

	@Override
	protected String getTable() {
		return TYPE_MOCK_TABLE;
	}

	@Test
	public void testBidDecimalsType() throws Exception {
		try (final IStoreStrategy storeStrategy = getStrategyObject()) {
			fetch(storeStrategy, String.format("/csv/%S.csv", TYPE_MOCK_TABLE_200), 200);
		}
	}
}
