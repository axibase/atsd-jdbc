package com.axibase.tsd.driver.jdbc.content;

public class LongTypeMock extends AbstractTypeMock {
	private static final String TYPE_MOCK_TABLE = "jdbc.driver.test.metric.long";

	@Override
	protected String getTable() {
		return TYPE_MOCK_TABLE;
	}
}
