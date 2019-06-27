package com.axibase.tsd.driver.jdbc.content;

public class IntTypeMock extends AbstractTypeMock {
	private static final String TYPE_MOCK_TABLE = "jdbc.driver.test.metric.int";

	@Override
	protected String getTable() {
		return TYPE_MOCK_TABLE;
	}
}
