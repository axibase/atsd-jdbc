package com.axibase.tsd.driver.jdbc.content;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import com.axibase.tsd.driver.jdbc.DriverConstants;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import static com.axibase.tsd.driver.jdbc.TestConstants.SELECT_ALL_CLAUSE;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTypeMock extends AbstractFetchTest {
	protected static final String CONTEXT_START = "{";
	protected boolean isDefaultStrategy;

	@Before
	public void setUp() throws Exception {
		final Properties properties = new Properties();
		properties.setProperty("host", HTTP_ATSD_URL);
		properties.setProperty("user", LOGIN_NAME);
		properties.setProperty("password", LOGIN_PASSWORD);
		AtsdConnectionInfo info = new AtsdConnectionInfo(properties);
		final String endpoint = info.toEndpoint(DriverConstants.SQL_ENDPOINT);
		final ContentDescription cd = new ContentDescription(endpoint, info, SELECT_ALL_CLAUSE + getTable(), new StatementContext());
		cd.setJsonScheme(getSchema());
		this.protocolImpl = PowerMockito.spy(new SdkProtocolImpl(cd));
		isDefaultStrategy = READ_STRATEGY == null || READ_STRATEGY.equalsIgnoreCase("stream");
	}

	@Test
	public void testType() throws Exception {
		try (final IStoreStrategy storeStrategy = getMockStrategyObject()) {
			fetch(storeStrategy, String.format("/csv/%s.csv", getTable()), 1);
		}
	}

	protected abstract String getTable();

	protected String getJsonSchema() {
		return String.format("/json/%s.jsonld", getTable());
	}

	protected String getSchema() throws IOException {
		try (final InputStream is = this.getClass().getResourceAsStream(getJsonSchema());
			 final Scanner scanner = new Scanner(is)) {
			scanner.useDelimiter("\\A");
			String json = scanner.hasNext() ? scanner.next() : "";
			assertTrue(json != null && json.length() != 0 && json.startsWith(CONTEXT_START));
			return json;
		}
	}
}
