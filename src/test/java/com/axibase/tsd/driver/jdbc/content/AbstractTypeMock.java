package com.axibase.tsd.driver.jdbc.content;

import com.axibase.tsd.driver.jdbc.enums.Location;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionInfo;
import com.axibase.tsd.driver.jdbc.intf.IStoreStrategy;
import com.axibase.tsd.driver.jdbc.protocol.SdkProtocolImpl;
import org.apache.calcite.avatica.Meta;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import static com.axibase.tsd.driver.jdbc.AtsdProperties.HTTP_ATSD_URL;
import static com.axibase.tsd.driver.jdbc.AtsdProperties.LOGIN_NAME;
import static com.axibase.tsd.driver.jdbc.AtsdProperties.LOGIN_PASSWORD;
import static org.junit.Assert.assertTrue;

public abstract class AbstractTypeMock extends AbstractFetchTest {
	private static final String CONTEXT_START = "{";

	@Before
	public void setUp() throws Exception {
		final Properties properties = new Properties();
		properties.setProperty("host", HTTP_ATSD_URL);
		properties.setProperty("user", LOGIN_NAME);
		properties.setProperty("password", LOGIN_PASSWORD);
		AtsdConnectionInfo info = new AtsdConnectionInfo(properties);
		final String endpoint = Location.SQL_ENDPOINT.getUrl(info);
		final Meta.StatementHandle statementHandle = new Meta.StatementHandle("12345678", 1, null);
		final StatementContext context = new StatementContext(statementHandle, false);
		final ContentDescription contentDescription = new ContentDescription(
				endpoint, info, "SELECT * FROM " + getTable(), context);
		contentDescription.setJsonScheme(getSchema());
		this.protocolImpl = PowerMockito.spy(new SdkProtocolImpl(contentDescription));
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
		try (final InputStream inputStream = this.getClass().getResourceAsStream(getJsonSchema());
			 final Scanner scanner = new Scanner(inputStream)) {
			scanner.useDelimiter("\\A");
			String json = scanner.hasNext() ? scanner.next() : "";
			assertTrue(json != null && json.length() != 0 && json.startsWith(CONTEXT_START));
			return json;
		}
	}
}
