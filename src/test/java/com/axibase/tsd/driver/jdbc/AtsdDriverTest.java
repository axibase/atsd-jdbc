package com.axibase.tsd.driver.jdbc;

import com.axibase.tsd.driver.jdbc.enums.AtsdDriverConnectionProperties;
import com.axibase.tsd.driver.jdbc.ext.AtsdConnectionTest;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.*;

public class AtsdDriverTest {
	private AtsdDriver driver;
	private AvaticaConnection conn;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Before
	public void setUp() throws Exception {
		this.driver = new AtsdDriver();
		this.conn = AtsdConnectionTest.createTestConnection();
	}

	@Test
	public void testCreateDriverVersion() {
		DriverVersion driverVersion = driver.createDriverVersion();
		assertNotNull(driverVersion);
	}

	@Test
	public void testGetConnectStringPrefix() {
		String connectStringPrefix = driver.getConnectStringPrefix();
		assertNotNull(connectStringPrefix);
	}

	@Test
	public void testCreateMetaAvaticaConnection() {
		Meta meta = driver.createMeta(conn);
		assertNotNull(meta);
	}

	@Test
	public void testConnectionProperties() throws SQLException {
		Collection<ConnectionProperty> properties = driver.getConnectionProperties();
		assertTrue(properties.containsAll(Arrays.asList(AtsdDriverConnectionProperties.values())));
		DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(null, new Properties());
		assertNotNull(propertyInfo);
		assertEquals(13, propertyInfo.length);

	}

	@Test
	public void testAcceptsURLString() {
		boolean accepted = driver.acceptsURL(DriverConstants.CONNECT_URL_PREFIX);
		assertTrue(accepted);
	}

}
