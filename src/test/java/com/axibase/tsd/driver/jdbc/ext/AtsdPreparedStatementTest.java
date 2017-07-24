package com.axibase.tsd.driver.jdbc.ext;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.axibase.tsd.driver.jdbc.AtsdProperties;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.util.ISO8601Utils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class AtsdPreparedStatementTest extends AtsdProperties {
	private static final Map<String, Level> OVERRIDDEN_LOG_LEVELS = new HashMap<>(2);

	private static final String ENTITY_NAME = "entity1";
	private static final String METRIC_NAME = "metric1";
	private static final String INSERT = "INSERT INTO " + METRIC_NAME + " (time, entity, value, tags) VALUES (?,?,?,?)";

	private static AvaticaConnection connection;

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@BeforeClass
	public static void beforeClass() throws SQLException {
		connection = (AvaticaConnection) DriverManager.getConnection(JDBC_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
		Assert.assertNotNull(connection);

		setLoggerLevel(Logger.ROOT_LOGGER_NAME, Level.TRACE);
		setLoggerLevel("org.apache.calcite.sql.pretty.SqlPrettyWriter", Level.INFO);
	}

	@AfterClass
	public static void afterClass() throws SQLException {
		revertLoggerLevel(Logger.ROOT_LOGGER_NAME);
		revertLoggerLevel("org.apache.calcite.sql.pretty.SqlPrettyWriter");

		if (connection != null) {
			connection.close();
			connection = null;
		}
	}

	private static void setLoggerLevel(String name, Level level) {
		Logger logger = (Logger) LoggerFactory.getLogger(name);
		OVERRIDDEN_LOG_LEVELS.put(Logger.ROOT_LOGGER_NAME, logger.getEffectiveLevel());
		logger.setLevel(level);
	}

	private static void revertLoggerLevel(String name) {
		Logger logger = (Logger) LoggerFactory.getLogger(name);
		logger.setLevel(OVERRIDDEN_LOG_LEVELS.get(Logger.ROOT_LOGGER_NAME));
	}

	@Test
	public void testGetMetaData_MetricDoesNotExist() throws SQLException {
		try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM test_metric")) {
			ResultSetMetaData rsmd = stmt.getMetaData();
			Assert.assertEquals(7, rsmd.getColumnCount());
		}
	}

	@Test
	public void testGetMetaData() throws SQLException {
		try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM gc_time_percent ")) {
			ResultSetMetaData rsmd = stmt.getMetaData();
			Assert.assertEquals(7, rsmd.getColumnCount());
		}
	}

	@Test
	public void testSetters_Tags() throws SQLException, InterruptedException {
		testTags("t1=1");
		testTags("t2=2;t3=3");
	}

	@Test
	public void testSetters_Null() throws SQLException, InterruptedException {
		final long time = System.currentTimeMillis();
		final double value = 123.456;
		try (PreparedStatement stmt = connection.prepareStatement(INSERT)) {
			stmt.setLong(1, time);
			stmt.setString(2, ENTITY_NAME);
			stmt.setDouble(3, value);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
		Map<String, Object> last = getLasts(1);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(time, last.get("time"));
		Assert.assertEquals(value, (Double) last.get("value"), 0.001);
		Assert.assertNull(last.get("text"));
		Assert.assertNull(last.get("tags"));
	}

	@Test
	public void testSetters_InvalidValue() throws SQLException {
		expectedException.expect(SQLException.class);
		expectedException.expectMessage("Invalid value: Hello. Current type: String, expected type: Number");
		try (PreparedStatement stmt = connection.prepareStatement(INSERT)) {
			stmt.setLong(1, System.currentTimeMillis());
			stmt.setString(2, ENTITY_NAME);
			stmt.setString(3, "Hello");
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	@Test
	public void testSetters_InvalidTime() throws SQLException {
		expectedException.expect(SQLException.class);
		expectedException.expectMessage("Invalid value: 123. Current type: String, expected type: Number");
		try (PreparedStatement stmt = connection.prepareStatement(INSERT)) {
			stmt.setString(1, "123");
			stmt.setString(2, ENTITY_NAME);
			stmt.setDouble(3, 123.456);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	@Test
	public void testSetters_InvalidDateTime() throws SQLException {
		expectedException.expect(SQLException.class);
		expectedException.expectMessage("Invalid datetime value: 123. Expected formats: yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z', yyyy-MM-dd HH:mm:ss[.fffffffff]");
		final String sql = "INSERT INTO " + METRIC_NAME + " (datetime, entity, value, tags) VALUES (?,?,?,?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, "123");
			stmt.setString(2, ENTITY_NAME);
			stmt.setDouble(3, 123.456);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	private void testTags(String tags) throws SQLException, InterruptedException {
		final long time = System.currentTimeMillis();
		final double value = 123.456;
		insert(time, value, tags);
		Map<String, Object> last = getLasts(1);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(time, last.get("time"));
		Assert.assertEquals(value, (Double) last.get("value"), 0.001);
		Assert.assertNull(last.get("text"));
		Assert.assertEquals(tags, last.get("tags"));
	}

	private void insert(final long time, final double value, final String tags) throws SQLException {
		try (PreparedStatement stmt = connection.prepareStatement(INSERT)) {
			stmt.setLong(1, time);
			stmt.setString(2, ENTITY_NAME);
			stmt.setDouble(3, value);
			stmt.setString(4, tags);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	private Map<String, Object> getLasts(int limit) throws SQLException, InterruptedException {
		Thread.sleep(1000);
		final String sql = "SELECT time, value, text, tags FROM " + METRIC_NAME + " WHERE entity='" + ENTITY_NAME + "' ORDER BY time DESC LIMIT " + limit;
		try(PreparedStatement stmt = connection.prepareStatement(sql)) {
			try(ResultSet rs = stmt.executeQuery()) {
				Map<String, Object> map = new HashMap<>();
				if(rs.next()) {
					map.put("time", rs.getLong("time"));
					map.put("value", rs.getDouble("value"));
					map.put("text", rs.getString("text"));
					map.put("tags", rs.getString("tags"));
				}
				return map;
			}
		}
	}

	@Test
	public void testExecuteBatch() throws SQLException {
		try(PreparedStatement stmt = connection.prepareStatement(INSERT)) {
			final long time = System.currentTimeMillis();
			for (int i=0;i<3;i++) {
				stmt.setLong(1, time + i);
				stmt.setString(2, "entity_" + i);
				stmt.setDouble(3, i);
				stmt.setString(4, null);
				stmt.addBatch();
			}
			int[] res = stmt.executeBatch();
			Assert.assertEquals(3, res.length);
			Assert.assertArrayEquals(new int[] {1,1,1}, res);
		}
	}

	@Test
	public void testInsertDateTimeAsNumber() throws SQLException {
		final long time = System.currentTimeMillis();
		expectedException.expect(SQLException.class);
		expectedException.expectMessage("Invalid value: " + time + ". Current type: BigDecimal, expected type: Timestamp");
		final String sql = "INSERT INTO " + METRIC_NAME + " (datetime, entity, value, tags) VALUES (?,?,?,?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setDouble(1, time);
			stmt.setString(2, "entity_1");
			stmt.setDouble(3, 123);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	@Test
	public void testInsertDateTimeAsString() throws SQLException {
		final String dateTime = ISO8601Utils.format(new java.util.Date(), true);
		final String sql = "INSERT INTO " + METRIC_NAME + " (datetime, entity, value, tags) VALUES (?,?,?,?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, dateTime);
			stmt.setString(2, "entity_1");
			stmt.setDouble(3, 123);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

}
