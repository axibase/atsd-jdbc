package com.axibase.tsd.driver.jdbc.ext;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.axibase.tsd.driver.jdbc.AtsdProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.com.fasterxml.jackson.databind.util.ISO8601Utils;
import org.apache.commons.lang3.StringUtils;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static com.axibase.tsd.driver.jdbc.util.AtsdColumn.*;
import static com.axibase.tsd.driver.jdbc.TestUtil.buildVariablePrefix;
import static com.axibase.tsd.driver.jdbc.TestUtil.format;

public class AtsdPreparedStatementTest extends AtsdProperties {
	private static final Map<String, Level> OVERRIDDEN_LOG_LEVELS = new HashMap<>(2);

	private static final String INSERT = "INSERT INTO '{}' ({}, entity, value, tags) VALUES (?,?,?,?)";
	private static final double DEFAULT_VALUE = 123.456;

	private static AvaticaConnection connection;

	private long currentTime;

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

	@Before
	public void before() {
		currentTime = System.currentTimeMillis();
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
		final String metricName = buildVariablePrefix() + "metric";
		try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM '" + metricName + "'")) {
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
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		testTags(entityName + "-1", metricName + "-1","t1=1");
		testTags(entityName + "-2", metricName + "-2","t2=2;t3=3");
	}

	@Test
	public void testSetters_Null() throws SQLException, InterruptedException {
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
			stmt.setLong(1, currentTime);
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
		final String sql = "SELECT time, value, text, tags FROM '" + metricName + "' WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
		Map<String, Object> last = getLast(sql);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(currentTime, last.get(TIME));
		Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
		Assert.assertNull(last.get(TEXT));
		Assert.assertNull(last.get(TAGS));
	}

	@Test
	public void testSetters_InvalidValue() throws SQLException {
		expectedException.expect(SQLException.class);
		expectedException.expectMessage("Invalid value: Hello. Current type: String, expected type: Number");
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
			stmt.setLong(1, currentTime);
			stmt.setString(2, entityName);
			stmt.setString(3, "Hello");
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	@Test
	public void testSetters_InvalidTime() throws SQLException {
		expectedException.expect(SQLException.class);
		expectedException.expectMessage("Invalid value: 123. Current type: String, expected type: Number");
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
			stmt.setString(1, "123");
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	@Test
	public void testSetters_InvalidDateTime() throws SQLException {
		expectedException.expect(SQLException.class);
		expectedException.expectMessage("Invalid datetime value: 123. Expected formats: yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z', yyyy-MM-dd HH:mm:ss[.fffffffff]");
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, DATETIME))) {
			stmt.setString(1, "123");
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	private void testTags(final String entityName, final String metricName, final String tags) throws SQLException, InterruptedException {
		insert(entityName, metricName, currentTime, DEFAULT_VALUE, tags);
		final String sql = "SELECT time, value, text, tags FROM '" + metricName + "' WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
		Map<String, Object> last = getLast(sql);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(currentTime, last.get(TIME));
		Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
		Assert.assertNull(last.get(TEXT));
		Assert.assertEquals(tags, last.get(TAGS));
	}

	private void insert(final String entityName, final String metricName, final long time, final double value, final String tags) throws SQLException {
		try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
			stmt.setLong(1, time);
			stmt.setString(2, entityName);
			stmt.setDouble(3, value);
			stmt.setString(4, tags);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
	}

	private Map<String, Object> getLast(String sql) throws SQLException, InterruptedException {
		Thread.sleep(1000);
		try(PreparedStatement stmt = connection.prepareStatement(sql)) {
			try(ResultSet rs = stmt.executeQuery()) {
				ResultSetMetaData rsmd = rs.getMetaData();
				Map<String, Object> map = new HashMap<>();
				if(rs.next()) {
					for (int i=1;i<=rsmd.getColumnCount();i++) {
						map.put(rsmd.getColumnName(i), getValue(rs, i, rsmd.getColumnType(i)));
					}
				}
				return map;
			}
		}
	}

	private static Object getValue(ResultSet rs, int columnIndex, int columnType) throws SQLException {
		switch (columnType) {
			case Types.BIGINT : return rs.getLong(columnIndex);
			case Types.REAL : return rs.getDouble(columnIndex);
			case Types.TIMESTAMP : return rs.getTimestamp(columnIndex);
			case Types.BOOLEAN : return rs.getBoolean(columnIndex);
			case Types.JAVA_OBJECT : {
				String str = rs.getString(columnIndex);
				return StringUtils.isEmpty(str) ? null : str;
			}
			default : return rs.getString(columnIndex);
		}
	}

	@Test
	public void testExecuteBatch() throws SQLException {
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		try(PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, TIME))) {
			for (int i=0;i<3;i++) {
				stmt.setLong(1, currentTime + i);
				stmt.setString(2, entityName + '-' + i);
				stmt.setDouble(3, i);
				stmt.setString(4, null);
				stmt.addBatch();
			}
			int[] res = stmt.executeBatch();
			Assert.assertArrayEquals(new int[] {1,1,1}, res);
		}

		String sql = "INSERT INTO '" + metricName + "' (time, entity, value, entity.tags, metric.tags) VALUES (?,?,?,?,?)";
		List<List<Object>> batchValues = new ArrayList<>();
		batchValues.add(Arrays.<Object>asList(currentTime + 1, entityName, DEFAULT_VALUE + 1, null, "test1=value1"));
		batchValues.add(Arrays.<Object>asList(currentTime + 2, entityName, DEFAULT_VALUE + 2, "test1=value1", null));
		batchValues.add(Arrays.<Object>asList(currentTime + 3, entityName, DEFAULT_VALUE + 3, null, null));
		batchValues.add(Arrays.<Object>asList(currentTime + 4, entityName, DEFAULT_VALUE + 4, "test1=value1", "test1=value1"));
		try(PreparedStatement stmt = connection.prepareStatement(sql)) {
			for (List<Object> values : batchValues) {
				stmt.setLong(1, (Long) values.get(0));
				stmt.setString(2, (String) values.get(1));
				stmt.setDouble(3, (Double) values.get(2));
				stmt.setString(4, (String) values.get(3));
				stmt.setString(5, (String) values.get(4));
				stmt.addBatch();
			}
			int[] res = stmt.executeBatch();
			Assert.assertArrayEquals(new int[] {2,2,1,3}, res);
		}
	}

	@Test
	public void testInsertDatetimeAsNumber() throws SQLException, InterruptedException {
        final long time = System.currentTimeMillis();
        final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, DATETIME))) {
			stmt.setDouble(1, time);
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
		String sql = "SELECT datetime, value, tags FROM '" + metricName + "' WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
		Map<String, Object> last = getLast(sql);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(time, ((Timestamp) last.get(DATETIME)).getTime());
		Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
		Assert.assertNull(last.get(TAGS));
	}

	@Test
	public void testInsertDatetimeAsString() throws SQLException, InterruptedException {
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		try (PreparedStatement stmt = connection.prepareStatement(format(INSERT, metricName, DATETIME))) {
			stmt.setString(1, getISO(currentTime));
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, null);
			Assert.assertEquals(1, stmt.executeUpdate());
		}
		String sql = "SELECT datetime, value, tags FROM '" + metricName + "' WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
		Map<String, Object> last = getLast(sql);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(currentTime, ((Timestamp) last.get(DATETIME)).getTime());
		Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
		Assert.assertNull(last.get(TAGS));
	}

	@Test
	public void testInsertWithEntityColumns() throws SQLException, InterruptedException {
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		String sql = "INSERT INTO '" + metricName + "-1' (datetime, entity, value, tags, entity.label, entity.tags) VALUES (?,?,?,?,?,?)";
		final String entityLabel = entityName + "-label";
		final String entityTags = "test1=value1";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, getISO(currentTime));
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, null);
			stmt.setString(5, entityLabel);
			stmt.setString(6, entityTags);
			Assert.assertEquals(2, stmt.executeUpdate());
		}
		sql = "SELECT time, value, text, tags, entity.label, entity.tags FROM '" + metricName
				+ "-1' WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
		Map<String, Object> last = getLast(sql);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(currentTime, last.get(TIME));
		Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
		Assert.assertNull(last.get(TEXT));
		Assert.assertNull(last.get(TAGS));
		Assert.assertEquals(entityLabel, last.get(ENTITY_LABEL));
		Assert.assertEquals(entityTags, last.get(ENTITY_TAGS));

		sql = "INSERT INTO '" + metricName + "-2' (datetime, entity, value, tags, entity.label, entity.interpolate, entity.timeZone, entity.tags.test1)" +
				" VALUES (?,?,?,?,?,?,?,?)";
		final String entityTimeZone = "UTC";
		final String entityInterpolation = "linear";
		final String entityTagValue = "value1";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, getISO(currentTime));
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, null);
			stmt.setString(5, entityLabel);
			stmt.setString(6, entityInterpolation);
			stmt.setString(7, entityTimeZone);
			stmt.setString(8, entityTagValue);
			Assert.assertEquals(2, stmt.executeUpdate());
		}
		sql = "SELECT time, value, text, tags, entity.label, entity.interpolate, entity.timeZone, entity.tags FROM '" + metricName
				+ "-2' WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
		last = getLast(sql);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(currentTime, last.get(TIME));
		Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
		Assert.assertNull(last.get(TEXT));
		Assert.assertNull(last.get(TAGS));
		Assert.assertEquals(entityLabel, last.get(ENTITY_LABEL));
		Assert.assertEquals(entityInterpolation.toUpperCase(), last.get(ENTITY_INTERPOLATE));
		Assert.assertEquals(entityTimeZone, last.get(ENTITY_TIME_ZONE));
		Assert.assertEquals("test1=" + entityTagValue, last.get(ENTITY_TAGS));
	}

	@Test
	public void testUpdateWithMetricColumns() throws SQLException, InterruptedException {
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		String sql = "UPDATE '" + metricName + "-1' SET datetime=?, value=?, tags=?, metric.label=?, metric.tags=? WHERE entity=?";
		final String metricLabel = metricName + "-label";
		final String metricTags = "test1=value1";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, getISO(currentTime));
			stmt.setDouble(2, DEFAULT_VALUE);
			stmt.setString(3, null);
			stmt.setString(4, metricLabel);
			stmt.setString(5, metricTags);
			stmt.setString(6, entityName);
			Assert.assertEquals(2, stmt.executeUpdate());
		}
		sql = "SELECT time, value, text, tags, metric.label, metric.tags FROM '" + metricName
				+ "-1' WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
		Map<String, Object> last = getLast(sql);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(currentTime, last.get(TIME));
		Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
		Assert.assertNull(last.get(TEXT));
		Assert.assertNull(last.get(TAGS));
		Assert.assertEquals(metricLabel, last.get(METRIC_LABEL));
		Assert.assertEquals(metricTags, last.get(METRIC_TAGS));

		sql = "UPDATE '" + metricName + "-2' SET datetime=?, value=?, tags=?, metric.tags.test1=?, metric.label=?, metric.enabled=?, metric.interpolate=?" +
				", metric.timeZone=?, metric.description=?, metric.versioning=?, metric.filter=?, metric.units=? WHERE entity=?";
		final String metricTagValue = "M1";
		final boolean metricEnabled = true;
		final String metricTimeZone = "UTC";
		final String metricInterpolation = "linear";
		final String metricDescription = "description 1";
		final boolean metricVersioning = false;
		final String metricFilter = "filter1";
		final String metricUnits = "units1";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setString(1, getISO(currentTime));
			stmt.setDouble(2, DEFAULT_VALUE);
			stmt.setString(3, null);
			stmt.setString(4, metricTagValue);
			stmt.setString(5, metricLabel);
			stmt.setBoolean(6, metricEnabled);
			stmt.setString(7, metricInterpolation);
			stmt.setString(8, metricTimeZone);
			stmt.setString(9, metricDescription);
			stmt.setBoolean(10, metricVersioning);
			stmt.setString(11, metricFilter);
			stmt.setString(12, metricUnits);
			stmt.setString(13, entityName);
			Assert.assertEquals(2, stmt.executeUpdate());
		}
		sql = "SELECT time, value, text, tags, metric.name, metric.tags, metric.label, metric.enabled, metric.interpolate, metric.timeZone" +
				", metric.description, metric.versioning, metric.units, metric.minValue, metric.maxValue, metric.dataType, metric.filter" +
				", metric.invalidValueAction, metric.lastInsertTime, metric.persistent, metric.retentionIntervalDays, metric.timePrecision" +
				" FROM '" + metricName + "-2' WHERE entity='" + entityName + "' ORDER BY time DESC LIMIT 1";
		last = getLast(sql);
		Assert.assertFalse("No results", last.isEmpty());
		Assert.assertEquals(currentTime, last.get(TIME));
		Assert.assertEquals(DEFAULT_VALUE, (Double) last.get(VALUE), 0.001);
		Assert.assertNull(last.get(TEXT));
		Assert.assertNull(last.get(TAGS));
		Assert.assertEquals(metricName + "-2", last.get(METRIC_NAME));
		Assert.assertEquals("test1=" + metricTagValue, last.get(METRIC_TAGS));
		Assert.assertEquals(metricLabel, last.get(METRIC_LABEL));
		Assert.assertTrue(Boolean.valueOf((String) last.get(METRIC_ENABLED)));
		Assert.assertEquals(metricInterpolation.toUpperCase(), last.get(METRIC_INTERPOLATE));
		Assert.assertEquals(metricTimeZone, last.get(METRIC_TIME_ZONE));
		Assert.assertEquals(metricDescription, last.get(METRIC_DESCRIPTION));
		Assert.assertFalse(Boolean.valueOf((String) last.get(METRIC_VERSIONING)));
		Assert.assertEquals(metricUnits, last.get(METRIC_UNITS));
		Assert.assertNull(metricUnits, last.get(METRIC_MIN_VALUE));
		Assert.assertNull(metricUnits, last.get(METRIC_MAX_VALUE));
		Assert.assertEquals(metricFilter, last.get(METRIC_FILTER));
		Assert.assertEquals("NONE", last.get(METRIC_INVALID_VALUE_ACTION));
		Assert.assertNotNull(last.get(METRIC_LAST_INSERT_TIME));
		Assert.assertTrue(Boolean.valueOf((String) last.get(METRIC_PERSISTENT)));
		Assert.assertEquals("MILLISECONDS", last.get(METRIC_TIME_PRECISION));
	}

	@Test
	public void testNotInsertedField_EntityGroups() throws SQLException {
		expectedException.expect(SQLFeatureNotSupportedException.class);
		expectedException.expectMessage(ENTITY_GROUPS);
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		String sql = "INSERT INTO '" + metricName + "' (time, entity, value, entity.groups) VALUES (?,?,?,?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setLong(1, currentTime);
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, "group1");
			stmt.executeUpdate();
		}
	}

	@Test
	public void testNotInsertedField_MetricLastInsertTime() throws SQLException {
		expectedException.expect(SQLFeatureNotSupportedException.class);
		expectedException.expectMessage(METRIC_LAST_INSERT_TIME);
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		String sql = "INSERT INTO '" + metricName + "' (time, entity, value, metric.lastInsertTime) VALUES (?,?,?,?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setLong(1, currentTime);
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setLong(4, System.currentTimeMillis());
			stmt.executeUpdate();
		}
	}

	@Test
	public void testNotInsertedField_MetricPersistent() throws SQLException {
		expectedException.expect(SQLFeatureNotSupportedException.class);
		expectedException.expectMessage(METRIC_PERSISTENT);
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		String sql = "INSERT INTO '" + metricName + "' (time, entity, value, metric.persistent) VALUES (?,?,?,?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setLong(1, currentTime);
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setBoolean(4, false);
			stmt.executeUpdate();
		}
	}

	@Test
	public void testNotInsertedField_MetricRetentionIntervalDays() throws SQLException {
		expectedException.expect(SQLFeatureNotSupportedException.class);
		expectedException.expectMessage(METRIC_RETENTION_INTERVAL_DAYS);
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		String sql = "INSERT INTO '" + metricName + "' (time, entity, value, metric.retentionIntervalDays) VALUES (?,?,?,?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setLong(1, currentTime);
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setInt(4, 1);
			stmt.executeUpdate();
		}
	}

	@Test
	public void testNotInsertedField_MetricTimePrecision() throws SQLException {
		expectedException.expect(SQLFeatureNotSupportedException.class);
		expectedException.expectMessage(METRIC_TIME_PRECISION);
		final String entityName = buildVariablePrefix() + "entity";
		final String metricName = buildVariablePrefix() + "metric";
		String sql = "INSERT INTO '" + metricName + "' (time, entity, value, metric.timePrecision) VALUES (?,?,?,?)";
		try (PreparedStatement stmt = connection.prepareStatement(sql)) {
			stmt.setLong(1, currentTime);
			stmt.setString(2, entityName);
			stmt.setDouble(3, DEFAULT_VALUE);
			stmt.setString(4, "seconds");
			stmt.executeUpdate();
		}
	}

	private static  String getISO(long time) {
		return ISO8601Utils.format(new java.util.Date(time), true);
	}
}
