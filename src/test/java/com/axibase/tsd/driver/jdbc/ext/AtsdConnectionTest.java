package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.AtsdProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class AtsdConnectionTest extends AtsdProperties {

	private AtsdConnection connection;

	@Before
	public void before() {
		try {
			connection = (AtsdConnection) DriverManager.getConnection(JDBC_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
		} catch (SQLException e) {
			fail(e.getMessage());
		}
		assertNotNull(connection);
	}

	@After
	public void after() throws SQLException {
		if (connection != null) {
			connection.close();
		}
	}

	@Test
	public void testNativeSQL() throws SQLException {
		Assert.assertNull(connection.nativeSQL(null));

		String query = "test";
		Assert.assertEquals(query, connection.nativeSQL(query));

		query = "select * from metric";
		Assert.assertEquals(query, connection.nativeSQL(query));

		query = "insert into metric (time, entity, value, text, tags, tags.test, metric.timeZone, entity.tags) values (?,?,?,?,?,?,?,?)";
		String expected = "insert into metric (\"time\", entity, \"value\", text, tags, \"tags.test\", \"metric.timeZone\", \"entity.tags\")" +
				" values (?,?,?,?,?,?,?,?)";
		Assert.assertEquals(expected, connection.nativeSQL(query));

		query = "insert into 'metric' (time, entity, value, text, tags, 'tags.test', 'metric.timeZone', 'entity.tags') values (?,?,?,?,?,?,?,?)";
		expected = "insert into \"metric\" (\"time\", entity, \"value\", text, tags, \"tags.test\", \"metric.timeZone\", \"entity.tags\")" +
				" values (?,?,?,?,?,?,?,?)";
		Assert.assertEquals(expected, connection.nativeSQL(query));

		query = "update 'metric' set time=?, value=?, text=?, tags=?, tags.test=?, metric.timeZone=?, entity.tags=?) where entity=?";
		expected = "update \"metric\" set \"time\"=?, \"value\"=?, text=?, tags=?, \"tags.test\"=?, \"metric.timeZone\"=?, \"entity.tags\"=?) where entity=?";
		Assert.assertEquals(expected, connection.nativeSQL(query));
	}

}
