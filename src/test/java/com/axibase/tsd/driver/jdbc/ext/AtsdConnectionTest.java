package com.axibase.tsd.driver.jdbc.ext;

import com.axibase.tsd.driver.jdbc.AtsdDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

public class AtsdConnectionTest {

	private AtsdConnection connection;

	@Before
	public void before() {
		final Properties info = new Properties();
		info.setProperty("url", "test:8443");
		final AtsdVersion atsdVersion = new AtsdVersion(20000, "Community Edition");
		connection = new AtsdConnection(new AtsdDriver(), new AtsdFactory(atsdVersion), "atsd:jdbc://test:8443", info);
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

		query = "insert into \"metric\" (time, entity, value, text, tags, \"tags.test\", \"metric.timeZone\", \"entity.tags\") values (?,?,?,?,?,?,?,?)";
		expected = "insert into \"metric\" (\"time\", entity, \"value\", text, tags, \"tags.test\", \"metric.timeZone\", \"entity.tags\")" +
				" values (?,?,?,?,?,?,?,?)";
		Assert.assertEquals(expected, connection.nativeSQL(query));

		query = "update \"metric\" set time=?, value=?, text=?, tags=?, tags.test=?, metric.timeZone=?, entity.tags=?) where entity=?";
		expected = "update \"metric\" set \"time\"=?, \"value\"=?, text=?, tags=?, \"tags.test\"=?, \"metric.timeZone\"=?, \"entity.tags\"=?) where entity=?";
		Assert.assertEquals(expected, connection.nativeSQL(query));
	}

}
