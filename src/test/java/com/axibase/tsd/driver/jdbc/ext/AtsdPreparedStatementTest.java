package com.axibase.tsd.driver.jdbc.ext;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.axibase.tsd.driver.jdbc.AtsdProperties;
import com.axibase.tsd.driver.jdbc.TestConstants;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLDataException;
import java.sql.SQLException;
import org.apache.calcite.avatica.AvaticaConnection;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.slf4j.LoggerFactory;

public class AtsdPreparedStatementTest extends AtsdProperties {

    {
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.TRACE);
        setLoggerLevel("org.apache.calcite.sql.pretty.SqlPrettyWriter", Level.INFO);
    }

    private static void setLoggerLevel(String name, Level level) {
        Logger logger = (Logger) LoggerFactory.getLogger(name);
        logger.setLevel(level);
    }

    private AvaticaConnection connection;

    @Before
    public void before() throws SQLException {
        connection = (AvaticaConnection) DriverManager.getConnection(JDBC_ATSD_URL, LOGIN_NAME, LOGIN_PASSWORD);
        Assert.assertNotNull(connection);
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testGetMetaData_MetricDoesNotExist() throws SQLException {
        expectedException.expect(SQLDataException.class);
        expectedException.expectMessage("Metric 'test_metric' not found");
        try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM test_metric")) {
            stmt.getMetaData();
        }
    }

    @Test
    public void testGetMetaData() throws SQLException {
        AtsdConnection connection = (AtsdConnection) this.connection;
        try (PreparedStatement stmt = connection.prepareStatement("SELECT * FROM gc_time_percent ")) {
            ResultSetMetaData rsmd = stmt.getMetaData();
            Assert.assertEquals(7, rsmd.getColumnCount());
        }
    }

}
