package com.axibase.tsd.driver.jdbc.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AtsdCommandConverterTest {

    private AtsdCommandConverter converter;

    @Before
    public void setUp() {
        converter = new AtsdCommandConverter();
    }

    @Test
    public void testConvertInsertToSeries() throws SqlParseException {
        String sql = "INSERT INTO temperature (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 24.5, null, " +
                "'Celcius')";

        String command = converter.convertSqlToCommand(sql);
        String expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:temperature=24.5\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'Celcius')";

        command = converter.convertSqlToCommand(sql, Collections.<Object>singletonList(24.5));
        Assert.assertEquals(expected, command);

        List<List<Object>> valueBatch = new ArrayList<>();
        valueBatch.add(Collections.<Object>singletonList(14.5));
        valueBatch.add(Collections.<Object>singletonList(34.5));
        command = converter.convertBatchToCommands(sql, valueBatch);
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:temperature=14.5\n" +
                "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:temperature=34.5\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO atsd_series (entity, metric, datetime, value, text, tags.unit)  VALUES ('sensor-01', 'temperature', " +
                "'2017-06-21T00:00:00Z', 33.5, 'Hello', 'Celcius')";
        command = converter.convertSqlToCommand(sql);
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:temperature=33.5 x:temperature=\"Hello\"\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO atsd_series (entity, datetime, temperature, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 45.5, null, null)";
        command = converter.convertSqlToCommand(sql);
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z m:temperature=45.5\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO atsd_series (entity, datetime, text, tags.location, temperature, speed)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', " +
                "null, 'Vienna', 55.5, 120)";
        command = converter.convertSqlToCommand(sql);
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:location=\"Vienna\" m:temperature=55.5 m:speed=120.0\n";
        Assert.assertEquals(expected, command);

    }

}
