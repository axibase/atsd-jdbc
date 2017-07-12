package com.axibase.tsd.driver.jdbc.converter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.avatica.Meta;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class AtsdSqlConverterTest {

    @Test
    public void testConvertInsertToSeries() throws SQLException {
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT);
        String sql = "INSERT INTO test.temperature (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 24.5, null, " +
                "'Celcius')";

        String command = converter.convertToCommand(sql);
        String expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:test.temperature=24.5\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'Celcius')";

        command = converter.convertToCommand(sql, Collections.<Object>singletonList(24.5));
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:temperature=24.5\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'unit=Celcius')";

        command = converter.convertToCommand(sql, Collections.<Object>singletonList(24.5));
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'unit1=Celcius;unit2=test')";

        command = converter.convertToCommand(sql, Collections.<Object>singletonList(24.5));
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit1=\"Celcius\" t:unit2=\"test\" m:temperature=24.5\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'unit=Celcius')";

        List<List<Object>> valuesBatch = new ArrayList<>();
        valuesBatch.add(Collections.<Object>singletonList(14.5));
        valuesBatch.add(Collections.<Object>singletonList(34.5));
        command = converter.convertBatchToCommands(sql, valuesBatch);
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:temperature=14.5\n" +
                "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:temperature=34.5\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO atsd_series (entity, metric, datetime, value, text, tags.unit)  VALUES ('sensor-01', 'temperature', " +
                "'2017-06-21T00:00:00Z', 33.5, 'Hello', 'Celcius')";
        command = converter.convertToCommand(sql);
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:temperature=33.5 x:temperature=\"Hello\"\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO atsd_series (entity, datetime, temperature, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 45.5, null, null)";
        command = converter.convertToCommand(sql);
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z m:temperature=45.5\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO atsd_series (entity, datetime, text, tags.location, temperature, speed)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', " +
                "null, 'Vienna', 55.5, 120)";
        command = converter.convertToCommand(sql);
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:location=\"Vienna\" m:temperature=55.5 m:speed=120.0\n";
        Assert.assertEquals(expected, command);

        sql = "INSERT INTO atsd_series (entity, time, text, tags.location, temperature, speed)  VALUES ('sensor-01', 123456789, " +
                "null, 'Vienna', 55.5, 120)";
        command = converter.convertToCommand(sql);
        expected = "series e:sensor-01 ms:123456789 t:location=\"Vienna\" m:temperature=55.5 m:speed=120.0\n";
        Assert.assertEquals(expected, command);
    }

    @Test
    public void testConverUpdateToSeries() throws SQLException {
        AtsdSqlUpdateConverter converter = (AtsdSqlUpdateConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.UPDATE);
        String sql = "update test.temperature set datetime='2017-06-21T00:00:00Z', value=24.5, tags.unit='celcius' where entity='sensor-1'";
        String command = converter.convertToCommand(sql);
        String expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=\"celcius\" m:test.temperature=24.5\n";
        Assert.assertEquals(expected, command);

        sql = "update temperature set datetime='2017-06-21T00:00:00Z', value=24.5, tags.unit='celcius', text='test' where entity='sensor-1' AND tags" +
                ".unit2='celc'";
        command = converter.convertToCommand(sql);
        expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=\"celcius\" t:unit2=\"celc\" m:temperature=24.5 x:temperature=\"test\"\n";
        Assert.assertEquals(expected, command);

        sql = "update temperature set datetime=?, value=?, tags.unit=? where entity='sensor-1'";
        command = converter.convertToCommand(sql, Arrays.<Object>asList("2017-06-21T00:00:00Z", 24.5, "celcius"));
        expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=\"celcius\" m:temperature=24.5\n";
        Assert.assertEquals(expected, command);

        sql = "update temperature set datetime=?, value=?, tags.unit=? where entity='sensor-1'";
        List<List<Object>> valuesBatch = new ArrayList<>();
        valuesBatch.add(Arrays.<Object>asList("2017-06-21T00:00:00Z", 24.5, "celcius1"));
        valuesBatch.add(Arrays.<Object>asList("2017-06-22T00:00:00Z", 25.5, "celcius2"));
        valuesBatch.add(Arrays.<Object>asList("2017-06-23T00:00:00Z", 26.5, "celcius3"));
        command = converter.convertBatchToCommands(sql, valuesBatch);
        expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=\"celcius1\" m:temperature=24.5\n" +
                "series e:sensor-1 d:2017-06-22T00:00:00Z t:unit=\"celcius2\" m:temperature=25.5\n" +
                "series e:sensor-1 d:2017-06-23T00:00:00Z t:unit=\"celcius3\" m:temperature=26.5\n";
        Assert.assertEquals(expected, command);

        sql = "update atsd_series set datetime='2017-06-21T00:00:00Z', value=24.5, text='test', tags.unit='celcius' where entity='sensor-1' and " +
                "metric='temperature'";
        command = converter.convertToCommand(sql);
        expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=\"celcius\" m:temperature=24.5 x:temperature=\"test\"\n";
        Assert.assertEquals(expected, command);

        sql = "update atsd_series set time=123456789, value=24.5, text='test', tags.unit='celcius' where entity='sensor-1' and " +
                "metric='temperature'";
        command = converter.convertToCommand(sql);
        expected = "series e:sensor-1 ms:123456789 t:unit=\"celcius\" m:temperature=24.5 x:temperature=\"test\"\n";
        Assert.assertEquals(expected, command);
    }

    @Test
    public void testConvertInsertToSeriesWithEscapedTableName() throws SQLException {
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT);
        String sql = "INSERT INTO 'test.temperature' (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 24.5, null, " +
                "'Celcius')";

        String command = converter.convertToCommand(sql);
        String expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=\"Celcius\" m:test.temperature=24.5\n";
        Assert.assertEquals(expected, command);
    }

    @Test
    public void testConvertUpdateToSeriesWithEscapedTableName() throws SQLException {
        AtsdSqlUpdateConverter converter = (AtsdSqlUpdateConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.UPDATE);
        String sql = "update 'test.temperature' set datetime='2017-06-21T00:00:00Z', value=24.5, tags.unit='celcius' where entity='sensor-1'";
        String command = converter.convertToCommand(sql);
        String expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=\"celcius\" m:test.temperature=24.5\n";
        Assert.assertEquals(expected, command);
    }

}
