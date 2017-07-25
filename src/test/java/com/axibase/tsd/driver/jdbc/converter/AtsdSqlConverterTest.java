package com.axibase.tsd.driver.jdbc.converter;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.avatica.Meta;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AtsdSqlConverterTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testConvertInsertToSeries() throws SQLException {
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, false);
        String sql = "INSERT INTO test.temperature (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 24.5, null, " +
                "'Celcius')";

        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        String expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=Celcius m:test.temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'Celcius')";

        commands = converter.convertToCommands(sql, Collections.<Object>singletonList(24.5));
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=Celcius m:temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'unit=Celcius')";

        commands = converter.convertToCommands(sql, Collections.<Object>singletonList(24.5));
        Assert.assertEquals(1, commands.size());
        Assert.assertEquals(expected, commands.get(0));

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'unit1=Celcius;unit2=test')";

        commands = converter.convertToCommands(sql, Collections.<Object>singletonList(24.5));
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit1=Celcius t:unit2=test m:temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));

        sql = "INSERT INTO temperature (entity, datetime, value, text, tags)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', ?, null, " +
                "'unit=Celcius')";

        List<List<Object>> valuesBatch = new ArrayList<>();
        valuesBatch.add(Collections.<Object>singletonList(14.5));
        valuesBatch.add(Collections.<Object>singletonList(34.5));
        commands = converter.convertBatchToCommands(sql, valuesBatch);
        Assert.assertEquals(2, commands.size());
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=Celcius m:temperature=14.5\n" +
                "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=Celcius m:temperature=34.5";
        Assert.assertEquals(expected, StringUtils.join(commands, '\n'));

        sql = "INSERT INTO atsd_series (entity, metric, datetime, value, text, tags.unit)  VALUES ('sensor-01', 'temperature', " +
                "'2017-06-21T00:00:00Z', 33.5, 'Hello', 'Celcius')";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=Celcius m:temperature=33.5 x:temperature=Hello";
        Assert.assertEquals(expected, commands.get(0));

        sql = "INSERT INTO atsd_series (entity, datetime, temperature, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 45.5, null, null)";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z m:temperature=45.5";
        Assert.assertEquals(expected, commands.get(0));

        sql = "INSERT INTO atsd_series (entity, datetime, text, tags.location, temperature, speed)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', " +
                "null, 'Vienna', 55.5, 120)";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:location=Vienna m:temperature=55.5 m:speed=120.0";
        Assert.assertEquals(expected, commands.get(0));

        sql = "INSERT INTO atsd_series (entity, time, text, tags.location, temperature, speed)  VALUES ('sensor-01', 123456789, " +
                "null, 'Vienna', 55.5, 120)";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-01 ms:123456789 t:location=Vienna m:temperature=55.5 m:speed=120.0";
        Assert.assertEquals(expected, commands.get(0));
    }

    @Test
    public void testConverUpdateToSeries() throws SQLException {
        AtsdSqlUpdateConverter converter = (AtsdSqlUpdateConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.UPDATE, false);
        String sql = "update test.temperature set datetime='2017-06-21T00:00:00Z', value=24.5, tags.unit='celcius' where entity='sensor-1'";
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        String expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=celcius m:test.temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));

        sql = "update temperature set datetime='2017-06-21T00:00:00Z', value=24.5, tags.unit='celcius', text='test' where entity='sensor-1' AND tags" +
                ".unit2='celc'";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=celcius t:unit2=celc m:temperature=24.5 x:temperature=test";
        Assert.assertEquals(expected, commands.get(0));

        sql = "update temperature set datetime=?, value=?, tags.unit=? where entity='sensor-1'";
        commands = converter.convertToCommands(sql, Arrays.<Object>asList("2017-06-21T00:00:00Z", 24.5, "celcius"));
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=celcius m:temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));

        sql = "update temperature set datetime=?, value=?, tags.unit=? where entity='sensor-1'";
        List<List<Object>> valuesBatch = new ArrayList<>();
        valuesBatch.add(Arrays.<Object>asList("2017-06-21T00:00:00Z", 24.5, "celcius1"));
        valuesBatch.add(Arrays.<Object>asList("2017-06-22T00:00:00Z", 25.5, "celcius2"));
        valuesBatch.add(Arrays.<Object>asList("2017-06-23T00:00:00Z", 26.5, "celcius3"));
        commands = converter.convertBatchToCommands(sql, valuesBatch);
        Assert.assertEquals(3, commands.size());
        expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=celcius1 m:temperature=24.5\n" +
                "series e:sensor-1 d:2017-06-22T00:00:00Z t:unit=celcius2 m:temperature=25.5\n" +
                "series e:sensor-1 d:2017-06-23T00:00:00Z t:unit=celcius3 m:temperature=26.5";
        Assert.assertEquals(expected, StringUtils.join(commands, '\n'));

        sql = "update atsd_series set datetime='2017-06-21T00:00:00Z', value=24.5, text='test', tags.unit='celcius' where entity='sensor-1' and " +
                "metric='temperature'";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=celcius m:temperature=24.5 x:temperature=test";
        Assert.assertEquals(expected, commands.get(0));

        sql = "update atsd_series set time=123456789, value=24.5, text='test', tags.unit='celcius' where entity='sensor-1' and " +
                "metric='temperature'";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-1 ms:123456789 t:unit=celcius m:temperature=24.5 x:temperature=test";
        Assert.assertEquals(expected, commands.get(0));

        sql = "update atsd_series set time=123456789, value=24.5, tags.unit='celcius' where entity='sensor-1' and " +
                "metric='temperature' and text is null";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        expected = "series e:sensor-1 ms:123456789 t:unit=celcius m:temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));

    }

    @Test
    public void testConverUpdateToSeriesWithLikeComparison() throws SQLException {
        AtsdSqlUpdateConverter converter = (AtsdSqlUpdateConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.UPDATE, false);
        String sql = "update atsd_series set time=123456789, value=24.5, tags.unit='celcius' where entity='sensor-1' and " +
                "metric like 'jvm_memory_free'";
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        String expected = "series e:sensor-1 ms:123456789 t:unit=celcius m:jvm_memory_free=24.5";
        Assert.assertEquals(expected, commands.get(0));

        sql = "update atsd_series set time=123456789, value=24.5, tags.unit='celcius' where entity='sensor-1' and " +
                "metric like 'jvm#_memory#_free' escape '#'";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        Assert.assertEquals(expected, commands.get(0));

        sql = "update atsd_series set time=123456789, value=24.5, tags.unit='celcius' where entity='sensor-1' and " +
                "metric like ? escape '#'";
        commands = converter.convertToCommands(sql, Arrays.<Object>asList("jvm#_memory#_free"));
        Assert.assertEquals(1, commands.size());
        Assert.assertEquals(expected, commands.get(0));
    }

    @Test
    public void testConvertInsertToSeriesWithEscapedTableName() throws SQLException {
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, false);
        String sql = "INSERT INTO 'test.temperature' (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 24.5, null, " +
                "'Celcius')";

        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        String expected = "series e:sensor-01 d:2017-06-21T00:00:00Z t:unit=Celcius m:test.temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));
    }

    @Test
    public void testConvertUpdateToSeriesWithEscapedTableName() throws SQLException {
        AtsdSqlUpdateConverter converter = (AtsdSqlUpdateConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.UPDATE, false);
        String sql = "update 'test.temperature' set datetime='2017-06-21T00:00:00Z', value=24.5, tags.unit='celcius' where entity='sensor-1'";
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        String expected = "series e:sensor-1 d:2017-06-21T00:00:00Z t:unit=celcius m:test.temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));
    }

    @Test
    public void testConvertInsertToSeriesWithTimestamp() throws SQLException, ParseException {
        final String sql = "INSERT INTO 'test.temperature' (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-07-12 04:05:00.34567', " +
                "24.5, null, 'Celcius')";

        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        String expected = "series e:sensor-01 d:2017-07-12T04:05:00.345Z t:unit=Celcius m:test.temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));

        converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, false);
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(1, commands.size());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        Date date = sdf.parse("2017-07-12T04:05:00.345Z");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        expected = "series e:sensor-01 d:" + sdf.format(date) + " t:unit=Celcius m:test.temperature=24.5";
        Assert.assertEquals(expected, commands.get(0));
    }

    @Test
    public void testConvertInsertToCommands() throws SQLException {
        String sql = "INSERT INTO 'm-local-1' (entity, value, datetime, tags.test1, metric.tags.test1, entity.tags.test1) VALUES " +
                "('e-local-1', 123.0, '2017-07-12T08:05:02Z', 'S1', 'M1', 'E1')";

        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(3, commands.size());
        String expectedSeries = "series e:e-local-1 d:2017-07-12T08:05:02Z t:test1=S1 m:m-local-1=123.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        String expectedEntity = "entity e:e-local-1 t:test1=E1";
        Assert.assertEquals(expectedEntity, commands.get(1));
        String expectedMetric = "metric m:m-local-1 t:test1=M1";
        Assert.assertEquals(expectedMetric, commands.get(2));

        sql = "INSERT INTO 'm-local-1' (entity, value, datetime, tags, metric.tags, entity.tags) VALUES " +
                "('e-local-1', 123.0, '2017-07-12T08:05:02Z', 'test1=S1', 'test1=M1', 'test1=E1')";

        commands = converter.convertToCommands(sql);
        System.out.println(commands);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(expectedSeries, commands.get(0));
        Assert.assertEquals(expectedEntity, commands.get(1));
        Assert.assertEquals(expectedMetric, commands.get(2));

        sql = "INSERT INTO 'm-local-1' (entity, value, datetime, tags, metric.tags, entity.tags) VALUES " +
                "('e-local-1', 123.0, '2017-07-12T08:05:02Z', 'test1=S1;test2=S2', 'test1=M1;test2=M2', 'test1=E1;test2=E2')";

        commands = converter.convertToCommands(sql);
        Assert.assertEquals(3, commands.size());
        expectedSeries = "series e:e-local-1 d:2017-07-12T08:05:02Z t:test1=S1 t:test2=S2 m:m-local-1=123.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        expectedEntity = "entity e:e-local-1 t:test1=E1 t:test2=E2";
        Assert.assertEquals(expectedEntity, commands.get(1));
        expectedMetric = "metric m:m-local-1 t:test1=M1 t:test2=M2";
        Assert.assertEquals(expectedMetric, commands.get(2));

        sql = "INSERT INTO atsd_series (entity, time, text, tags.location, value, metric, speed, entity.label, entity.interpolate, entity.timeZone" +
                ", entity.enabled, entity.tags) VALUES ('sensor-01', 123456789, null, 'Vienna', 55.5, 'temperature', 120, 'label1', 'linear', 'UTC'" +
                ", false, 'test=t2')";
        commands = converter.convertToCommands(sql);
        Assert.assertEquals(2, commands.size());
        expectedSeries = "series e:sensor-01 ms:123456789 t:location=Vienna m:temperature=55.5 m:speed=120.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        expectedEntity = "entity " + "e:sensor-01 b:false l:label1 i:linear z:UTC t:test=t2";
        Assert.assertEquals(expectedEntity, commands.get(1));
    }

    @Test
    public void testConvertUpdateToCommands() throws SQLException {
        String sql = "update 'm-local-1' set datetime='2017-07-12T08:05:02Z', value=123.0, tags.test1='S1'" +
                ", metric.tags.test1='M1', entity.tags.test1='E1' where entity='e-local-1'";

        AtsdSqlUpdateConverter converter = (AtsdSqlUpdateConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.UPDATE, false);
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(3, commands.size());
        String expectedSeries = "series e:e-local-1 d:2017-07-12T08:05:02Z t:test1=S1 m:m-local-1=123.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        String expectedEntity = "entity e:e-local-1 t:test1=E1";
        Assert.assertEquals(expectedEntity, commands.get(1));
        String expectedMetric = "metric m:m-local-1 t:test1=M1";
        Assert.assertEquals(expectedMetric, commands.get(2));

        sql = "update 'm-local-1' set datetime='2017-07-12T08:05:02Z', value=123.0, tags='test1=S1'" +
                ", metric.tags='test1=M1', entity.tags='test1=E1' where entity='e-local-1'";

        commands = converter.convertToCommands(sql);
        System.out.println(commands);
        Assert.assertEquals(3, commands.size());
        Assert.assertEquals(expectedSeries, commands.get(0));
        Assert.assertEquals(expectedEntity, commands.get(1));
        Assert.assertEquals(expectedMetric, commands.get(2));

        sql = "update 'm-local-1' set datetime='2017-07-12T08:05:02Z', value=123.0, tags='test1=S1;test2=S2'" +
                ", metric.tags='test1=M1;test2=M2', entity.tags='test1=E1;test2=E2' where entity='e-local-1'";

        commands = converter.convertToCommands(sql);
        Assert.assertEquals(3, commands.size());
        expectedSeries = "series e:e-local-1 d:2017-07-12T08:05:02Z t:test1=S1 t:test2=S2 m:m-local-1=123.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        expectedEntity = "entity e:e-local-1 t:test1=E1 t:test2=E2";
        Assert.assertEquals(expectedEntity, commands.get(1));
        expectedMetric = "metric m:m-local-1 t:test1=M1 t:test2=M2";
        Assert.assertEquals(expectedMetric, commands.get(2));
    }

    @Test
    public void testConvertInsertWithEntityGroups() throws SQLException {
        expectedException.expect(SQLFeatureNotSupportedException.class);
        expectedException.expectMessage("entity.groups");
        String sql = "INSERT INTO atsd_series (entity, time, text, tags.location, value, metric, speed, entity.label, entity.groups) " +
                "VALUES ('sensor-01', 123456789, null, 'Vienna', 55.5, 'temperature', 120, 'label1', 'group1')";
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        converter.convertToCommands(sql);
    }

    @Test
    public void testConvertInsertWithIllegalEntityInterpolation() throws SQLException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal entity interpolation: test");
        String sql = "INSERT INTO atsd_series (entity, time, text, tags.location, value, metric, speed, entity.label, entity.interpolate) " +
                "VALUES ('sensor-01', 123456789, null, 'Vienna', 55.5, 'temperature', 120, 'label1', 'test')";
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        converter.convertToCommands(sql);
    }

    @Test
    public void testConvertInsertWithIllegalMetricInterpolation() throws SQLException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal metric interpolation: test");
        String sql = "INSERT INTO atsd_series (entity, time, text, tags.location, value, metric, speed, metric.label, metric.interpolate) " +
                "VALUES ('sensor-01', 123456789, null, 'Vienna', 55.5, 'temperature', 120, 'label1', 'test')";
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        converter.convertToCommands(sql);
    }

    @Test
    public void testConvertInsertWithIllegalMetricDataType() throws SQLException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal metric data type: bigint");
        String sql = "INSERT INTO atsd_series (entity, time, text, tags.location, value, metric, speed, metric.label, metric.dataType) " +
                "VALUES ('sensor-01', 123456789, null, 'Vienna', 55.5, 'temperature', 120, 'label1', 'bigint')";
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        converter.convertToCommands(sql);
    }

    @Test
    public void testConvertInsertWithIllegalMetricActionType() throws SQLException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal metric action: test");
        String sql = "INSERT INTO atsd_series (entity, time, text, tags.location, value, metric, speed, metric.label, metric.invalidValueAction) " +
                "VALUES ('sensor-01', 123456789, null, 'Vienna', 55.5, 'temperature', 120, 'label1', 'test')";
        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        converter.convertToCommands(sql);
    }

    @Test
    public void testConvertInsertToSeriesAndEntityCommands() throws SQLException {
        String sql = "INSERT INTO 'm-local-1' (entity, value, datetime, tags.test1, entity.tags.test1, entity.label, entity.enabled, entity.interpolate" +
                ", entity.timeZone) VALUES ('e-local-1', 123.0, '2017-07-12T08:05:02Z', 'S1', 'E1', 'label1', true, 'linear', 'UTC')";

        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(2, commands.size());
        String expectedSeries = "series e:e-local-1 d:2017-07-12T08:05:02Z t:test1=S1 m:m-local-1=123.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        String expectedEntity = "entity e:e-local-1 b:true l:label1 i:linear z:UTC t:test1=E1";
        Assert.assertEquals(expectedEntity, commands.get(1));
    }

    @Test
    public void testConvertUpdateToSeriesAndEntityCommands() throws SQLException {
        String sql = "UPDATE 'm-local-1' SET value=123, datetime='2017-07-12T08:05:02Z', tags.test1='S1', entity.tags.test1='E1', entity.label='label1'" +
                ", entity.enabled=true, entity.interpolate='linear', entity.timeZone='UTC' WHERE entity='e-local-1'";

        AtsdSqlUpdateConverter converter = (AtsdSqlUpdateConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.UPDATE, true);
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(2, commands.size());
        String expectedSeries = "series e:e-local-1 d:2017-07-12T08:05:02Z t:test1=S1 m:m-local-1=123.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        String expectedEntity = "entity e:e-local-1 b:true l:label1 i:linear z:UTC t:test1=E1";
        Assert.assertEquals(expectedEntity, commands.get(1));
    }

    @Test
    public void testConvertInsertToSeriesAndMetricCommands() throws SQLException {
        String sql = "INSERT INTO 'm-local-1' (entity, value, datetime, tags.test1, metric.tags.test1, metric.label, metric.enabled, metric.interpolate" +
                ", metric.timeZone, metric.description, metric.filter, metric.versioning, metric.units, metric.invalidValueAction, metric.maxValue" +
                ", metric.minValue) VALUES ('e-local-1', 123.0, '2017-07-12T08:05:02Z', 'S1', 'M1', 'label1', true, 'linear', 'UTC', 'description 1'" +
                ", 'filter 1', false, 'unit1', 'none', 12345, 123)";

        AtsdSqlInsertConverter converter = (AtsdSqlInsertConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.INSERT, true);
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(2, commands.size());
        String expectedSeries = "series e:e-local-1 d:2017-07-12T08:05:02Z t:test1=S1 m:m-local-1=123.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        String expectedMetric = "metric m:m-local-1 b:true l:label1 d:\"description 1\" i:linear u:unit1 f:\"filter 1\" z:UTC v:false a:none min:123.0 " +
                "max:12345.0 t:test1=M1";
        Assert.assertEquals(expectedMetric, commands.get(1));
    }

    @Test
    public void testConvertUpdateToSeriesAndMetricCommands() throws SQLException {
        String sql = "UPDATE 'm-local-1' SET value=123, datetime='2017-07-12T08:05:02Z', tags.test1='S1', metric.tags.test1='M1', metric.label='label1'" +
                ", metric.enabled=true, metric.interpolate='linear', metric.timeZone='UTC', metric.description='description 1', metric.filter='filter 1'" +
                ", metric.versioning=false, metric.units='unit1', metric.invalidValueAction='none', metric.maxValue=12345, metric.minValue=123" +
                " WHERE entity='e-local-1'";

        AtsdSqlUpdateConverter converter = (AtsdSqlUpdateConverter) AtsdSqlConverterFactory.getConverter(Meta.StatementType.UPDATE, true);
        List<String> commands = converter.convertToCommands(sql);
        Assert.assertEquals(2, commands.size());
        String expectedSeries = "series e:e-local-1 d:2017-07-12T08:05:02Z t:test1=S1 m:m-local-1=123.0";
        Assert.assertEquals(expectedSeries, commands.get(0));
        String expectedMetric = "metric m:m-local-1 b:true l:label1 d:\"description 1\" i:linear u:unit1 f:\"filter 1\" z:UTC v:false a:none min:123.0 " +
                "max:12345.0 t:test1=M1";
        Assert.assertEquals(expectedMetric, commands.get(1));
    }

}
