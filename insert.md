# ATSD JDBC Driver Data Modification Capabilities

## Overview

ATSD doesn't support INSERT and UPDATE statements, but the driver does. Such queries are parsed by the driver and translated to a set of ATSD [network commands](https://github.com/axibase/atsd/tree/master/api/network#network-api)
You are allowed to use only predefined columns and types in INSERT and UPDATE statements.
The following fields are supported:

Field | Datatype | Comments
------|----------|-----------
datetime | timestamp | time or datetime required
entity | varchar | required
metric | varchar | required
text | varchar | value and/or text required
time | bigint | time or datetime required
value | decimal | value and/or text required
tags | varchar | Serialized as `key=value` pairs separated by a semicolon. To set a single tag, use the syntax `tags.key = value`
metric.dataType | varchar |
metric.description | varchar |
metric.enabled | boolean |
metric.filter | varchar |
metric.interpolate | varchar |
metric.invalidValueAction | varchar |
metric.label | varchar |
metric.lastInsertTime | varchar |
metric.maxValue | float |
metric.minValue | float |
metric.name | varchar |
metric.persistent | boolean |
metric.retentionIntervalDays | integer |
metric.tags | varchar | Serialized as `key=value` pairs separated by a semicolon. To set a single tag, use the syntax `metric.tags.key = value`
metric.timePrecision | varchar |
metric.timeZone | varchar |
metric.versioning | boolean |
metric.units | varchar |
entity.enabled | boolean |
entity.label | varchar |
entity.interpolate | varchar |
entity.tags | varchar | Serialized as `key=value` pairs separated by a semicolon. To set a single tag, use the syntax `entity.tags.key = value`
entity.timeZone | varchar |

If a field is set to null, it will be ignored while generating ATSD network commands.

## Syntax

### INSERT

The following syntaxes are supported:

```sql
INSERT INTO {metric name} (entity, [datetime | time], [value | text][, {other fields}]) VALUES ({comma-separated values})
```

```sql
INSERT INTO atsd_series (entity, [datetime | time], {metric1 name}[, {metric2 name} [, {metric names}]] [, {other fields}]) VALUES ({comma-separated})
```

Comparison table

Feature | INSERT INTO metric_name | INSERT INTO atsd_series
--------|-------------------------|-------------------------
Number of generated queries | 1 series, 0 or 1 metric, 1 or 1 entity | 1 series command with multiple metrics
Supported metric fields | yes | no
Supported entity fields | yes | no
Text column | text | not supported
Value column | value | {metric_name}

Examples:

```sql
INSERT INTO temperature (entity, datetime, value, text, tags.unit)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 24.5, null, 'Celcius')
```
will be translated to 
```css
series e:sensor-01 d:2017-06-21T00:00:00Z m:temperature=24.5 t:unit=Celcius
```

```sql
INSERT INTO atsd_series (entity, datetime, text, tags.location, temperature, speed)  VALUES ('sensor-01', '2017-06-21T00:00:00Z', 24.5, null, 'Vienna', 55.5, 120)
```
will be translated to 
```css
series e:sensor-01 d:2017-06-21T00:00:00Z m:temperature=55.5 m:speed=120 t:location=Vienna
```

### UPDATE

Update statements with constant expressions only supported. Using table alias in UPDATE command is not allowed.
The following syntaxes are supported:

```sql
UPDATE "{metric name}" SET value = {numeric value} [, text = '{string value}' [, {metric field} = {metric field value}]] WHERE entity = {entity name} AND [time = {millis} | datetime = '{iso8601 time}']
```

The LIKE expression with ESCAPE is supported if it can be substituted with a constant comparison:

```sql
WHERE metric LIKE "jvm#_memory#_used" ESCAPE '#'
```

Example:

```sql
UPDATE "user-dirs-size" SET value = 81812238 WHERE entity = 'axibase-dev' AND tags.directory = '/Users/axibase/.docker' AND datetime = '2017-08-22T15:09:04.000Z'
```

### Quotes

Since ATSD revision 17082 usage of single and double quotes functionality is strictly divided. Single quotes must be used only in string literals, and double quotes are to be used in table aliases and table or column name escaping.

```sql
INSERT INTO "escaped-metric" (entity, datetime, value) VALUES ('test-entity', '2017-08-22T14:50:00.000Z', 0)"
```

### Time Zone

INSERT and UPDATE statements allow to specify series datetime as string. Two formats are supported:
* `yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z'` (ISO 8601 with or without milliseconds. Only UTC timestamps are allowed)
* `yyyy-MM-dd HH:mm:ss[.fffffffff]` (Fraction of seconds will be rounded to 3 digits (milliseconds))

If the last format is used, time zone used to parse the timestamp depends on the `timestamptz` connection string parameter. 
If timestamptz=true (by default), timestamp is understood to be in UTC time zone. If timestamptz=false, local time zone is used.

If the datetime is set in prepared statement using `PreparedStatement#setTimestamp` method, the result will be different. 
If timestamptz=true (by default), `Timestamp.getTime()` call returns the number of milliseconds since 1970-01-01T00:00:00.000Z. 
If timestamptz=false, `Timestamp.getTime()` call is determined to return the number of milliseconds since 1970-01-01T00:00:00 in local time zone.

Here is the comparison table with results of inserting timestamps with different methods and different timestamptz values.
The time zone during the insert operation was GMT+2. See the used code [here](https://github.com/axibase/atsd-jdbc-test/blob/master/src/test/java/com/axibase/tsd/driver/jdbc/examples/TimeZoneInsertExample.java)

```sql
SELECT datetime, tags.* FROM "m-insert-dt"
```

datetime | tags.setter | tags.timestamptz
---------|-------------|--------------------
2017-08-21T22:00:00.000Z | setString | false
2017-08-22T00:00:00.000Z | setString | true
2017-08-22T00:00:00.000Z | setTimestamp | true
2017-08-22T02:00:00.000Z | setTimestamp | false

## Batch

Batch queries aim to improve insert performance by reusing statement objects and network connections.

```java
final String entityName = "test-entity";
final String metricName = "test-metric";
final String simplePattern = "INSERT INTO \"{}\" (time, entity, value, entity.tags, metric.tags) VALUES ({},'{}',{},{},{})";
final String atsdSeriesPattern = "INSERT INTO atsd_series (metric, time, entity, value, entity.tags, metric.tags) VALUES ('{}',{},'{}',{},{},{})";
try (Statement stmt = connection.createStatement()) {
    int i = 1;
    stmt.addBatch(format(simplePattern, metricName, i * 1000, entityName, i++, null, "'test1=value1'"));
    stmt.addBatch(format(simplePattern, metricName, i * 1000, entityName, i++, "'test1=value1'", null));
    stmt.addBatch(format(atsdSeriesPattern, metricName, i * 1000, entityName, i++, null, null));
    stmt.addBatch(format(atsdSeriesPattern, metricName, i * 1000, entityName, i++, "'test1=value1'", "'test1=value1'"));
    int[] res = stmt.executeBatch();
    Assert.assertArrayEquals(new int[] {2,2,1,3}, res);
}
```

The following commands will be generated and sent to ATSD:
```css
series e:test-entity ms:1000 m:test-metric=1.0
metric m:test-metric t:test1=value1

series e:test-entity ms:2000 m:test-metric=2.0
entity e:test-entity t:test1=value1

series e:test-entity ms:3000 m:test-metric=3.0

series e:test-entity ms:4000 m:test-metric=4.0
entity e:test-entity t:test1=value1
metric m:test-metric t:test1=value1
```

Additional examples:

* [Collect Directories Size in User's Home Directory](https://github.com/axibase/atsd-jdbc-test/blob/master/src/test/java/com/axibase/tsd/driver/jdbc/examples/BatchExample.java)

## Transactions

ATSD implements eventual consistency model. ACID transactions are not supported.