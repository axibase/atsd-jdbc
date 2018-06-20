# Inserting Data

## Overview

The ATSD JDBC driver provides support for writing data into ATSD using `INSERT` and `UPDATE` statements. These statements are parsed by the driver into [network commands](https://github.com/axibase/atsd/tree/master/api/network#network-api) which are inserted into the database with the [Data API `command`](https://github.com/axibase/atsd/blob/master/api/data/ext/command.md) method.

```sql
INSERT INTO temperature (entity, datetime, value, tags.surface)
                 VALUES ('sensor-01', '2017-08-21T00:00:00Z', 24.5, 'Outer')
```

The above query is translated into a [series command](https://github.com/axibase/atsd/blob/master/api/network/series.md) sent into the database:

```ls
series e:sensor-01 d:2017-08-21T00:00:00Z m:temperature=24.5 t:surface=Outer
```

The same command can be produced with the following, equivalent `UPDATE` statements:

```sql
UPDATE temperature SET value = 24.5 WHERE entity = 'sensor-01' AND datetime = '2017-08-21T00:00:00Z' AND tags.surface = 'Outer'
UPDATE temperature SET datetime = '2017-08-21T00:00:00Z', value = 24.5  WHERE entity = 'sensor-01' AND tags.surface = 'Outer'
```

## UPDATE Statement

The `UPDATE` statement is processed similarly to the `INSERT` statement in that it is converted into one `series` command and optional `metric` and `entity` commands.

The `series` command contains all columns referenced in the `SET` and `WHERE` conditions. The `WHERE` clause must contain at least one column.

There are no checks for the existence of records as part of `UPDATE` statement processing.

A new value is inserted if the record identified by the series key and time does not exist. Otherwise, an existing record is updated with the new value.

This type of `UPDATE` query is often referred to as `UPSERT` or `MERGE`.

## Tables

Because new metrics are automatically registered by the database, there is no need to create metrics ahead of time to insert records into a new table.

```sql
/*  
  The database will automatically create new metric 'mtr-1', if necessary.
*/
INSERT INTO "mtr-1" (entity, datetime, value)
             VALUES ('ent-1', '2017-08-21T00:00:00Z', 100)
```

## Requirements

The `INSERT` and `UPDATE` statements can reference only the following predefined columns.

### Series Columns

**Column** | **Datatype** | **Comments**
------|------|------
`entity`         | `varchar` | Required
`metric`         | `varchar` | Required (1)
`datetime`       | `timestamp` | Required (2)
`time`           | `bigint` | Required (2)
`text`           | `varchar` | Required (3)
`value`          | `decimal` | Required (3)
`tags.{name}`    | `varchar` | Series tag with name `{name}`
`tags`           | `varchar` | Multiple series tags (4)

### Metric Metadata Columns

**Column** | **Datatype** | **Comments**
------|------|------
`metric.dataType` | `varchar` |
`metric.description` | `varchar` |
`metric.enabled` | `boolean` |
`metric.filter` | `varchar` |
`metric.interpolate` | `varchar` |
`metric.invalidValueAction` | `varchar` |
`metric.label` | `varchar` |
`metric.lastInsertTime` | `varchar` |
`metric.maxValue` | `float` |
`metric.minValue` | `float` |
`metric.name` | `varchar` |
`metric.persistent` | `boolean` |
`metric.retentionIntervalDays` | `integer` |
`metric.tags.{name}` | `varchar` | Metric tag with name `{name}`
`metric.tags` | `varchar` | Multiple metric tags (4)
`metric.timePrecision` | `varchar` |
`metric.timeZone` | `varchar` |
`metric.versioning` | `boolean` |

> Refer to metric field [details](https://github.com/axibase/atsd/blob/master/api/meta/metric/list.md#fields).

### Entity Metadata Columns

**Column** | **Datatype** | **Comments**
------|------|------
`metric.units` | `varchar` |
`entity.enabled` | `boolean` |
`entity.label` | `varchar` |
`entity.interpolate` | `varchar` |
`entity.tags.{name}` | `varchar` | Entity tag with name `{name}`
`entity.tags` | `varchar` | Multiple entity tags (4)
`entity.timeZone` | `varchar` |

> Refer to entity field [details](https://github.com/axibase/atsd/blob/master/api/meta/entity/list.md#fields).

**Notes**:

* (1) The required metric name is specified as the table name.
* (2) Either the `time` or `datetime` column must be included in the statement.
* (3) Either the `text` or `value` column must be included in the statement.
* (4) The `tags` column can contain multiple series, metric, and entity tags serialized as `key1=value1` pairs separated by a semicolon.

When inserting records into the reserved `atsd_series` table, all numeric columns other then predefined columns are classified as **metric** columns, with column name equal to metric name and column value equal to series value. At least one numeric metric column is required.

## INSERT Syntax

`INSERT` statement provides the following syntax options:

### Insert into `metric`

```sql
INSERT INTO "{metric-name}" (entity, [datetime | time], [value | text][, {other fields}])
       VALUES ({comma-separated values})
```

Example:

```sql
INSERT INTO "temperature"
         (entity, datetime, value, tags.surface)
  VALUES ('sensor-01', '2017-08-21T00:00:00Z', 24.5, 'Outer')
```

```ls
series e:sensor-01 d:2017-08-21T00:00:00Z m:temperature=24.5 t:surface=Outer
```

Example with metadata:

```sql
INSERT INTO "temperature"
         (entity, datetime, value, tags.surface, metric.units, entity.tags.location)
  VALUES ('sensor-01', '2017-08-21T00:00:00Z', 24.5, 'Outer', 'Celsius', 'SVL')
```

```ls
series e:sensor-01 d:2017-08-21T00:00:00Z m:temperature=24.5 t:surface=Outer
metric m:temperature u:Censius
entity e:sensor-01 t:location=SVL
```

### Insert into `atsd_series`

```sql
INSERT INTO "atsd_series"
         (entity, [datetime | time], {metric1 name}[, {metric2 name} [, {metric names}]] [, {other fields}])
  VALUES ({comma-separated})
```

Example:

```sql
INSERT INTO "atsd_series"
         (entity, datetime, tags.surface, temperature, humidity)
  VALUES ('sensor-01', '2017-08-21T00:00:00Z', 'Outer', 24.5, 68)
```

```ls
series e:sensor-01 d:2017-08-21T00:00:00Z t:surface=Outer m:temperature=55.5 m:humidity=68
```

### INSERT Syntax Comparison

Feature | Insert into `metric` | Insert into `atsd_series`
--------|-------------------------|-------------------------
Metric metadata columns | Allowed | Not allowed
Entity metadata columns | Allowed | Not allowed
Text column | text | Not supported
Value column | value | {metric_name}
Generated commands | 1 series command<br>1 optional metric command<br>1 optional entity command | 1 series command<br>With multiple metrics

## UPDATE Syntax

```sql
UPDATE "{metric name}" SET value = {numeric value} [, text = '{string value}' [, {metric field} = {metric field value}]] WHERE entity = {entity name} AND [time = {millis} | datetime = '{iso8601 time}']
```

> The columns can be specified in the `SET` clause or the `WHERE` clause with equal results. The `WHERE` clause must contain at least one column.

Example:

```sql
UPDATE "temperature" SET value = 25.5 WHERE entity = 'sensor-01' AND tags.surface = 'Outer' AND datetime = '2017-08-21T00:00:00Z'
-- equivalent syntax
UPDATE "temperature" SET value = 25.5, datetime = '2017-08-21T00:00:00Z' WHERE entity = 'sensor-01' AND tags.surface = 'Outer'
```

The `WHERE` condition in `UPDATE` statements supports the following comparators and operators:

* `=` (equal) comparator.
* `LIKE` comparator with optional `ESCAPE` clause if it can be substituted with an `=` (equal) comparator.
* Logical operator `AND` (`OR` operator is not supported).

```sql
WHERE entity = 'sensor-1'
WHERE entity LIKE 'sensor-1'
/* Underscore in sensor_2 below must be escaped
   because underscore and percentage signs are SQL wildcards */
WHERE entity LIKE 'sensor#_2' ESCAPE '#'
```

### Quotes

* Table and column names containing special characters or database identifiers must be enclosed in double quotes.
* String and date literals must be enclosed in single quotes.

### NULL values

Column values set to `null` or empty string are not included into the generated network commands.

### Date

Literal date values can be specified using the following formats:

* ISO 8601  `yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z'`

    ISO 8601 with optional milliseconds. Only UTC time zone 'Z' is allowed.

* Local Time `yyyy-MM-dd HH:mm:ss[.fffffffff]`

    Fractional second are rounded to milliseconds (3 digits).

### Time Zone

#### Time Zone in `Statement` Queries

Parsing `datetime` column values specified in the local time format, e.g. '2017-08-15 16:00:00', depends on the value of the `timestamptz` connection string parameter.

* If `timestamptz` is set to `true` (default)

    The local timestamp is converted to a date object using UTC time zone.

* If `timestamptz` is set to `false`

    The local timestamp is converted to a date object using the time zone on the client which is based on system time or `-Duser.timeZone` parameter.

#### Time Zone in `PreparedStatement` Queries

The results of setting `datetime` column value using `PreparedStatement#setTimestamp` method depend on the value of the `timestamptz` connection string parameter.

* If `timestamptz` is set to `true` (default)

    The `Timestamp.getTime()` method returns the number of milliseconds since 1970-Jan-01 00:00:00 UTC.

* If `timestamptz` is set to `false`

    The `Timestamp.getTime()` method returns the number of milliseconds since 1970-Jan-01 00:00:00 in **local** time zone.

```java
    final String timeZone = "Europe/Berlin";
    final String stringTime = "2017-08-15 00:00:00";
    final DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of(timeZone));
    final long millis = ZonedDateTime.parse(stringTime, formatter).toInstant().toEpochMilli();
    final String query = "INSERT INTO temperature (entity, datetime, value) VALUES (?, ?, 24.5)";

    // timestamptz=true (default value)
    try (final PreparedStatement stmt = tzTrueConnection.prepareStatement(query)) {

        stmt.setString(1, "sensor-01");
        stmt.setString(2, stringTime);
        stmt.executeUpdate(); // stored as 2017-08-15T00:00:00Z (utc) - 2017-08-15 02:00:00 (local)

        stmt.setString(1, "sensor-02");
        stmt.setTimestamp(2, new Timestamp(millis));
        stmt.executeUpdate(); // stored as 2017-08-14T22:00:00Z (utc) - 2017-08-15 00:00:00 (local)

        stmt.setString(1, "sensor-03");
        stmt.setLong(2, millis);
        stmt.executeUpdate(); // stored as 2017-08-14T22:00:00Z (utc) - 2017-08-15 00:00:00 (local)
    }

    // timestamptz=false
    try (final PreparedStatement stmt = tzFalseConnection.prepareStatement(query)) {

        stmt.setString(1, "sensor-04");
        stmt.setString(2, stringTime);
        stmt.executeUpdate(); // stored as 2017-08-14T22:00:00Z (utc) - 2017-08-15 00:00:00 (local)

        stmt.setString(1, "sensor-05");
        stmt.setTimestamp(2, new Timestamp(millis));
        stmt.executeUpdate(); // stored as 2017-08-15T00:00:00Z (utc) - 2017-08-15 02:00:00 (local)

        stmt.setString(1, "sensor-06");
        stmt.setLong(2, millis);
        stmt.executeUpdate(); // stored as 2017-08-14T22:00:00Z (utc) - 2017-08-15 00:00:00 (local)
    }
```

## Parameterized Queries

Parameterized queries improve parsing performance and ensure correct mappings between column data types and parameter values.

A question mark (?) without quotes is used as a parameter placeholder. Question marks inside string literals and object identifiers are treated as regular characters.

```java
    String sensorId = "sensor-01";
    long sampleTime = System.currentTimeMillis();
    String tagString = "surface=Outer;status=Initial";
    double value = 24.5;

    String insertQuery = "INSERT INTO temperature (entity, tags, time, value) VALUES (?, ?, ?, ?)";
    PreparedStatement statement = connection.prepareStatement(insertQuery);
    statement.setString(1, sensorId);
    statement.setString(2, tagString);
    statement.setLong(3, sampleTime);
    statement.setDouble(4, value);

    statement.execute();
```

To set multiple tags as map, cast the `PreparedStatement` to `AtsdPreparedStatement`.

```java
    String sensorId = "sensor-01";
    String timeStringUtc = "2017-08-20 08:30";
    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(ZoneId.of("UTC"));
    final Timestamp ts = Timestamp.from(ZonedDateTime.parse(stringTime, formatter).toInstant());
    Map<String, String> seriesTags = new HashMap<String, String>();
    seriesTags.put("surface", "Outer");
    seriesTags.put("status", "Initial");
    double value = 24.5;

    String insertQuery = "INSERT INTO temperature (entity, tags, datetime, value) VALUES (?, ?, ?, ?)";
    PreparedStatement statement = connection.prepareStatement(insertQuery);
    AtsdPreparedStatement atsdStatement = (AtsdPreparedStatement)statement;
    statement.setString(1, sensorId);
    atsdStatement.setTags(2, seriesTags);
    statement.setTimestamp(3, ts);
    statement.setDouble(4, value);

    statement.execute();
```

## Batch Inserts

Batch queries improve insert performance by sending commands in batches.

```java
    int maxBatchSize = 50;
    String sensorId = "sensor-01";
    String tagString = "surface=Outer";
    String insertQuery = "INSERT INTO temperature (entity, tags, time, value) VALUES (?, ?, ?, ?)";
    long sampleTime = System.currentTimeMillis() - 60000 * 60;
    PreparedStatement statement = connection.prepareStatement(insertQuery);
    int batchSize = 0;
    while (sampleTime < System.currentTimeMillis()) {
        statement.setString(1, sensorId);
        statement.setString(2, tagString);
        statement.setLong(3, sampleTime);
        statement.setLong(4, 20 + (long)(10 * Math.random()));
        statement.addBatch();
        sampleTime += 60000;
        batchSize++;
        if (batchSize >= maxBatchSize) {
            int[] results = statement.executeBatch();
            System.out.println("Inserted batch: " + Arrays.toString(results));
            batchSize = 0;
            statement.clearBatch();
        }
    }

    if (batchSize > 0) {
        int[] results = statement.executeBatch();
        System.out.println("Inserted last batch: " + Arrays.toString(results));
    }
```

## Transactions

Transactions are not supported.
