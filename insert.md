# Inserting Data

## Overview

The ATSD JDBC driver provides support for writing data into ATSD using `INSERT` and `UPDATE` statements. The statements are parsed by the driver into [network commands](https://github.com/axibase/atsd/tree/master/api/network#network-api) which are uploaded into the database with the [Data API `command`](https://github.com/axibase/atsd/blob/master/api/data/ext/command.md) method.

```sql
INSERT INTO temperature (entity, datetime, value, tags.surface)
                 VALUES ('sensor-01', '2017-08-21T00:00:00Z', 24.5, 'Outer')
```

The above query is translated into a [series command](https://github.com/axibase/atsd/blob/master/api/network/series.md) sent into the database:

```ls
series e:sensor-01 d:2017-08-21T00:00:00Z m:temperature=24.5 t:surface=Outer
```

## Insert vs Update

`INSERT` and `UPDATE` statements are converted into the same commands which are processed as follows:

* If the record doesn't exist, new value is inserted.

* If the record exists, the old value is automatically updated.

This type of query is sometimes referred to as `UPSERT` or `MERGE`.

There are no checks for the existence of records as part of the `UPDATE` statement.

Because new metric and entities are automatically registered by the database, there is no need to create metrics ahead of time in order to insert records into a new table.

```sql
/*  
  The database will automatically create new metric 'mtr-1'
  and new entity 'ent-1', if necessary.
*/
INSERT INTO "mtr-1" (entity, datetime, value)
             VALUES ('ent-1', '2017-08-21T00:00:00Z', 100)
```

## Requirements

The `INSERT` and `UPDATE` statements can reference only the following predefined columns.

### Series Columns

**Column** | **Datatype** | **Comments**
------|------|------
entity | varchar | Required
metric | varchar | Required (1)
datetime | timestamp | Required (2)
time | bigint | Required (2)
text | varchar | Required (3)
value | decimal | Required (3)
tags.{name} | varchar | Series tag with name `{name}`
tags | varchar | Multiple series tags (4)

### Metric Metadata Columns

**Column** | **Datatype** | **Comments**
------|------|------
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
metric.tags.{name} | varchar | Metric tag with name `{name}`
metric.tags | varchar | Multiple metric tags (4)
metric.timePrecision | varchar |
metric.timeZone | varchar |
metric.versioning | boolean |

> Refer to metric field [details](https://github.com/axibase/atsd/blob/master/api/meta/metric/list.md#fields).

### Entity Metadata Columns

**Column** | **Datatype** | **Comments**
------|------|------
metric.units | varchar |
entity.enabled | boolean |
entity.label | varchar |
entity.interpolate | varchar |
entity.tags.{name} | varchar | Entity tag with name `{name}`
entity.tags | varchar | Multiple entity tags (4)
entity.timeZone | varchar |

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
Text column | text | not supported
Value column | value | {metric_name}
Generated commands | 1 series command<br>1 optional metric command<br>1 optional entity command | 1 series command<br>with multiple metrics

## UPDATE Syntax

```sql
UPDATE "{metric name}" SET value = {numeric value} [, text = '{string value}' [, {metric field} = {metric field value}]] WHERE entity = {entity name} AND [time = {millis} | datetime = '{iso8601 time}']
```

Example:

```sql
UPDATE "temperature" SET value = 25.5 WHERE entity = 'sensor-01' AND tags.surface = 'Outer' AND datetime = '2017-08-21T00:00:00Z'
```

The `WHERE` condition in `UPDATE` statements supports only:

* `=` (equal) comparator.
* `LIKE` comparator with optional `ESCAPE` clause if it can be substituted with an `=` (equal) comparator.
* Boolean operator `AND` (`OR` operator is not supported).

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

The date literal values can be specified using the following formats:

* ISO 8601  `yyyy-MM-dd'T'HH:mm:ss[.SSS]'Z'`

    ISO 8601 with optional milliseconds. Only UTC timezone 'Z' is allowed.

* Local Time `yyyy-MM-dd HH:mm:ss[.fffffffff]`

    Fractional second are rounded to milliseconds (3 digits).

### Time Zone

#### Time Zone in `Statement` Queries

Parsing of `datetime` column values specified in the local time format, e.g. '2017-08-15 16:00:00', depends on the value of the `timestamptz` connection string parameter.

* If `timestamptz` is set to `true` (default)

    The local timestamp is converted into the date object using the UTC time zone.

* If `timestamptz` is set to `false`

    The local timestamp is converted into the date object using the timezone on the client which is based on system time or `-Duser.timeZone` parameter.

#### Time Zone in `PreparedStatement` Queries

The results of setting `datetime` column value using `PreparedStatement#setTimestamp` method depend on the value of the `timestamptz` connection string parameter.

* If `timestamptz` is set to `true` (default)

    The `Timestamp.getTime()` method returns the number of milliseconds since 1970-Jan-01 00:00:00 UTC.

* If `timestamptz` is set to `false`

    The `Timestamp.getTime()` method returns the number of milliseconds since 1970-Jan-01 00:00:00 in **local** time zone.

```java
    // Assuming that current time zone is Europe/Berlin
    final String user = "atsd_user_name";
    final String password = "atsd_user_password";
    final String timeStamp = "2017-08-22 00:00:00";
    final DateTimeFormatter formatter = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));
    final long millis = ZonedDateTime.parse(timeStamp, formatter).toInstant().toEpochMilli();
    final String query = "INSERT INTO \"m-insert-dt\" (datetime, entity, value) VALUES (?,'test-tz',0)";
    
    try (final Connection connection = DriverManager.getConnection("jdbc:atsd://atsd_host:8443;timestamptz=true", user, password);
         final PreparedStatement stmt = connection.prepareStatement(query)) {
    
        stmt.setString(1, timeStamp);
        stmt.executeUpdate(); // 2017-08-22T00:00:00.000Z
    
        stmt.setTimestamp(1, new Timestamp(millis));
        stmt.executeUpdate(); // 2017-08-22T00:00:00.000Z
    
        stmt.setLong(1, millis);
        stmt.executeUpdate(); // 2017-08-22T00:00:00.000Z
    }
    
    try (final Connection connection = DriverManager.getConnection("jdbc:atsd://atsd_host:8443;timestamptz=false", user, password);
         final PreparedStatement stmt = connection.prepareStatement(query)) {
    
        stmt.setString(1, timeStamp);
        stmt.executeUpdate(); // 2017-08-21T22:00:00.000Z
    
        stmt.setTimestamp(1, new Timestamp(millis));
        stmt.executeUpdate(); // 2017-08-22T02:00:00.000Z
    
        stmt.setLong(1, millis);
        stmt.executeUpdate(); // 2017-08-22T00:00:00.000Z
    }
```

## Batch Inserts

Batch queries improve insert performance by sending commands in batches.

```java
public class BatchStatementExample {
	public static void main(String[] args) throws Exception {
		Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");

		String userName = "atsd_user_name";
        String password = "atsd_user_password";
        String connectionString = "jdbc:atsd://atsd_host:8443";

		final String insertQuery = "INSERT INTO \"test-metric\" (time, entity, value, entity.tags, metric.tags) VALUES (%s,%s,%s,%s,%s)";
		final String insertAtsdSeriesQuery = "INSERT INTO atsd_series (metric, time, entity, value, entity.tags, metric.tags) VALUES ('test-metric',%s,%s,%s,%s,%s)";
		try (final Connection connection = DriverManager.getConnection(connectionString, userName, password);
			 Statement stmt = connection.createStatement()) {
			final String entityName = "test-entity";
			int i = 1;
			stmt.addBatch(buildQuery(insertQuery, i * 1000, entityName, i++, null, "test1=value1"));
			stmt.addBatch(buildQuery(insertQuery, i * 1000, entityName, i++, "test1=value1", null));
			stmt.addBatch(buildQuery(insertAtsdSeriesQuery, i * 1000, entityName, i++, null, null));
			stmt.addBatch(buildQuery(insertAtsdSeriesQuery, i * 1000, entityName, i++, "test1=value1", "test1=value1"));
			System.out.println("Inserted: " + Arrays.toString(stmt.executeBatch()));
		}
	}

	private static String buildQuery(String query, long time, String entity, double value,
									 String entityTags, String metricTags) {
		return String.format(query, time, formatStringArgument(entity), value, formatStringArgument(entityTags), formatStringArgument(metricTags));
	}

	private static String formatStringArgument(String parameter) {
		return parameter == null ? "null" : "'" + parameter + "'";
	}
}
```

The following commands will be generated and sent to ATSD.

```ls
series e:test-entity ms:1000 m:test-metric=1.0
metric m:test-metric t:test1=value1

series e:test-entity ms:2000 m:test-metric=2.0
entity e:test-entity t:test1=value1

series e:test-entity ms:3000 m:test-metric=3.0

series e:test-entity ms:4000 m:test-metric=4.0
entity e:test-entity t:test1=value1
metric m:test-metric t:test1=value1
```

## Parameterized Queries

In the query above String operations were used for filling parameters. A better approach is to use PreparedStatement which will apply proper arguments formatting based on their type.

A question mark (?) is used as a parameter placeholder. Question marks inside single or double quotes are not determined as placeholders.

```java
public class PreparedStatementExample {
	public static void main(String[] args) throws Exception {
		Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");

		String userName = "atsd_user_name";
		String password = "atsd_user_password";
		String connectionString = "jdbc:atsd://atsd_host:8443";

		final String insertQuery = "INSERT INTO \"test-metric\" (time, entity, value, entity.tags, metric.tags) VALUES (?,?,?,?,?)";
		final String insertAtsdSeriesQuery = "INSERT INTO atsd_series (metric, time, entity, value, entity.tags, metric.tags) VALUES ('test-metric',?,?,?,?,?)";
		try (final Connection connection = DriverManager.getConnection(connectionString, userName, password);
			 PreparedStatement simplePs = connection.prepareStatement(insertQuery);
			 PreparedStatement atsdSeriesPs = connection.prepareStatement(insertAtsdSeriesQuery)) {
			final String entityName = "test-entity";
			long i = 0;
			fillParameters(simplePs, i * 1000, entityName, i++, null, "'test1=value1'").addBatch();
			fillParameters(simplePs, i * 1000, entityName, i++, "'test1=value1'", null).addBatch();
			fillParameters(atsdSeriesPs, i * 1000, entityName, i++, null, null).addBatch();
			fillParameters(atsdSeriesPs, i * 1000, entityName, i++, "'test1=value1'", "'test1=value1'").addBatch();
			System.out.println("Inserted with 'INSERT INTO \"test-metric\"': " + Arrays.toString(simplePs.executeBatch()));
			System.out.println("Inserted with 'INSERT INTO atsd_series': " + Arrays.toString(atsdSeriesPs.executeBatch()));
		}
	}

	private static PreparedStatement fillParameters(PreparedStatement ps, long time, String entity, double value,
													String entityTags, String metricTags) throws SQLException {
		int i = 1;
		ps.setLong(i++, time);
		ps.setString(i++, entity);
		ps.setDouble(i++, value);
		ps.setString(i++, entityTags);
		ps.setString(i, metricTags);
		return ps;
	}
}
```

## Examples

* [BatchExample.java](https://github.com/axibase/atsd-jdbc-test/blob/master/src/test/java/com/axibase/tsd/driver/jdbc/examples/BatchExample.java)

## Transactions

Transactions are not supported.
