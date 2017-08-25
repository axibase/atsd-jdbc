[![Build Status](https://secure.travis-ci.org/axibase/atsd-jdbc.png?branch=master)](https://travis-ci.org/axibase/atsd-jdbc)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/791a8e6d43634307a1649ca6f5ad7a2e)](https://www.codacy.com/app/anton-rib/atsd-jdbc)
[![License](https://img.shields.io/badge/License-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.axibase/atsd-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.axibase/atsd-jdbc)
[![Dependency Status](https://www.versioneye.com/user/projects/57b45de0f0b3bb0049ff712b/badge.svg?style=flat-square)](https://www.versioneye.com/user/projects/57b45de0f0b3bb0049ff712b)

# JDBC driver

The ATSD JDBC driver enables Java applications to read and write time-series data from the Axibase Time Series Database using SQL.

## Reading Data

To retrieve records from the database, execute `SELECT` statements following the query syntax and examples provided in the [ATSD SQL documentation](https://github.com/axibase/atsd/tree/master/api/sql#overview).

## Writing Data

To write data into ATSD, execute `INSERT` or `UPDATE` [statements](insert.md) which are parsed by the driver and transformed into `series` commands sent into the database.

## JDBC URL

The ATSD JDBC driver prefix is `jdbc:atsd:`, followed by the ATSD hostname (IP address) and port, optional catalog and driver properties.

```ls
jdbc:atsd://hostname:port[/catalog][;property_name=property_value]
```

```ls
jdbc:atsd://10.102.0.6:8443
```

Properties can be appended to the JDBC URL using a semicolon as a separator:

```ls
jdbc:atsd://10.102.0.6:8443;tables=infla*;expandTags=true`
```

## License

This project is released under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).

## Compatibility

The table below specifies a range of compatible driver versions for a given database revision number displayed on the **Admin > System Information** page.

For example, database revision number 16200 supports driver versions between 1.2.20 (inclusive) and 1.3.0 (exclusive).

| **Database Version** | **Driver Version** |
|:---|---|
| 12400 | 1.2.1  |
| 12500 | 1.2.6  |
| 14049 | 1.2.8  |
| 14126 | 1.2.10 |
| 14220 | 1.2.12 |
| 14451 | 1.2.15 |
| 14540 | 1.2.16 |
| 16130 | 1.2.20 |
| 16620 | 1.3.0  |
| 16643 | 1.3.2  |
| 16855 | 1.3.3  |

## JDBC Connection Properties Supported by Driver

| **Name** | **Type** | **Supported drivers** | **Default** | **Description** |
| :--- | --- | --- | ---: | --- |
| trust | boolean | 1.3.1+ | `true` | Skip SSL certificate validation. |
| secure | boolean | 1.3.1+ | `true` | Use HTTPS protocol to communicate with ATSD. |
| connectTimeout | number | 1.2.7+ | 5 | Connection timeout, in seconds. |
| readTimeout | number | 1.2.7+ | 0 | Read I/O timeout, in seconds. |
| strategy | `file`, `memory`, `stream` | 1.0+ | `stream` | Resultset processing strategy. |
| tables | comma-separated list | 1.2.21+ | `*` | List of metric names or metric expressions returned as tables by the `DatabaseMetaData#getTables` method. |
| expandTags | boolean | 1.2.21+ | `false` | Return series tags as separate columns in the `DatabaseMetaData#getColumns` method. |
| metaColumns | boolean | 1.2.21+ | `false` | Add `metric.tags`, `entity.tags`, and `entity.groups` columns to the list of columns returned by the `DatabaseMetaData#getColumns` method. |
| assignColumnNames | boolean | 1.3.0+ | `false` | Force `ResultSetMetaData.getColumnName(index)` method to return column names.<br> If disabled, the method returns column labels. |
| timestamptz | boolean | 1.3.2+ | `true` | Instantiate Timestamp fields with the timezone stored in the database (UTC). If `timestamptz` is set to `false`, the Timestamp fields are created based on the client's local timezone. |
| missingMetric | `error`, `warning`, `none` | 1.3.2+ | `warning` | Control the behavior when the referenced metric doesn't exist. If 'error', the driver will raise an `AtsdMetricNotFoundException`. If `warning`, an SQL Warning will be returned without errors. If `none`, no error or warning will be created. |
| compatibility | `odbc2` | 1.3.2+ | not set | Simulate behavior of ODBC2.0 drivers: substitute `bigint` datatype with `double`, return `11` as `timestamp` type code |

## Resultset Processing Strategy

Choose the appropriate strategy based on available Java heap memory, disk space, and expected row counts produced by typical queries.

|**Name**|**Description**|
|:--|---|
|`stream`| Reads data received from the database in batches when triggered by the `ResultSet.next()` invocation. This command keeps the connection open until all rows are processed by the client.|
|`file`| Buffers data received from the database to a temporary file on the local file system and reads rows from the file on the `ResultSet.next()` invocation. |
|`memory`| Buffers data received from the database into the application memory and returns rows on the `ResultSet.next()` invocation directly from a memory structure. |

* While the `memory` strategy may be more efficient than `file`, it requires more memory. The `memory` strategy is optimal queries returning thousands of rows, whereas the `file` strategy can process millions of rows, provided disk space is available.
* The `stream` strategy is faster than the alternatives, at the expense of keeping the database connection open. It is not recommended if row processing may last a significant time on a slow client.

## Requirements

* Java 1.7 and later

## Downloads

* Compiled drivers are listed on the [Releases](https://github.com/axibase/atsd-jdbc/releases/) page.
* `atsd-jdbc-*.jar` files are built without dependencies.
* `atsd-jdbc-*-DEPS.jar` files contain dependencies.
* The latest jar file with dependencies is [`atsd-jdbc-1.3.4-DEPS.jar`](https://github.com/axibase/atsd-jdbc/releases/download/RELEASE-1.3.4/atsd-jdbc-1.3.4-DEPS.jar).

## Integration

### Classpath

Download the [jar file](https://github.com/axibase/atsd-jdbc/releases/download/RELEASE-1.3.4/atsd-jdbc-1.3.4-DEPS.jar) with dependencies and add it to the classpath of your application.

* Unix:

```sh
java -cp "atsd-jdbc-1.3.4-DEPS.jar:lib/*" your.package.MainClass
```

* Windows:

```sh
java -cp "atsd-jdbc-1.3.4-DEPS.jar;lib/*" your.package.MainClass
```

### Apache Maven

Add `atsd-jdbc` dependency to `pom.xml` in your project.

```xml
<dependency>
    <groupId>com.axibase</groupId>
    <artifactId>atsd-jdbc</artifactId>
    <version>1.3.4</version>
</dependency>
```

The ATSD JDBC driver is published in MavenCentral/SonaType repositories and will be imported automatically.

Alternatively, build the project from sources:

```bash
$ mvn clean package -DskipTests=true
```

### Database Tools

Download the [jar file](https://github.com/axibase/atsd-jdbc/releases/download/RELEASE-1.3.4/atsd-jdbc-1.3.4-DEPS.jar) with dependencies and import it into your database client tool.

Follow the instructions to create a custom JDBC driver based on the ATSD jar file.

### Reporting Tools

  * [Alteryx Designer](https://github.com/axibase/atsd/blob/master/integration/alteryx/README.md)
  * [IBM SPSS Modeler](https://github.com/axibase/atsd/blob/master/integration/spss/modeler/README.md)
  * [IBM SPSS Statistics](https://github.com/axibase/atsd/blob/master/integration/spss/statistics/README.md)
  * [MatLab](https://github.com/axibase/atsd/blob/master/integration/matlab/README.md)
  * [Pentaho Data Integration](https://github.com/axibase/atsd/blob/master/integration/pentaho/data-integration/README.md)
  * [Pentaho Report Designer](https://github.com/axibase/atsd/blob/master/integration/pentaho/report-designer/README.md)  
  * [Stata](https://github.com/axibase/atsd/blob/master/integration/stata/README.md)
  * [Tableau](https://github.com/axibase/atsd/blob/master/integration/tableau/README.md)
  * Generic [ODBC](https://github.com/axibase/atsd/blob/master/integration/odbc/README.md)

## Supported Data Types

| **TYPE NAME** | **DATA TYPE** | **PRECISION**  |
|:---------|----------:|-----------:|
| BOOLEAN | 16 | 1 |
| DECIMAL | 3 | 0 |
| DOUBLE | 8 | 15 |
| FLOAT | 7 | 7 |
| INTEGER | 4 | 10 |
| BIGINT | -5 | 19 |
| SMALLINT | 5 | 5 |
| VARCHAR | 12 | 131072 |
| TIMESTAMP | 93 | 23 |
| JAVA_OBJECT | 2000 | 2147483647 |

## Capabilities

* [Driver capabilities](capabilities.md) reference guide.
* ATSD [SQL API documentation](https://github.com/axibase/atsd/tree/master/api/sql#overview).

## Usage

To execute a query load the driver class, open a connection, create a SQL statement, execute the query, and process the result set:

```java
    Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");
    Connection connection = DriverManager.getConnection("jdbc:atsd://10.102.0.5:8443", "user-1", "my-pwd!");
    String query = "SELECT value, datetime FROM \"mpstat.cpu_busy\" WHERE entity = 'nurswgvml007' LIMIT 1";
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
```

## Prepared Statements

Initialize a prepared statement, set placeholder parameters, and execute the query:

```java
    String query = "SELECT value, datetime FROM \"mpstat.cpu_busy\" WHERE entity = ? LIMIT 1";
    PreparedStatement preparedStatement = connection.prepareStatement(query);
    preparedStatement.setString(1, "nurswgvml007");
    ResultSet resultSet = prepareStatement.executeQuery();
```

### EndTime Expressions in Prepared Statements

> Supported in 1.2.9+.

To set an [`endTime`](https://github.com/axibase/atsd/blob/master/end-time-syntax.md) expression as a parameter in a prepared statement, cast the statement to `AtsdPreparedStatement` and invoke the `setTimeExpression` method.

```java
    String query = "SELECT * FROM \"df.disk_used\" WHERE datetime > ? LIMIT 1";
    PreparedStatement preparedStatement = connection.prepareStatement(query);
    AtsdPreparedStatement axibaseStatement = (AtsdPreparedStatement)preparedStatement;
    axibaseStatement.setTimeExpression(1, "current_day - 1 * week + 2 * day");
```

## SQL Warnings

The database may return SQL warnings as opposed to raising a non-recoverable error in cases, such as unknown tag or tag value.

To retrieve SQL warnings, invoke the `resultSet.getWarnings()` method:

```java
    SQLWarning rsWarning = resultSet.getWarnings();
    if (rsWarning != null) {
        System.err.println(rsWarning.getMessage());
    }
```

## Basic Example

```java
import java.sql.*;


public class TestQuery {

    public static void main(String[] args) throws Exception {

        Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");  
	String host = args[0];
	String port = args[1];
        String username = args[2];
        String password = args[3];

        String jdbcUrl = "jdbc:atsd://" + host + ":" port;

        String query = "SELECT * FROM \"mpstat.cpu_busy\" WHERE datetime > now - 1 * HOUR LIMIT 5";
        Connection connection = null;
        try {
            System.out.println("Connecting to " + jdbcUrl);
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            System.out.println("Connection established to " + jdbcUrl);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(query);
            System.out.println("Query complete.");
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            int rowNumber = 1;
            while (resultSet.next()) {
                System.out.println("= row " + rowNumber++);
                for (int colIndex = 1; colIndex <= columnCount; colIndex++) {
                    System.out.println("  " + metaData.getColumnLabel(colIndex) + " = " + resultSet.getString(colIndex));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
           try {
               connection.close();
           } catch(Exception e){}
        }
    }
}
```

## Additional Examples

```java

    Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");

    String host = args[0];
    String port = args[1];
    String username = args[2];
    String password = args[3];
    String jdbcUrl = "jdbc:atsd://" + host + ":" port;

    String query = "SELECT entity, datetime, value, tags.mount_point, tags.file_system "
            + "FROM \"df.disk_used_percent\" WHERE entity = 'NURSWGHBS001' AND datetime > now - 1 * HOUR LIMIT 10";

    try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
         Statement statement = conn.createStatement();
         ResultSet resultSet = statement.executeQuery(query)) {

        int rowNumber = 1;
        while (resultSet.next()) {
            System.out.print(rowNumber++);
            System.out.print("\tentity = " + resultSet.getString("entity"));
            System.out.print("\tdatetime = " + resultSet.getTimestamp("datetime").toString());
            System.out.print("\tvalue = " + resultSet.getString("value"));
            System.out.print("\ttags.mount_point = " + resultSet.getString("tags.mount_point"));
            System.out.println("\ttags.file_system = " + resultSet.getString("tags.file_system"));
        }

        final SQLWarning warnings = resultSet.getWarnings();
        if (warnings != null) {
            warnings.printStackTrace();
        }
    } catch (Exception e){
        e.printStackTrace();
    }
```

Results:

```ls
 1 entity = nurswghbs001 datetime = 2016-08-22 12:52:03.0 value = 28.8116 tags.mount_point = / tags.file_system = /dev/md2
 2 entity = nurswghbs001 datetime = 2016-08-22 12:52:04.0 value = 28.8116 tags.mount_point = / tags.file_system = /dev/md2
 3 entity = nurswghbs001 datetime = 2016-08-22 12:52:18.0 value = 28.8116 tags.mount_point = / tags.file_system = /dev/md2
 4 entity = nurswghbs001 datetime = 2016-08-22 12:52:19.0 value = 28.8116 tags.mount_point = / tags.file_system = /dev/md2
 5 entity = nurswghbs001 datetime = 2016-08-22 12:52:33.0 value = 28.8117 tags.mount_point = / tags.file_system = /dev/md2
 6 entity = nurswghbs001 datetime = 2016-08-22 12:52:34.0 value = 28.8117 tags.mount_point = / tags.file_system = /dev/md2
 7 entity = nurswghbs001 datetime = 2016-08-22 12:52:48.0 value = 28.8117 tags.mount_point = / tags.file_system = /dev/md2
 8 entity = nurswghbs001 datetime = 2016-08-22 12:52:49.0 value = 28.8117 tags.mount_point = / tags.file_system = /dev/md2
 9 entity = nurswghbs001 datetime = 2016-08-22 12:53:03.0 value = 28.8117 tags.mount_point = / tags.file_system = /dev/md2
10 entity = nurswghbs001 datetime = 2016-08-22 12:53:04.0 value = 28.8117 tags.mount_point = / tags.file_system = /dev/md2
```

The following example shows how to extract metadata from the database:

```java

    Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");

    String host = args[0];
    String port = args[1];
    String username = args[2];
    String password = args[3];

    String jdbcUrl = "jdbc:atsd://" + host + ":" port;

    try (Connection connection = DriverManager.getConnection(jdbcUrl, userName, password)) {

        DatabaseMetaData metaData = connection.getMetaData();
        String databaseProductName = metaData.getDatabaseProductName();
        String databaseProductVersion = metaData.getDatabaseProductVersion();
        String driverName = metaData.getDriverName();
        String driverVersion = metaData.getDriverVersion();
        System.out.println("Product Name:   \t" + databaseProductName);
        System.out.println("Product Version:\t" + databaseProductVersion);
        System.out.println("Driver Name:    \t" + driverName);
        System.out.println("Driver Version: \t" + driverVersion);
        System.out.println("\nTypeInfo:");

        ResultSet rs = metaData.getTypeInfo();
        while (rs.next()) {
            String name = rs.getString("TYPE_NAME");
            int type = rs.getInt("DATA_TYPE");
            int precision = rs.getInt("PRECISION");
            boolean isCS = rs.getBoolean("CASE_SENSITIVE");
            System.out.println(String.format(
                    "\tName:%s \tCS: %s \tType: %s \tPrecision: %s", name, isCS, type, precision));
        }
        System.out.println("\nTableTypes:");

        rs = metaData.getTableTypes();
        while (rs.next()) {
            String type = rs.getString(1);
            System.out.println('\t' + type);
        }
        rs = metaData.getCatalogs();

        while (rs.next()) {
            String catalog = rs.getString(1);
            System.out.println("\nCatalog: \t" + catalog);
            ResultSet rs1 = metaData.getSchemas(catalog, null);
            while (rs1.next()) {
                String schema = rs1.getString(1);
                System.out.println("Schema: \t" + schema);
            }
        }
    }

```

Results:

```
Product Name:   	Axibase
Product Version:	Axibase Time Series Database, <ATSD_EDITION>, Revision: <ATSD_REVISION_NUMBER>
Driver Name:    	ATSD JDBC driver
Driver Version: 	1.3.4

TypeInfo:
	Name:bigint 	    CS: false 	Type: -5 	Precision: 19
	Name:boolean 	    CS: false 	Type: 16 	Precision: 1
	Name:decimal 	    CS: false 	Type: 3 	Precision: 0
	Name:double 	    CS: false 	Type: 8 	Precision: 15
	Name:float          CS: false 	Type: 7 	Precision: 7
	Name:integer 	    CS: false 	Type: 4 	Precision: 10
	Name:java_object 	CS: false 	Type: 2000 	Precision: 2147483647
	Name:smallint 	    CS: false 	Type: 5 	Precision: 5
	Name:varchar 	    CS: true 	Type: 12 	Precision: 131072
	Name:timestamp 	    CS: false 	Type: 93 	Precision: 23

TableTypes:
	TABLE
	VIEW
	SYSTEM

Catalog: null
```

## Spring Framework Integration

We recommend the [Spring Data JDBC](https://github.com/nurkiewicz/spring-data-jdbc-repository) library to integrate ATSD JDBC driver with Spring.

See an example [here](https://github.com/axibase/atsd-jdbc-test/tree/master/src/main/java/com/axibase/tsd/driver/jdbc/spring).

[config file](https://github.com/axibase/atsd-jdbc-test/blob/master/src/main/java/com/axibase/tsd/driver/jdbc/spring/AtsdRepositoryConfig.java) gist:

```java

    @Configuration
    public class AtsdRepositoryConfig {

    @Bean
    public SqlGenerator sqlGenerator() {
        return new AtsdSqlGenerator();
    }

    @Bean
    public DataSource dataSource() {
        final HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(login);
        dataSource.setPassword(password);
        dataSource.setReadOnly(true);
        return dataSource;
    }

    @Bean
    public EntityValueDoubleRepository entityRepository() {
        return new EntityValueDoubleRepository(table);
    }

}
```

[repository file](https://github.com/axibase/atsd-jdbc-test/blob/master/src/main/java/com/axibase/tsd/driver/jdbc/spring/EntityValueDoubleRepository.java) gist:

```java

    @Repository
    public class EntityValueDoubleRepository extends JdbcRepository<EntityValueDouble, Double> {

    public EntityValueDoubleRepository(String table) {
        super(ROW_MAPPER, new MissingRowUnmapper<EntityValueDouble>(), table);
    }

    public static final RowMapper<EntityValueDouble> ROW_MAPPER = new RowMapper<EntityValueDouble>() {
        @Override
        public EntityValueDouble mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new EntityValueDouble(rs.getString("entity"), rs.getLong("time"), rs.getDouble("value"));
        }
    };

}
```

Usage example with [Spring Boot](https://github.com/axibase/atsd-jdbc-test/blob/master/src/main/java/com/axibase/tsd/driver/jdbc/spring/SampleDriverApplication.java):

```java

    @Resource
    private EntityValueDoubleRepository entityRepository;

    @Override
    public void run(String... args) throws Exception {
        PageRequest page = new PageRequest(0, 1000, Direction.DESC, "time", "value");
        Page<EntityValueDouble> result = entityRepository.findAll(page);
        List<EntityValueDouble> list = result.getContent();
        assert list != null && !list.isEmpty();
    }

```
