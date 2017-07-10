[![Build Status](https://secure.travis-ci.org/axibase/atsd-jdbc.png?branch=master)](https://travis-ci.org/axibase/atsd-jdbc)  [![Codacy Badge](https://api.codacy.com/project/badge/grade/4cdddfc67ef742818be7d81d8f53aebc)](https://www.codacy.com/app/alexey-reztsov/atsd-jdbc)
[![License](https://img.shields.io/badge/License-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.axibase/atsd-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.axibase/atsd-jdbc)

# JDBC driver

The JDBC driver provides a convenient way to query the Axibase Time Series Database via SQL. 

Internal communication is implemented by transferring results in CSV format via HTTP or HTTPS protocols. See the [SQL API Documentation](https://github.com/axibase/atsd/tree/master/api/sql#overview) for a description of the query syntax and examples.

## JDBC URL

The ATSD JDBC driver prefix is `jdbc:axibase:atsd:`, followed by the http/https URL of the ATSD SQL API endpoint, and optional driver properties.

```ls
jdbc:axibase:atsd:http://atsd_hostname:8088/api/sql
jdbc:axibase:atsd:https://atsd_hostname:8443/api/sql;trustServerCertificate=true
```

## License

This project is released under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).

## Compatibility

| **Database Version** | **Driver Version** |
|:---|---|
| 12400 |  1.2.1 |
| 12500 |  1.2.6 |
| 14049 |  1.2.8 |
| 14126 | 1.2.10 |
| 14220 | 1.2.12 |
| 14451 | 1.2.15 |
| 14540 | 1.2.16 |
| 16130 | 1.2.20 |

For a given database version, the above table specifies a range of compatible driver versions.

For example, database version 14150 supports all driver versions between 1.2.10 (inclusive) and 1.2.12 (exclusive).

## JDBC Connection Properties Supported by Driver

| **Name** | **Type** | **Default** | **Description** |
| :--- | --- | ---: | --- |
| trustServerCertificate | boolean | `false` | Skip SSL certification validation |
| connectTimeout | number | 5 | Connection timeout, in seconds. |
| readTimeout | number | 0 | Read timeout, in seconds. |
| strategy | `file`, `memory`, `stream` | `stream` | Resultset processing strategy. |
| tables | comma-separated list of strings | | List of metrics or metric expressions to be exposed as tables |
| catalog | string | not set | Specify a catalog name |
| expandTags | boolean | `true` | Expose series tags as separate table columns |
| assignColumnNames | boolean | `false` | Force `ResultSetMetaData.getColumnName(index)` method to return column names.<br> If disabled, the method returns column labels. |

Properties can be included as part of the JDBC url using a semicolon as a separator. For example: `jdbc:axibase:atsd:https://10.102.0.6:8443/api/sql;trustServerCertificate=true;strategy=file`.

## Resultset Processing Strategy

|**Name**|**Description**|
|:--|---|
|`stream`| Reads data received from the database in batches when triggered by the `ResultSet.next()` invocation. This command keeps the connection open until all rows are processed by the client.|
|`file`| Buffers data received from the database to a temporary file on the local file system and reads rows from the file on the `ResultSet.next()` invocation. |
|`memory`| Buffers data received from the database into the application memory and returns rows on the `ResultSet.next()` invocation directly from a memory structure. |

* The `stream` strategy is faster than the alternatives, at the expense of keeping the database connection open. It is not recommended if row processing may last a significant time. 
* While the `memory` strategy may be more efficient than `file`, it requires more memory. Generally speaking, the `memory` strategy is better suited to queries returning thousands of rows, whereas the `file`/`stream` strategy can process millions of rows (provided disk space is available).

Choose the appropriate strategy based on available Java heap memory, disk space, and expected row counts produced by typical queries.

## Integration

### Requirements

* Java 1.7 and later

### Apache Maven

Add dependency to `pom.xml` in your project. The JDBC driver will be imported automatically since the project is hosted in the MavenCentral/SonaType repositories. 

```xml
<dependency>
    <groupId>com.axibase</groupId>
    <artifactId>atsd-jdbc</artifactId>
    <version>1.2.21</version>
</dependency>
```

Alternatively, build the project with Maven:

```bash
$ mvn clean install -DskipTests=true
```

### Classpath

Download the driver [jar file](https://github.com/axibase/atsd-jdbc/releases/download/RELEASE-1.2.21/atsd-jdbc-1.2.21-DEPS.jar) with dependencies and add it to the classpath of your application.

```
* Unix: java -cp "atsd-jdbc-1.2.21-DEPS.jar:lib/*" your.package.MainClass
* Windows java -cp "atsd-jdbc-1.2.21-DEPS.jar;lib/*" your.package.MainClass
```

### Database Tools

Download the jar file with the dependencies from above and import into your database manager. For example [DbVisualizer](https://www.dbvis.com). 

Follow the instructions in the manager's user guide to create a custom driver based on the ATSD jar file.

## Supported Data Types

| **TYPE NAME** | **DATA TYPE** | **PRECISION**  |
|:---------|----------:|-----------:|
| BOOLEAN | 16 | 1 |
| DECIMAL | 3 | -1 |
| DOUBLE | 8 | 52 |
| FLOAT | 6 | 23 |
| INTEGER | 4 | 10 |
| BIGINT | -5 | 19 |
| SMALLINT | 5 | 5 |
| VARCHAR | 12 | 2147483647 |
| TIMESTAMP | 93 | 23 |
| JAVA_OBJECT | 2000 | 2147483647 |

## Capabilities

* [Driver capabilities](capabilities.md) reference guide.
* ATSD [SQL API documentation](https://github.com/axibase/atsd/tree/master/api/sql#overview).

## Usage

To execute a query load the driver class, open a connection, create a SQL statement, execute the query, and process the result set:

```java
    Class.forName("com.axibase.tsd.driver.jdbc.AtsdDriver");
    Connection connection = DriverManager.getConnection("jdbc:axibase:atsd:" + <ATSD_URL>, <USERNAME>, <PASSWORD>);
    String query = "SELECT value, datetime FROM cpu_busy WHERE entity = 'nurswgvml007' LIMIT 1";
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(query);
```

## Prepared Statements

Initialize a prepared statement, set placeholder parameters, and execute the query:

```java
    String query = "SELECT value, datetime FROM cpu_busy WHERE entity = ? LIMIT 1";
    PreparedStatement preparedStatement = connection.prepareStatement(query);
    preparedStatement.setString(1, "nurswgvml007");
    ResultSet resultSet = prepareStatement.executeQuery();
```

### EndTime Expressions in Prepared Statements

> Supported in 1.2.9+.

To set an [`endTime`](https://github.com/axibase/atsd/blob/master/end-time-syntax.md) expression as a parameter in a prepared statement, cast the statement to `AtsdPreparedStatement` and invoke the `setTimeExpression` method.

```java
    String query = "SELECT * FROM df.disk_used WHERE datetime > ? LIMIT 1";
    PreparedStatement preparedStatement = connection.prepareStatement(query);
    AtsdPreparedStatement axibaseStatement = (AtsdPreparedStatement)preparedStatement;
    axibaseStatement.setTimeExpression(1, "current_day - 1 * week + 2 * day");
```

## SQL Warnings

The database may return SQL warnings as opposed to raising a non-recoverable error in some cases, such as unknown tag or tag value.

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
        String username = System.getProperty("atsd.user");
        String password = System.getProperty("atsd.password");
        String hostUrl = System.getProperty("atsd.host");
        String sqlUrl = "jdbc:axibase:atsd:" + hostUrl + "/api/sql;trustServerCertificate=true";

        String query = "SELECT * FROM mpstat.cpu_busy WHERE datetime > now - 1 * HOUR LIMIT 5";
        Connection connection = null;
        try {
            System.out.println("Connecting to " + sqlUrl);
            connection = DriverManager.getConnection(sqlUrl, username, password);
            System.out.println("Connection established to " + sqlUrl);
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
    
    String atsdHost = System.getProperty("atsd.host");
    String username = System.getProperty("atsd.user");
    String password = System.getProperty("atsd.password");
    String url = "jdbc:axibase:atsd:" + atsdHost + "/api/sql;trustServerCertificate=true";
    
    String query = "SELECT entity, datetime, value, tags.mount_point, tags.file_system "
            + "FROM df.disk_used_percent WHERE entity = 'NURSWGHBS001' AND datetime > now - 1 * HOUR LIMIT 10";

    try (Connection conn = DriverManager.getConnection(url, username, password);
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

    String hostUrl = System.getProperty("atsd.host");
    String userName = System.getProperty("atsd.user");
    String password = System.getProperty("atsd.password");
    String sqlUrl = "jdbc:axibase:atsd:" + hostUrl + "/api/sql;trustServerCertificate=true";

    try (Connection connection = DriverManager.getConnection(sqlUrl, userName, password)) {

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
Driver Version: 	1.2.21

TypeInfo:
	Name:BIGINT 	    CS: false 	Type: -5 	Precision: 19
	Name:BOOLEAN 	    CS: false 	Type: 16 	Precision: 1
	Name:DECIMAL 	    CS: false 	Type: 3 	Precision: -1
	Name:DOUBLE 	    CS: false 	Type: 8 	Precision: 52
	Name:FLOAT 	    CS: false 	Type: 6 	Precision: 23
	Name:INTEGER 	    CS: false 	Type: 4 	Precision: 10
	Name:JAVA_OBJECT    CS: false 	Type: 2000 	Precision: 2147483647
	Name:SMALLINT 	    CS: false 	Type: 5 	Precision: 5
	Name:VARCHAR 	    CS: true 	Type: 12 	Precision: 2147483647
	Name:TIMESTAMP 	    CS: false 	Type: 93 	Precision: 23

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
