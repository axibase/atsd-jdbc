# Changelog

## 1.4.0

* Force double quotes for database identifiers (table and column names), single quotes for string literals.
* Added missing columns for the `atsd_series` table when calling `DatabaseMetadata#getColumns()`.
* Fixed offset calculation for UPDATE statements when `timestamptz=false`.
* Added the `AtsdResultSet#getTags()` and `AtsdPreparedStatement#setTags(Map<String, String> tags)` methods.
* Fixed comments handling on the first line.
* Use SQL wildcards `%` and `_` instead of `*` and `?` in the `tables` connection property.

## 1.3.4

* Added support for metric and entity fields in `INSERT` and `UPDATE` queries.

## 1.3.3

* Fixed `PreparedStatement#setObject` behavior.
* Fixed `Statement#setQueryTimeout` taking millis instead of seconds.
* Added ODBC2 compatibility mode.
* Numeric values can be used to set `datetime` column in `INSERT` statements.

## 1.3.2

* New syntax for connection string: `jdbc:atsd://host:port/catalog;params`.
* Fixed Statement#getMetadata() for long queries.
* Connection string properties refactoring. `trustServerCertificate` -> `trust`, `protocol` -> `secure`.
* PreparedStatement#getMetadata throws SQLDataException instead of AtsdRuntimeException if queried metric is not found.
* Added support for `INSERT` and `UPDATE` statements with escaped table names.
* Added support for `tags` field in `INSERT` statements.
* Extended list of supported time functions with `CURRENT_TIMESTAMP` and `DBTIMEZONE`.
* Added the `timestamptz` connection property.
* Added the `missingMetric` connection property to specify the behavior when querying a non-existing metric.
* Added the `atsd_series` table to the list of tables returned by `DatabaseMetadata#getTables` method.

## 1.3.0

* Added support for INSERT and UPDATE statements.
* Changed connection string. Host must be specified without `/api/sql` endpoint.
* Fixed datatype information.

## 1.2.21

* Exposed `tables`, `catalog`, `expandTags` connection string parameters.

## 1.2.20

* Added support for the boolean data type.

## 1.2.19

* Humanized errors representation.
* Fixed skipping columns with single NULL values.
* Fixed a problem with colliding Jackson and commons-codecs dependencies in the classpath.

## 1.2.18

* Fixed classpath collision error.
* Added ability to specify connection properties in GUI.

## 1.2.17

* Updated dependencies.
* Improved performance.

## 1.2.16

* Added support for ATSD `java_object` type.
* Improved performance.
* Driver correctly recognizes NULL and empty values as of ATSD version 14540.

## 1.2.15

* Fixed generation of database metadata result sets. Better support of DbVis 9.2.
* The `Connection#getCatalog` method is implemented.
* Fixed NPE in `AtsdResultSet#close` for constant result sets.
* Implemented the `Statement#cancel` method. As of ATSD revision 14451, `Statement#cancel` forces ATSD to cancel the executed query. Older ATSD revisions release drivers break the connection. Manual query cancellation can be needed.

## 1.2.14

* Better support for GUI tools working with custom databases.

## 1.2.13

* `ResultSet#getRow` now counts rows starting with 1 instead of 0.

## 1.2.12

* Implemented the `stream` fetching strategy. The old `stream` strategy is renamed to `memory`.
* Better support for screened values.
* `ResultSet.getObject()` returns `null` for absent tags.
* `SQLFeatureNotSupportedException` is thrown while trying to create not forward-only result set.

## 1.2.11

* Older ATSD versions support (13919 and less).

## 1.2.10

* Fixed the `date_format` function result representation.
* Fixed the `Exception` throw when the value in first column starts with a quote.
* ATSD v.14126 support.

## 1.2.9-RELEASE

* Fixed the `PreparedStatement.setMaxRows` behavior.
* `SQLException` is thrown on a `ResultSet` object creation, not invocating `ResultSet.next()`.
* Fixed database metadata representation.

## 1.2.8

* Time expression support implemented. Use the `AtsdPreparedStatement.setTimeExpression(String expression)` method to validate the string as a time expression.
* Better errors description.
* Fixed `ClassCastException` on `ResultSet.getFloat()`.
* Changed `bigint` and `smallint` datatypes to be treated as strings.
* Performance improvements of `PreparedStatement`.
* Support for long column schema.
* ATSD v.14049 support.
* `connectTimeout` is 5 seconds by default.
* Default `displaySize` for varchar type is 10 kB.

## 1.2.7

* Standard SQL types are displayed in driver instead of original types from ATSD.
* `Statement.setQueryTimeout()` implemented.
* `Statement.setMaxRows()` implemented.
* Added `connectTimeout` and `readTimeout` options in the connection string.
* Error messages from `/sql/api` are handled.
* No retry after erroneous query.
