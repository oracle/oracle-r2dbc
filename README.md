![](https://github.com/oracle/oracle-r2dbc/actions/workflows/build-and-test.yml/badge.svg)

# About Oracle R2DBC

The Oracle R2DBC Driver is a Java library that supports reactive programming with Oracle Database.

Oracle R2DBC implements the R2DBC Service Provider Interface (SPI) as specified by the Reactive Relational Database Connectivity (R2DBC) project. The R2DBC SPI exposes Reactive Streams as an abstraction for remote database operations. Reactive Streams is a well defined standard for asynchronous, non-blocking, and back-pressured communication. This standard allows an R2DBC driver to interoperate with other reactive libraries and frameworks, such as Spring, Project Reactor, RxJava, and Akka Streams.

### Learn More About R2DBC:
[R2DBC Project Home Page](https://r2dbc.io)

[R2DBC Javadocs v1.0.0.RELEASE](https://r2dbc.io/spec/1.0.0.RELEASE/api/)

[R2DBC Specification v1.0.0.RELEASE](https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/)

### Learn More About Reactive Streams:
[Reactive Streams Project Home Page](http://www.reactive-streams.org)

[Reactive Streams Javadocs v1.0.3](http://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/package-summary.html)

[Reactive Streams Specification v1.0.3](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.3/README.md)

# About This Version
The 1.0.0 release Oracle R2DBC implements version 1.0.0.RELEASE of the R2DBC SPI.

Changes in this release:
- Added support for long valued update counts
- New OracleR2dbcOptions constants for V$SESSION tracing

### Integration with Spring and Other Libraries
Oracle R2DBC can interoperate with other libraries that support the 
1.0.0.RELEASE version of the R2DBC SPI. When using libraries like Spring and
r2dbc-pool, be sure to use a version which supports the 1.0.0.RELEASE of the
SPI.

Oracle R2DBC depends on the JDK 11 build of Oracle JDBC 21.5.0.0. Other 
libraries may depend on a different version of Oracle JDBC which is 
incompatible. To resolve this incompatibility, it may be necessary to explicitly 
declare the dependency in your project, ie:
```xml
<dependency>
    <groupId>com.oracle.database.jdbc</groupId>
    <artifactId>ojdbc11</artifactId>
    <version>21.5.0.0</version>
</dependency>
```

# Installation
Oracle R2DBC can be obtained from Maven Central.
```xml
<dependency>
  <groupId>com.oracle.database.r2dbc</groupId>
  <artifactId>oracle-r2dbc</artifactId>
  <version>1.0.0</version>
</dependency>
```

Oracle R2DBC can also be built from source using Maven:
`mvn clean install -DskipTests=true`

> If -DskipTests=true is omitted from the command above, then it will execute 
> end-to-end tests which connect to an Oracle Database. Tests read the connection 
> configuration from
> [src/test/resources/config.properties](src/test/resources/example-config.properties).

Oracle R2DBC is compatible with JDK 11 (or newer), and has the following runtime dependencies:
- R2DBC SPI 1.0.0.RELEASE
- Reactive Streams 1.0.3
- Project Reactor 3.4.18
- Oracle JDBC 21.5.0.0 for JDK 11 (ojdbc11.jar)
  - Oracle R2DBC relies on the Oracle JDBC Driver's [Reactive Extensions
  ](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/jdbc-reactive-extensions.html#GUID-1C40C43B-3823-4848-8B5A-D2F97A82F79B) APIs.

The Oracle R2DBC Driver has been verified with Oracle Database versions 18, 19,
 and 21.

# Code Examples

The following method returns an Oracle R2DBC `ConnectionFactory`
```java
  static ConnectionFactory getConnectionFactory() {
    String user = getUser();
    char[] password = getPassword();
    try {
      return ConnectionFactories.get(
        ConnectionFactoryOptions.builder()
          .option(ConnectionFactoryOptions.DRIVER, "oracle")
          .option(ConnectionFactoryOptions.HOST, "db.host.example.com")
          .option(ConnectionFactoryOptions.PORT, 1521)
          .option(ConnectionFactoryOptions.DATABASE, "db.service.name")
          .option(ConnectionFactoryOptions.USER, user)
          .option(ConnectionFactoryOptions.PASSWORD, CharBuffer.wrap(password))
          .build());
    }
    finally {
      Arrays.fill(password, (char)0);
    }
  }
```

The following method uses Project Reactor's [Flux](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html) to open a connection, execute a SQL query, and then close the connection:
```java
Flux.usingWhen(
  getConnectionFactory().create(),
  connection ->
    Flux.from(connection.createStatement(
      "SELECT 'Hello, Oracle' FROM sys.dual")
      .execute())
      .flatMap(result ->
        result.map(row -> row.get(0, String.class))),
  Connection::close)
  .doOnNext(System.out::println)
  .doOnError(Throwable::printStackTrace)
  .subscribe();
```
When executed, the code above will _asynchronously_ print the result of the SQL query.

The next example uses a named parameter marker, `:locale_name`, in the SQL command:
```java
Flux.usingWhen(
  getConnectionFactory().create(),
  connection ->
    Flux.from(connection.createStatement(
      "SELECT greeting FROM locale WHERE locale_name = :locale_name")
      .bind("locale_name", "France")
      .execute())
      .flatMap(result ->
        result.map(row ->
          String.format("%s, Oracle", row.get("greeting", String.class)))),
  Connection::close)
  .doOnNext(System.out::println)
  .doOnError(Throwable::printStackTrace)
  .subscribe();
```
Like the previous example, executing the code above will _asynchronously_ print 
a greeting message. "France" is set as the bind value for `locale_name`, so the
query should return a greeting like "Bonjour" when `row.get("greeting")`
is called.

Additional code examples can be found [here](sample).

# Help
For help programming with Oracle R2DBC, ask questions on Stack Overflow tagged with [[oracle] and [r2dbc]](https://stackoverflow.com/tags/oracle+r2dbc). The development team monitors Stack Overflow regularly.

Issues may be opened as described in [our contribution guide](CONTRIBUTING.md).

# Contributing

This project welcomes contributions from the community. Before submitting a pull
request, please [review our contribution guide](./CONTRIBUTING.md).

# Security

Please consult the [security guide](./SECURITY.md) for our responsible security
vulnerability disclosure process.

# License

Copyright (c) 2021 Oracle and/or its affiliates.

This software is dual-licensed to you under the Universal Permissive License
(UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License
2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
either license.

# Documentation
This document specifies the behavior of the R2DBC SPI as implemented for the 
Oracle Database. This SPI implementation is referred to as the "Oracle R2DBC
Driver" or "Oracle R2DBC" throughout the remainder of this document.

The Oracle R2DBC Driver implements behavior specified by the R2DBC 1.0.0.RELEASE 
[Specification](https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/)
and [Javadoc](https://r2dbc.io/spec/1.0.0.RELEASE/api/)

Publisher objects created by Oracle R2DBC implement behavior specified by
the Reactive Streams 1.0.3 [Specification](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.3/README.md)
and [Javadoc](http://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/package-summary.html)

The R2DBC and Reactive Streams specifications include requirements that are
optional for a compliant implementation. The remainder of this document specifies 
the Oracle R2DBC Driver's implementation of these optional requirements.

### Connection Creation
The Oracle R2DBC Driver is identified by the name "oracle". The driver 
implements a ConnectionFactoryProvider located by an R2DBC URL identifing
"oracle" as a driver, or by a DRIVER `ConnectionFactoryOption` with the value
of "oracle".

#### Support for Standard R2DBC Options
The following standard [ConnectionFactoryOptions](https://r2dbc.io/spec/1.0.0.RELEASE/api/io/r2dbc/spi/ConnectionFactoryOptions.html)
are supported by Oracle R2DBC: 
 - `DRIVER`
 - `HOST`
 - `PORT`
 - `DATABASE`
 - `USER`
 - `PASSWORD`
 - `SSL`
 - `CONNECT_TIMEOUT`
 - `STATEMENT_TIMEOUT`.

> Oracle R2DBC interprets the `DATABASE` option as the
> [service name](https://docs.oracle.com/en/database/oracle/oracle-database/21/netag/identifying-and-accessing-database.html#GUID-153861C1-16AD-41EC-A179-074146B722E6)
> of an Oracle Database instance. _System Identifiers (SID) are not recognized_.

#### Support for Extended R2DBC Options
Oracle R2DBC extends the standard set of R2DBC options to offer functionality 
that is specific to Oracle Database and the Oracle JDBC Driver. Extended options
are declared in the
[OracleR2dbcOptions](src/main/java/oracle/r2dbc/OracleR2dbcOptions.java)
class.

#### Configuring an Oracle Net Descriptor
The `oracle.r2dbc.OracleR2dbcOptions.DESCRIPTOR` option may be used to configure
an Oracle Net Descriptor of the form ```(DESCRIPTION=...)```. If this option is
used to configure a descriptor, then it is invalid to specify any 
other option that conflicts with information in the descriptor. Conflicting
options include `HOST`, `PORT`, `DATABASE`, and `SSL`. These options all 
conflict with information that appears in a descriptor.

The `DESCRIPTOR` option has the name `oracle.r2dbc.descriptor`. This name can 
be used to configure a descriptor in the query section of an R2DBC URL: 
```
r2dbc:oracle://?oracle.r2dbc.descriptor=(DESCRIPTION=...)
```
The `DESCRIPTOR` constant may also be used to configure a descriptor 
programmatically:
```java
ConnectionFactoryOptions.builder()
  .option(OracleR2dbcOptions.DESCRIPTOR, "(DESCRIPTION=...)")
```
The `DESCRIPTOR` option may be set to an aliased entry of a `tnsnames.ora` file.
Use the `TNS_ADMIN` option to specify the directory where `tnsnames.ora` is 
located:
```
r2dbc:oracle://?oracle.r2dbc.descriptor=myAlias&TNS_ADMIN=/path/to/tnsnames/
```

#### Configuring a java.util.concurrent.Executor
The `oracle.r2dbc.OracleR2dbcOptions.EXECUTOR` option configures a 
`java.util.concurrent.Executor` for executing asynchronous callbacks. The 
`EXECUTOR` option may be used to configure an `Executor` programmatically:
```java
ConnectionFactoryOptions.builder()
  .option(OracleR2dbcOptions.EXECUTOR, getExecutor())
```
> There is no way to configure an executor with a URL query parameter

If this option is not configured, then the common 
`java.util.concurrent.ForkJoinPool` is used as a default.
#### Configuring Oracle JDBC Connection Properties
A subset of Oracle JDBC's connection properties are also supported by Oracle 
R2DBC. These connection properties may be configured as options having the same
name as the Oracle JDBC connection property, and may have `CharSequence` value
types.

For example, the following URL configures the `oracle.net.wallet_location` 
connection property:
```
r2dbcs:oracle://db.host.example.com:1522/db.service.name?oracle.net.wallet_location=/path/to/wallet/
```
The same property can also be configured programmatically:
```java
 ConnectionFactoryOptions.builder()
  .option(OracleR2dbcOptions.TLS_WALLET_LOCATION, "/path/to/wallet")
```

The following is a list of all Oracle JDBC connection properties that are 
supported by Oracle R2DBC:
  - [oracle.net.tns_admin](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_TNS_ADMIN)
  - [oracle.net.wallet_location](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_WALLET_LOCATION)
  - [oracle.net.wallet_password](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_WALLET_PASSWORD)
  - [javax.net.ssl.keyStore](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORE)
  - [javax.net.ssl.keyStorePassword](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD)
  - [javax.net.ssl.keyStoreType](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE)
  - [javax.net.ssl.trustStore](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORE)
  - [javax.net.ssl.trustStorePassword](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD)
  - [javax.net.ssl.trustStoreType](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORETYPE)
  - [oracle.net.authentication_services](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES)
  - [oracle.net.ssl_certificate_alias](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_CERTIFICATE_ALIAS)
  - [oracle.net.ssl_server_dn_match](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_SERVER_DN_MATCH)
  - [oracle.net.ssl_server_cert_dn](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_SERVER_CERT_DN)
  - [oracle.net.ssl_version](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_VERSION)
  - [oracle.net.ssl_cipher_suites](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_CIPHER_SUITES)
  - [ssl.keyManagerFactory.algorithm](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_KEYMANAGERFACTORY_ALGORITHM)
  - [ssl.trustManagerFactory.algorithm](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_TRUSTMANAGERFACTORY_ALGORITHM)
  - [oracle.net.ssl_context_protocol](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_SSL_CONTEXT_PROTOCOL)
  - [oracle.jdbc.fanEnabled](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_FAN_ENABLED)
  - [oracle.jdbc.implicitStatementCacheSize](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE)
  - [oracle.jdbc.defaultLobPrefetchSize](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE)
  - [oracle.net.disableOob](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_NET_DISABLE_OUT_OF_BAND_BREAK)
    - Out of band (OOB) breaks effect statement timeouts. Set this to "true" if statement timeouts are not working correctly.
  - [oracle.jdbc.enableQueryResultCache](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_ENABLE_QUERY_RESULT_CACHE)
    - Cached query results can cause phantom reads even if the serializable
     transaction isolation level is set. Set this to "false" if using the
      serializable isolation level.
  - [v$session.terminal](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_TERMINAL)
  - [v$session.machine](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_MACHINE)
  - [v$session.osuser](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_OSUSER)
  - [v$session.program](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_PROGRAM)
  - [v$session.process](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_PROCESS)

### Thread Safety and Parallel Execution
Oracle R2DBC's `ConnectionFactory` and `ConnectionFactoryProvider` are the only 
classes that have a thread safe implementation. All other classes implemented 
by Oracle R2DBC are not thread safe. For instance, it is not safe for multiple
threads to concurrently access a single instance of `Result`.
> It is recommended to use a Reactive Streams library such as Project Reactor
> or RxJava to manage the consumption of non-thread safe objects

Oracle Database does not allow multiple database calls to execute in parallel
over a single `Connection`. If an attempt is made to execute a database call
before a previous call has completed, then Oracle R2DBC will enqueue that call
and only execute it after the previous call has completed.

To illustrate, the following code attempts to execute two statements in 
parallel:
```java
Flux.merge(
  connection.createStatement(
    "INSERT INTO example (id, value) VALUES (0, 'x')")
    .execute(),
  connection.createStatement(
    "INSERT INTO example (id, value) VALUES (1, 'y')")
    .execute())
```
When the publisher of the second statement is subscribed to, Oracle R2DBC will 
enqueue a task for sending that statement to the database. The enqueued task 
will only be executed after the publisher of the first statement has completed.

### Reactive Streams
Every method implemented by Oracle R2DBC that returns a Publisher has a JavaDoc
which specifies the Publisher's behavior with regard to deferred execution and 
support for multiple Subscribers.

Oracle R2DBC's implementation of Publishers that emit one or zero items will
typically defer execution until a Subscriber subscribes, support multiple 
Subscribers, and cache the result of a database call (the same result of the 
same call is emitted to each Subscriber).

Oracle R2DBC's implementation of Publishers that emit multiple items will 
typically defer execution until a Subscriber signals demand, and not support 
multiple subscribers.

### Errors
Oracle R2DBC creates R2dbcExceptions having the same ORA-XXXXX error codes 
used by Oracle Database and Oracle JDBC.

A reference for the ORA-XXXXX error codes can be found 
[here](https://docs.oracle.com/en/database/oracle/oracle-database/21/errmg/ORA-00000.html#GUID-27437B7F-F0C3-4F1F-9C6E-6780706FB0F6)

### Transactions
Oracle R2DBC uses READ COMMITTED as the default transaction isolation level.

Oracle R2DBC also supports the SERIALIZABLE isolation level. If SERIALIZABLE
isolation is configured, then the 
`oracle.r2dbc.OracleR2dbcOptions.ENABLE_QUERY_RESULT_CACHE` option must also be
configured as `false` to avoid phantom reads.

> READ COMMITTED and SERIALIZABLE are the only isolation levels supported by 
> Oracle Database

Oracle Database does not support a lock wait timeout that is configurable within
the scope of a transaction or session. Oracle R2DBC implements SPI methods that 
configure a lock wait timeout to throw ```UnsupportedOperationException```.

### Statements
Oracle R2DBC supports SQL execution with the `Statement` SPI.

#### Parameter Markers
A SQL command passed to `Connection.createStatement(String)` may include 
named parameter markers, unnamed parameter markers, or both.

Unnamed parameter markers may appear in SQL as a question mark
(`?`):
```java
connection.createStatement(
  "SELECT value FROM example WHERE id=?")
  .bind(0, 99)
```
The `bind` method must be called with a zero-based index to set the value of an
unnamed parameter.

Named parameter markers may appear in SQL as a colon character (`:`) followed by 
an alpha-numeric name:
```java
connection.createStatement(
  "SELECT value FROM example WHERE id=:id")
  .bind("id", 99)
```
The `bind` method may be called with a `String` valued name, or with zero-based
index, to set the value of a named parameter. Parameter names are
case-sensitive.

#### Batch Execution
The `Statement.add()` method may be used execute a DML command multiple times
with a batch of different bind values. Oracle Database only supports batch
execution for DML type SQL commands (INSERT/UPDATE/DELETE). Attempting to
execute a SELECT query with a batch of bind values will result in an error.

#### Returning Generated Values
The `Statement.returnGeneratedValues(String...)` method may be called to return 
generated values from basic forms of `INSERT` and `UPDATE` statements.

If an empty set of column names is passed to `returnGeneratedValues`, the 
`Statement` will return the
[ROWID](https://docs.oracle.com/en/database/oracle/oracle-database/21/cncpt/tables-and-table-clusters.html#GUID-0258C4C2-2BF2-445F-B1E1-F282A57A6859)
of each row affected by an INSERT or UPDATE.
> Programmers are advised not to use the ROWID as if it were a primary key.
> The ROWID of a row change, or be reassigned to a different row.
> See
> https://asktom.oracle.com/pls/apex/asktom.search?tag=is-it-safe-to-use-rowid-to-locate-a-row
> for more information.

Returning generated values is only supported for `INSERT` and `UPDATE` commands 
in which a `RETURNING INTO` clause would be valid. For example, if a table is 
declared as:
```sql
CREATE TABLE example (
  id NUMBER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  value VARCAHR(100))
```
Returning generated values is supported for the following statement:
```java
connection.createStatement(
  "INSERT INTO example(value) VALUES (:value)")
  .bind("value", "x")
  .returningGeneratedValues("id")
```
This statement is supported because the `INSERT` could be written to include a 
`RETURNING INTO` clause:
```sql
INSERT INTO example(value) VALUES (:value) RETURING id INTO :id
```
As a counter example, returning generated values is not supported for the 
following statement:
```java
connection.createStatement(
  "INSERT INTO example (value) SELECT 'y' FROM sys.dual")
  .returningGeneratedValues("id")
```
This statement is not supported because it can not be written to include a 
`RETURNING INTO` clause.

> The Oracle Database SQL Language Reference specifies the INSERT and UPDATE 
> commands for which a RETURNING INTO clause is supported.
> 
> For the INSERT syntax, see:
> https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/INSERT.html
> 
> For the UPDATE syntax, see:
> https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/UPDATE.html

#### Procedural Calls
The SQL string passed to ```Connection.createStatement(String)``` may execute a
PL/SQL call:
  ```java
  connection.createStatement("BEGIN sayHello(:name_in, :greeting_out); END;")
  ```
OUT parameters are registered by invoking 
`Statement.bind(int, Object)` or `Statement.bind(String, Object)`
 with an instance of ```io.r2dbc.spi.Parameter``` implementing the 
```io.r2dbc.spi.Parameter.Out``` marker interface:
```java
statement.bind("greeting_out", Parameters.out(R2dbcType.VARCHAR))
```
Likewise, an IN OUT parameter would be registered by invoking
`Statement.bind(int, Object)` or `Statement.bind(String, Object)`
with an instance of ```io.r2dbc.spi.Parameter``` implementing both the
`io.r2dbc.spi.Parameter.Out` and `io.r2dbc.spi.Parameter.In` marker interfaces.

OUT parameters are consumed by invoking `Result.map(Function)`:
```java
result.map(outParameters -> outParameters.get("greeting_out", String.class))
```
If a procedural call returns multiple results, the publisher returned by 
`Statement.execute()` emits one `Result` for each cursor returned by 
`DBMS_SQL.RETURN_RESULT` in the procedure. The order in which each 
`Result` is emitted corresponds to the order in which the procedure returns each
cursor.

If a procedure returns cursors, and also has out parameters, then the `Result` 
for the out parameters is emitted last, after the `Result` for each cursor.

### Type Mappings
Oracle R2DBC supports type mappings between Java and SQL for non-standard data 
types of Oracle Database.

| Oracle SQL Type                                                                                                                                         | Java Type |
|---------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| [JSON](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html#GUID-E441F541-BA31-4E8C-B7B4-D2FB8C42D0DF)                   |  `javax.json.JsonObject` or `oracle.sql.json.OracleJsonObject` |
| [DATE](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html#GUID-5405B652-C30E-4F4F-9D33-9A4CB2110F1B)                                                                                                                                                | `java.time.LocalDateTime` |
| [INTERVAL DAY TO SECOND](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html#GUID-B03DD036-66F8-4BD3-AF26-6D4433EBEC1C) | `java.time.Duration` |
| [INTERVAL YEAR TO MONTH](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html#GUID-ED59E1B3-BA8D-4711-B5C8-B0199C676A95) | `java.time.Period` |
> Unlike the standard SQL type named "DATE", the Oracle Database type named 
> "DATE" stores values for year, month, day, hour, minute, and second. The 
> standard SQL type only stores year, month, and day. LocalDateTime objects are able 
> to store the same values as a DATE in Oracle Database.

### BLOB, CLOB, and NCLOB
Oracle R2DBC allows large objects (LOBs) to be read and written as a reactive 
stream, or as a
fully materialized value.

#### Prefetched LOB Data
When a SQL query returns a LOB column,  only a portion of the LOB's content
is received in the response from Oracle Database. The portion received in the 
SQL query response is referred to as "prefetched data". Any content remaining
after the prefetched portion must be fetched with additional database calls. 

For example, if a SQL query returns a LOB that is 100MB in size, then the 
response might prefetch only the first 1MB of the LOB's content. Additional 
database calls would be required to fetch the remaining 99MB of content.

By default, Oracle R2DBC attempts to prefetch the entire content of a LOB. Oracle R2DBC will
request up to 1GB of prefetched data from Oracle Database when executing a SQL
query.

#### Materialzed Type Mapping
The `Row.get(...)` method allows LOB values to be mapped into materialized
types like `ByteBuffer` and `String`. If the entire LOB has been prefetched, 
then `Row.get(...)`  can return a `ByteBuffer/String` without any additional 
database calls. However, if the LOB value is larger than the prefetch size, then
`Row.get(...)` must execute a **blocking database call** to fetch the remainder of that value.

#### Streamed Type Mapping 
In a system that consumes very large LOBs, a very large amount of memory will be
consumed if the entire LOB is prefetched. When a LOB is too large to be 
prefetched entirely, a smaller prefetch size can be configured using the
[oracle.jdbc.defaultLobPrefetchSize](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE)
option, and the LOB can be consumed as a stream. By mapping LOB columns to 
`Blob` or `Clob` objects, the content can be consumed as a reactive stream. 

## Secure Programming Guidelines
The following security related guidelines should be adhered to when programming
with the Oracle R2DBC Driver.
### Defend Against SQL Injection Attacks
- Always specify the parameters of a SQL command using the bind methods of io.r2dbc.spi.Statement. 
  - Do not use String concatenation to specify parameters of a SQL command. 
  - Do not use format Strings to specify parameters of a SQL command.
### Protect Passwords
- Do not hard code passwords in your source code.
- Avoid hard coding passwords in the R2DBC URL.
  - When handling URL strings in code, be aware that a clear text password may appear in the user info section.
- Use a sensitive io.r2dbc.spi.Option to specify passwords.
  - If possible, specify the Option's value as an instance of java.nio.CharBuffer or java.lang.StringBuffer and clear the contents immediately after ConnectionFactories.get(ConnectionFactoryOptions) has returned. Oracle R2DBC's implementation of ConnectionFactory does not retain a reference to the clear text password.
### Protect Network Communications
- Use SSL/TLS if possible. Use any of the following methods to enable SSL/TLS:
  - Specify the boolean value of true for io.r2dbc.spi.ConnectionFactoryOptions.SSL
  - Specify "r2dbcs:" as the R2DBC URL schema.
  - Specify "ssl=true" in the query section of the R2DBC URL.
- Use Option.sensitiveValueOf(String) when creating an Option that specifies a password.
  - Option.sensitiveValueOf(OracleConnection.CONNECTION_PROPERTY_WALLET_PASSWORD)
    - An SSO wallet does not require a password.
  - Option.sensitiveValueOf(OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD)
  - Option.sensitiveValueOf(OracleConnection.CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD)
### Defend Against Denial-of-Service Attacks
- Use a connection pool and configure a maximum size to limit the number of database sessions created by ConnectionFactory.create()
- Enforce a maximum batch size to limit invocations of Statement.add() or Batch.add(String).
- Enforce a maximum fetch size to limit values supplied to Statement.fetchSize(int).
- Enforce a maximum buffer size to limit memory usage when reading Blob and Clob objects.
