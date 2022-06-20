![](https://github.com/oracle/oracle-r2dbc/actions/workflows/build-and-test.yml/badge.svg)

# About Oracle R2DBC

The Oracle R2DBC Driver is a Java library that supports reactive programming with Oracle Database.

Oracle R2DBC implements the R2DBC Service Provider Interface (SPI) as specified by the Reactive Relational Database Connectivity (R2DBC) project. The R2DBC SPI exposes Reactive Streams as an abstraction for remote database operations. Reactive Streams is a well defined standard for asynchronous, non-blocking, and back-pressured communication. This standard allows an R2DBC driver to interoperate with other reactive libraries and frameworks, such as Spring, Project Reactor, RxJava, and Akka Streams.

### Learn More About R2DBC:
[R2DBC Project Home Page](https://r2dbc.io)

[R2DBC Javadocs v0.9.0.RELEASE](https://r2dbc.io/spec/0.9.0.RELEASE/api/)

[R2DBC Specification v0.9.0.RELEASE](https://r2dbc.io/spec/0.9.0.RELEASE/spec/html/)

### Learn More About Reactive Streams:
[Reactive Streams Project Home Page](http://www.reactive-streams.org)

[Reactive Streams Javadocs v1.0.3](http://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/package-summary.html)

[Reactive Streams Specification v1.0.3](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.3/README.md)
# About This Version
The 0.4.0 release Oracle R2DBC implements version 0.9.0.RELEASE of the R2DBC SPI.

Bug fixes included in this release:
- Resolved a stack overflow that occcured when reading JSON columns
- Using asynchronous lock acquisition to avoid contention for JDBC's lock

Functionality added in this release:
- Support for SERIALIZABLE transaction isolation
- Support for transaction savepoints
- Support for R2dbcException.getSql()

API changes in this release:
- Addition of oracle.r2dbc.OracleR2dbcOptions, a class that declares Oracle R2DBC's extended Options
- Addition of oracle.r2dbc.OracleR2dbcOptions.EXECUTOR, an Option for configuring a non-default Executor
- Renamed the "oracleNetDescriptor" Option to "oracle.r2dbc.descriptor"
- Statement.add() results in an IllegalStateException if bind values are not set afterwards 

### Spring Integration
Use the 0.1.0 version of Oracle R2DBC if you are programming with Spring.
The later versions of Oracle R2DBC implement the 0.9.x versions of the R2DBC
 SPI. Currently, Spring only supports drivers that implement the 0.8.x versions
 of the SPI.
 
### Performance Goals
The primary goal of these early releases of Oracle R2DBC is to support the R2DBC
 SPI on Oracle Database. The only performance goal is to enable concurrent
 database calls to be executed by a single thread.

The R2DBC SPI and Oracle's implementation are both pre-production. As these
 projects mature we will shift our development focus from implementing
 the SPI to optimizing the implementation.


# Installation
Oracle R2DBC can be built from source using Maven:

`mvn clean install -DskipTests=true`

> Omitting -DskipTests=true from the command above will execute the test suite, where end-to-end tests connect to an Oracle Database instance. The connection configuration is read from [src/test/resources/config.properties](src/test/resources/example-config.properties).

Artifacts can also be found on Maven Central.
```
<dependency>
  <groupId>com.oracle.database.r2dbc</groupId>
  <artifactId>oracle-r2dbc</artifactId>
  <version>0.4.0</version>
</dependency>
```

Oracle R2DBC is compatible with JDK 11 (or newer), and has the following runtime dependencies:
- R2DBC SPI 0.9.0.RELEASE
- Reactive Streams 1.0.3
- Project Reactor 3.3.0.RELEASE
- Oracle JDBC 21.3.0.0 for JDK 11 (ojdbc11.jar)
  - Oracle R2DBC relies on the Oracle JDBC Driver's [Reactive Extensions
  ](https://docs.oracle.com/en/database/oracle/oracle-database/21/jjdbc/jdbc-reactive-extensions.html#GUID-1C40C43B-3823-4848-8B5A-D2F97A82F79B) APIs.

The Oracle R2DBC Driver has been verified with Oracle Database versions 18, 19,
 and 21.

# Code Examples

The following code example uses the Oracle R2DBC Driver with Project Reactor's [Mono](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html) and [Flux](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html) types to open a database connection and execute a SQL query:
```java
ConnectionFactory connectionFactory = ConnectionFactories.get(
  "r2dbc:oracle://db.example.com:1521/db.service.name");

Mono.from(connectionFactory.create())
  .flatMapMany(connection ->
    Flux.from(connection.createStatement(
      "SELECT 'Hello, Oracle' FROM sys.dual")
      .execute())
      .flatMap(result ->
        result.map(row -> row.get(0, String.class)))
      .doOnNext(System.out::println)
      .thenMany(connection.close()))
  .subscribe();
```
When executed, the code above will _asynchronously_ print the result of the SQL query.

The next example includes a named parameter marker, ":locale_name", in the SQL command:
```java
Mono.from(connectionFactory.create())
  .flatMapMany(connection ->
    Flux.from(connection.createStatement(
      "SELECT greeting FROM locale WHERE locale_name = :locale_name")
      .bind("locale_name", "France")
      .execute())
      .flatMap(result ->
        result.map(row ->
          String.format("%s, Oracle", row.get("greeting", String.class))))
      .doOnNext(System.out::println)
      .thenMany(connection.close()))
  .subscribe();
```
Like the previous example, executing the code above will _asynchronously_ print a greeting message. "France" is set as the bind value for locale_name, so the query should return a greeting like "Bonjour" when row.get("greeting") is called.

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
This document specifies the behavior of the R2DBC SPI implemented for the 
Oracle Database. This SPI implementation is referred to as the "Oracle R2DBC
Driver" or "Oracle R2DBC" throughout the remainder of this document.

The Oracle R2DBC Driver implements behavior specified by the R2DBC 0.9.0.RELEASE 
[Specification](https://r2dbc.io/spec/0.9.0.RELEASE/spec/html/)
and [Javadoc](https://r2dbc.io/spec/0.9.0.RELEASE/api/)

Publisher objects created by Oracle R2DBC implement behavior specified by
the Reactive Streams 1.0.3 [Specification](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.3/README.md)
and [Javadoc](http://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/package-summary.html)

The R2DBC and Reactive Streams specifications include requirements that are
optional for a compliant implementation. The remainder of this document specifies 
the Oracle R2DBC Driver's implementation of these optional requirements.

### Connection Creation
- The Oracle R2DBC Driver is identified by the name "oracle". The driver 
implements a ConnectionFactoryProvider located by an R2DBC URL identifing
"oracle" as a driver, or by a DRIVER ConnectionFactoryOption with the value
of "oracle".
- The following well-known ConnectionFactory Options are supported: 
`DRIVER`, `USER`, `PASSWORD`, `HOST`, `PORT`, `DATABASE`, `SSL`, 
`CONNECT_TIMEOUT`, `STATEMENT_TIMEOUT`.
- The `DATABASE` `ConnectionFactoryOption` is interpreted as the 
[service name](https://docs.oracle.com/en/database/oracle/oracle-database/21/netag/identifying-and-accessing-database.html#GUID-153861C1-16AD-41EC-A179-074146B722E6) of an Oracle Database instance. 
System Identifiers (SID) are not recognized.
- A subset of Oracle JDBC's connection properties are supported as extended
options. Extended options that configure Oracle JDBC connection properties are declared in `oracle.r2dbc.OracleR2dbcOptions`. These options all have the same name as their corresponding Oracle JDBC connection property, and will accept a `CharSequence` value:
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
- Oracle Net Descriptors of the form ```(DESCRIPTION=...)``` may be specified using `oracle.r2dbc.OracleR2dbcOptions.DESCRIPTOR`.
  - If a descriptor is specified, then it is invalid to specify any other options that might conflict with information in the descriptor, such as: `HOST`, `PORT`, `DATABASE`, and `SSL`.
  - The `DESCRIPTOR` option has the name `oracle.r2dbc.descriptor`, and this may appear in the query section of an R2DBC URL: `r2dbc:oracle://?oracle.r2dbc.descriptor=(DESCRIPTION=...)`
  - The `DESCRIPTOR` option may be provided programmatically: 
  ```java
  ConnectionFactoryOptions.builder().option(OracleR2dbcOptions.DESCRIPTOR, "(DESCRIPTION=...)")
  ```
  - The `DESCRIPTOR` option may be set to an aliased entry of a tnsnames.ora file. The directory of tnsnames.ora may be set using an option with the name `TNS_ADMIN`: `r2dbc:oracle://?oracle.r2dbc.descriptor=myAlias&TNS_ADMIN=/path/to/tnsnames/`
- A `java.util.concurrent.Executor` to use for executing asynchronous callbacks may specified using `oracle.r2dbc.OracleR2dbcOptions.EXECUTOR`.
  - The `EXECUTOR` option can only be set programmatically, it can not be set in the query section of an R2DBC URL:
  ```java
  Executor myExecutor = getMyExecutor();
  ConnectionFactoryOptions options = ConnectionFactoryOptions.builder()
    .option(ConnectionFactoryOptions.DRIVER, "oracle")
    .option(OracleR2dbcOptions.EXECUTOR, myExecutor)
    ...
    .build();
  ```

### Thread Safety and Parallel Execution
- Oracle R2DBC's `ConnectionFactory` and `ConnectionFactoryProvider` are thread safe.
- All other SPI implementations are not thread safe.
- Executing parallel database calls is not supported over a single `Connection`. 
If a thread attempts to initiate a parallel call, that call will be enqueued. The enqueued call will not be executed until the connection is no longer executing any other call. This is a limitation of the Oracle Database, which does not support parallel calls within a single session.

### Reactive Streams
- The Oracle R2DBC javadoc of every method that returns a Publisher specifies the 
behavior of that Publisher in regards to deferred execution and multiple Subscribers.
- Typically, a Publisher of one or zero items defers execution until a Subscriber 
subscribes, supports multiple Subscribers, and caches the result of a database call
(the same result of the same call is emitted to each Subscriber).
- Typically, a Publisher of multiple items defers execution until a Subscriber 
signals demand, and does not support multiple subscribers.

### Errors
- The error code of an R2dbcException is an [Oracle Database 
or Oracle JDBC Driver error message](https://docs.oracle.com/en/database/oracle/oracle-database/21/errmg/ORA-00000.html#GUID-27437B7F-F0C3-4F1F-9C6E-6780706FB0F6)

### Transactions
- READ COMMITTED is the default transaction isolation level
- SERIALIZABLE is the only isolation level, besides READ COMMITED, that
 Oracle Database supports.
  - To avoid phantom reads, configure `oracle.r2dbc.OracleR2dbcOptions.ENABLE_QUERY_RESULT_CACHE` as `false` when using SERIALIZABLE isolation. 
- Oracle Database does not support a lock wait timeout that is configurable
 within the scope of a transaction or session. SPI methods that configure a
  lock wait timeout throw ```UnsupportedOperationException```

### Statements
- Batch execution is only supported for DML type SQL commands (INSERT/UPDATE/DELETE).
- SQL commands may contain JDBC style parameter markers where question
mark characters (?) designate unnamed parameters. A numeric index must
be used when setting the bind value of an unnamed parameter.
- SQL commands may contain named parameter markers where the
colon character (:) is followed by an alphanumeric parameter name. A name
or numeric index may be used when setting the bind value of a named parameter.
- Parameter names are case-sensitive.
- When an empty set of column names is specified to Statement.returnGeneratedValues(String...), executing that ```Statement``` returns the [ROWID](https://docs.oracle.com/en/database/oracle/oracle-database/21/cncpt/tables-and-table-clusters.html#GUID-0258C4C2-2BF2-445F-B1E1-F282A57A6859) 
of each row affected by an INSERT or UPDATE.
  - This behavior may change in a later release.
  - Programmers are advised not to use the ROWID as if it were a primary key.
    - The ROWID of a row may change.
    - After a row is deleted, its ROWID may be reassigned to a new row.
    - Further Reading: https://asktom.oracle.com/pls/apex/asktom.search?tag=is-it-safe-to-use-rowid-to-locate-a-row
- Returning generated values is only supported for INSERT and UPDATE commands when a RETURNING INTO clause can be appended to the end of that command. (This limitation may be resolved in a later release)
  - Example: `INSERT INTO my_table(val) VALUES (:val)` is supported because a RETURNING INTO clause may be appended to this command.
  - Example: `INSERT INTO my_table(val) SELECT 1 FROM sys.dual` is not supported because a RETURNING INTO clause may not be appended to this command.
  - The Oracle Database SQL Language Reference defines INSERT and UPDATE commands for which a RETURNING INTO clause is supported.
  - INSERT: https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/INSERT.html#GUID-903F8043-0254-4EE9-ACC1-CB8AC0AF3423
  - UPDATE: https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/UPDATE.html#GUID-027A462D-379D-4E35-8611-410F3AC8FDA5

### Procedural Calls
- Use ```Connection.createStatement(String)``` to create a
 ```Statement``` that executes a PL/SQL call:
  ```java
  connection.createStatement("BEGIN sayHello(:name_in, :greeting_out); END;")
  ```
- Register out parameters by invoking ```Statement.bind(int/String, Object)```
 with an instance of ```io.r2dbc.spi.Parameter``` implementing the 
  ```io.r2dbc.spi.Parameter.Out``` marker interface:
  ```java
  statement.bind("greeting_out", Parameters.out(R2dbcType.VARCHAR))
  ```
- Register in out parameters  by invoking
 ```Statement.bind(int/String, Object)``` with an instance of
  ```io.r2dbc.spi.Parameter``` implementing both the
   ```io.r2dbc.spi.Parameter.Out``` and 
   ```io.r2dbc.spi.Parameter.In``` marker interfaces.
- Consume out parameters by invoking
 ```Result.map(Function)```:
  ```java
  result.map(outParameters -> outParameters.get("greeting_out", String.class))
  ```
- ```Statement.execute()``` returns a ```Publisher<Result>``` that emits one
 ```Result``` for each cursor returned by ```DBMS_SQL.RETURN_RESULT```
    - The order in which a ```Result``` is emitted for a cursor
     corresponds to the order in which the procedure returns each cursor.
    - If a procedure returns cursors and also has out parameters, then the 
    ```Result``` for out parameters is emitted last, after the
    ```Result``` for each returned cursor.

### Type Mappings
- `javax.json.JsonObject` and `oracle.sql.json.OracleJsonObject` are supported as 
Java type mappings for `JSON` column values.
- `java.time.Duration` is supported as a Java type mapping for `INTERVAL DAY TO SECOND` 
column values.
- `java.time.Period` is supported as a Java type mapping for `INTERVAL YEAR TO MONTH` 
column values.
- `java.time.LocalDateTime` is supported as a Java type mapping for `DATE` column values.
The Oracle Database type named "DATE" stores the same information as a `LocalDateTime`:
year, month, day, hour, minute, and second.

### BLOB, CLOB, and NCLOB
When a SQL query returns a LOB value, a
portion of that value is prefetched from the database and the remaining portion
must be fetched with additional database calls. The number of prefetched
bytes is configured by an `Option` named [oracle.jdbc.defaultLobPrefetchSize](https://docs.oracle.com/en/database/oracle/oracle-database/21/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE)
. The default value of this `Option` is 1 GB.
 
The `Row.get(...)` method allows LOB values to be mapped into materialized
types like `ByteBuffer` and `String`. If the prefetch size is large
enough to have fetched the entire LOB value, then `Row.get(...)`  can
return a `ByteBuffer/String` without any additional database calls.
Otherwise, if the LOB value is larger than the prefetch size, then
`Row.get(...)`  must execute a **blocking database call** to fetch the
remainder of that value. 

For systems in which LOB values are too large to be prefetched, a smaller
prefetch size can be configured, and LOB values may be mapped into `Blob`
or `Clob` objects rather than `ByteBuffer` or `String`. `Blob`
and `Clob` objects allow the LOB value to be streamed using non-blocking
database calls.

# Secure Programming Guidelines
The following security guidelines should be followed when programming with the Oracle R2DBC Driver.
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
