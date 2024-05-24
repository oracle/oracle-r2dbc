# About Oracle R2DBC

The Oracle R2DBC Driver is a Java library that supports reactive programming with Oracle Database.

Oracle R2DBC implements the R2DBC Service Provider Interface (SPI) as specified
by the Reactive Relational Database Connectivity (R2DBC) project. The R2DBC SPI
exposes Reactive Streams as an abstraction for remote database operations.
Reactive Streams is a well defined standard for asynchronous, non-blocking, and
back-pressured communication. This standard allows an R2DBC driver to
interoperate with other reactive libraries and frameworks, such as Spring,
Project Reactor, RxJava, and Akka Streams.

### Learn More About R2DBC:
[R2DBC Project Home Page](https://r2dbc.io)

[R2DBC Javadocs v1.0.0.RELEASE](https://r2dbc.io/spec/1.0.0.RELEASE/api/)

[R2DBC Specification v1.0.0.RELEASE](https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/)

### Learn More About Reactive Streams:
[Reactive Streams Project Home Page](http://www.reactive-streams.org)

[Reactive Streams Javadocs v1.0.3](http://www.reactive-streams.org/reactive-streams-1.0.3-javadoc/org/reactivestreams/package-summary.html)

[Reactive Streams Specification v1.0.3](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.3/README.md)

# About This Version
The 1.2.0 release Oracle R2DBC implements version 1.0.0.RELEASE of the R2DBC SPI.

Fixes in this release:
 - [Fixed "Operator has been terminated" message](https://github.com/oracle/oracle-r2dbc/pull/134)
 - [Checking for Zero Threads in the common ForkJoinPool](https://github.com/oracle/oracle-r2dbc/pull/131)

New features in this release:
- [Supporting Option Values from Supplier and Publisher](https://github.com/oracle/oracle-r2dbc/pull/137)
- [Added Options for Kerberos](https://github.com/oracle/oracle-r2dbc/pull/127)

Updated dependencies:
- Updated Oracle JDBC from 21.7.0.0 to 21.11.0.0
- Updated Project Reactor from 3.5.0 to 3.5.11

## Installation
Oracle R2DBC can be obtained from Maven Central.
```xml
<dependency>
  <groupId>com.oracle.database.r2dbc</groupId>
  <artifactId>oracle-r2dbc</artifactId>
  <version>1.2.0</version>
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
- Project Reactor 3.5.11
- Oracle JDBC 21.11.0.0 for JDK 11 (ojdbc11.jar)
  - Oracle R2DBC relies on the Oracle JDBC Driver's [Reactive Extensions
  ](https://docs.oracle.com/en/database/oracle/oracle-database/23/jjdbc/jdbc-reactive-extensions.html#GUID-1C40C43B-3823-4848-8B5A-D2F97A82F79B) APIs.

The Oracle R2DBC Driver has been verified with Oracle Database versions 18, 19,
21, and 23.

### Integration with Spring and Other Libraries
Oracle R2DBC can only interoperate with libraries that support the 1.0.0.RELEASE
version of the R2DBC SPI. When using libraries like Spring and r2dbc-pool, be
sure to use a version which supports the 1.0.0.RELEASE of the SPI.

Oracle R2DBC depends on the JDK 11 build of Oracle JDBC 21.11.0.0. Other
libraries may depend on a different version of Oracle JDBC, and this version may
be incompatible. To resolve incompatibilities, it may be necessary to explicitly
declare the dependency in your project, ie:
```xml
<dependency>
    <groupId>com.oracle.database.jdbc</groupId>
    <artifactId>ojdbc11</artifactId>
    <version>21.11.0.0</version>
</dependency>
```

## Code Examples

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

The following method uses Project Reactor's
[Flux](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html)
to open a connection, execute a SQL query, and then close the connection:
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

## Help
For help programming with Oracle R2DBC, ask questions on Stack Overflow tagged with [[oracle] and [r2dbc]](https://stackoverflow.com/tags/oracle+r2dbc). The development team monitors Stack Overflow regularly.

Issues may be opened as described in [our contribution guide](CONTRIBUTING.md).

## Contributing

This project welcomes contributions from the community. Before submitting a pull
request, please [review our contribution guide](./CONTRIBUTING.md).

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security
vulnerability disclosure process.

## License

Copyright (c) 2021, 2023 Oracle and/or its affiliates.

This software is dual-licensed to you under the Universal Permissive License
(UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License
2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
either license.

## Documentation
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
optional for a compliant implementation. Oracle R2DBC's implementation of these
optional are specified in this document. This document also specifies additional
functionality that is supported by Oracle R2DBC, but is not part of the R2DBC
1.0.0 Specification.

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
   - The database option is interpreted as the
     [service name](https://docs.oracle.com/en/database/oracle/oracle-database/23/netag/identifying-and-accessing-database.html#GUID-153861C1-16AD-41EC-A179-074146B722E6)
      of an Oracle Database instance. _System Identifiers (SID) are not recognized_.
 - `USER`
 - `PASSWORD`
 - `SSL`
 - `CONNECT_TIMEOUT`
 - `STATEMENT_TIMEOUT`.
 - `PROTOCOL`
   - Accepted protocol values are "tcps", "ldap", and "ldaps"

#### Support for Extended R2DBC Options
Oracle R2DBC extends the standard set of R2DBC options to offer functionality 
that is specific to Oracle Database and the Oracle JDBC Driver. Extended options
are declared in the
[OracleR2dbcOptions](src/main/java/oracle/r2dbc/OracleR2dbcOptions.java) class.

#### Support for Supplier and Publisher as Option Values
Most options can have a value provided by a `Supplier` or `Publisher`.

Oracle R2DBC requests the value of an `Option` from a `Supplier` or `Publisher`
each time the `Publisher` returned by `ConnectionFactory.create()` creates a new
`Connection`. Each `Connection` can then be configured with values that change 
over time, such as a password which is periodically rotated.

If a `Supplier` provides the value of an `Option`, then Oracle R2DBC requests 
the value by invoking `Supplier.get()`. If `get()` returns `null`,
then no value is configured for the `Option`. If `get()` throws a
`RuntimeException`, then it is set as the initial cause of an
`R2dbcException` emitted by the `Publisher` returned by
`ConnectionFactory.create()`. The `Supplier` must have a thread safe `get()` 
method, as multiple subscribers may request connections concurrently.

If a `Publisher` provides the value of an `Option`, then Oracle R2DBC requests
the value by subscribing to the `Publisher` and signalling demand.
The first value emitted to `onNext` will be used as the value of the `Option`.
If the `Publisher` emits `onComplete` before `onNext`, then no value is
configured for the `Option`. If the `Publisher` emits `onError` before `onNext`, 
then the `Throwable` is set as the initial cause of an
`R2dbcException` emitted by the `Publisher` returned by
`ConnectionFactory.create()`.

The following example configures the `PASSWORD` option with a `Supplier`:
```java
  void configurePassword(ConnectionFactoryOptions.Builder optionsBuilder) {
  
    // Cast the PASSWORD option
    Option<Supplier<CharSequence>> suppliedOption = OracleR2dbcOptions.supplied(PASSWORD);
    
    // Supply a password
    Supplier<CharSequence> supplier = () -> getPassword();
    
    // Configure the builder
    optionsBuilder.option(suppliedOption, supplier); 
  }
```
A more concise example configures `TLS_WALLET_PASSWORD` as a `Publisher`
```java
  void configurePassword(ConnectionFactoryOptions.Builder optionsBuilder) {
    optionsBuilder.option(
      OracleR2dbcOptions.published(TLS_WALLET_PASSWORD),
      Mono.fromSupplier(() -> getWalletPassword()));
  }
```
These examples use the `supplied(Option)` and `published(Option)` methods 
declared by `oracle.r2dbc.OracleR2dbcOptions`. These methods cast an `Option<T>`
to `Option<Supplier<T>>` and `Option<Publisher<T>>`, respectively. It is 
necessary to cast the generic type of the `Option` when calling
`ConnectionFactoryOptions.Builder.option(Option<T>, T)` in order for the call to
compile and not throw a `ClassCastException` at runtime. It is not strictly 
required that `supplied(Option)` or `published(Option)` be used to cast the 
`Option`. These methods are only meant to offer code readability and
convenience.

Note that the following code would compile, but fails at runtime with a
`ClassCastException`:
```java
  void configurePassword(ConnectionFactoryOptions.Builder optionsBuilder) {
    Publisher<CharSequence> publisher = Mono.fromSupplier(() -> getPassword());
    // Doesn't work. Throws ClassCastException at runtime:
    optionsBuilder.option(PASSWORD, PASSWORD.cast(publisher));
  }
```
To avoid a `ClassCastException`, the generic type of an `Option` must match the
actual type of the value passed to 
`ConnectionFactoryOptions.Builder.option(Option<T>, T)`.

For a small set of options, providing values with a `Supplier` or `Publisher` 
is not supported:
- `DRIVER`
- `PROTOCOL`

Providing values for these options would not be interoperable with
`io.r2dbc.spi.ConnectionFactories` and `r2dbc-pool`.

Normally, Oracle R2DBC will not retain references to `Option` values after
`ConnectionFactories.create(ConnectionFactoryOptions)` returns. However, if
the value of at least one `Option` is provided by a `Supplier` or `Publisher`, 
then Oracle R2DBC will retain a reference to all `Option` values until the 
`ConnectionFactory.create()` `Publisher` emits a `Connection` or error. This is
important to keep in mind when `Option` values may be mutated. In particular,
a password may only be cleared from memory after the `create()` `Publisher` 
emits a `Connection` or error.

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

#### Configuring an LDAP URL
Use `ldap` or `ldaps` as the URL protocol to have an Oracle Net Descriptor 
retrieved from an LDAP server:
```
r2dbc:oracle:ldap://ldap.example.com:7777/sales,cn=OracleContext,dc=com
r2dbc:oracle:ldaps://ldap.example.com:7778/sales,cn=OracleContext,dc=com
```
Use a space separated list of LDAP URIs for fail over and load balancing:
```
r2dbc:oracle:ldap://ldap1.example.com:7777/sales,cn=OracleContext,dc=com%20ldap://ldap2.example.com:7777/sales,cn=OracleContext,dc=com%20ldap://ldap3.example.com:7777/sales,cn=OracleContext,dc=com
```
> Space characters in a URL must be percent encoded as `%20`

An LDAP server request will **block a thread for network I/O** when Oracle R2DBC
creates a new connection.

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

The next sections list Oracle JDBC connection properties which are supported by
Oracle R2DBC.

##### TLS/SSL Connection Properties
  - [oracle.net.tns_admin](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_TNS_ADMIN)
  - [oracle.net.wallet_location](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_WALLET_LOCATION)
  - [oracle.net.wallet_password](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_WALLET_PASSWORD)
  - [javax.net.ssl.keyStore](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORE)
  - [javax.net.ssl.keyStorePassword](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTOREPASSWORD)
  - [javax.net.ssl.keyStoreType](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_KEYSTORETYPE)
  - [javax.net.ssl.trustStore](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORE)
  - [javax.net.ssl.trustStorePassword](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTOREPASSWORD)
  - [javax.net.ssl.trustStoreType](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_JAVAX_NET_SSL_TRUSTSTORETYPE)
  - [oracle.net.authentication_services](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_SERVICES)
  - [oracle.net.ssl_certificate_alias](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_CERTIFICATE_ALIAS)
  - [oracle.net.ssl_server_dn_match](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_SERVER_DN_MATCH)
  - [oracle.net.ssl_server_cert_dn](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_SERVER_CERT_DN)
  - [oracle.net.ssl_version](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_VERSION)
  - [oracle.net.ssl_cipher_suites](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_CIPHER_SUITES)
  - [ssl.keyManagerFactory.algorithm](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_KEYMANAGERFACTORY_ALGORITHM)
  - [ssl.trustManagerFactory.algorithm](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_SSL_TRUSTMANAGERFACTORY_ALGORITHM)
  - [oracle.net.ssl_context_protocol](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_SSL_CONTEXT_PROTOCOL)

##### Miscellaneous Connection Properties
  - [oracle.jdbc.fanEnabled](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_FAN_ENABLED)
  - [oracle.jdbc.implicitStatementCacheSize](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_IMPLICIT_STATEMENT_CACHE_SIZE)
  - [oracle.jdbc.defaultLobPrefetchSize](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE)
  - [oracle.net.disableOob](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_THIN_NET_DISABLE_OUT_OF_BAND_BREAK)
    - Out of band (OOB) breaks effect statement timeouts. Set this to "true" if
      statement timeouts are not working correctly. OOB breaks are a
    - [requirement for pipelining](#requirements-for-pipelining)
  - [oracle.jdbc.enableQueryResultCache](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_ENABLE_QUERY_RESULT_CACHE)
    - Cached query results can cause phantom reads even if the serializable
      transaction isolation level is set. Set this to "false" if using the
      serializable isolation level.
  - [oracle.jdbc.timezoneAsRegion](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_TIMEZONE_AS_REGION)
    - Setting this option to "false" may resolve "ORA-01882: timezone region not
      found". The ORA-01882 error happens when Oracle Database doesn't recognize
      the name returned by `java.util.TimeZone.getDefault().getId()`.

##### Database Tracing Connection Properties
  - [v$session.terminal](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_TERMINAL)
  - [v$session.machine](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_MACHINE)
  - [v$session.osuser](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_OSUSER)
  - [v$session.program](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_PROGRAM)
  - [v$session.process](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_VSESSION_PROCESS)

##### Oracle Net Encryption Connection Properties
  - [oracle.net.encryption_client](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_NET_ENCRYPTION_LEVEL)
  - [oracle.net.encryption_types_client](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_NET_ENCRYPTION_TYPES)
  - [oracle.net.crypto_checksum_client](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_NET_CHECKSUM_LEVEL)
  - [oracle.net.crypto_checksum_types_client](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_NET_CHECKSUM_TYPES)

##### Kerberos Connection Properties
  - [oracle.net.kerberos5_cc_name](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_KRB5_CC_NAME)
  - [oracle.net.kerberos5_mutual_authentication](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_KRB5_MUTUAL)
  - [oracle.net.KerberosRealm](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_KRB_REALM)
  - [oracle.net.KerberosJaasLoginModule](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_NET_AUTHENTICATION_KRB_JAAS_LOGIN_MODULE)

##### LDAP Connection Properties
  - [oracle.net.ldap.security.authentication](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SECURITY_AUTHENTICATION)
  - [oracle.net.ldap.security.principal](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SECURITY_PRINCIPAL)
  - [oracle.net.ldap.security.credentials](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SECURITY_CREDENTIALS)
  - [com.sun.jndi.ldap.connect.timeout](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_JNDI_LDAP_CONNECT_TIMEOUT)
  - [com.sun.jndi.ldap.read.timeout](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_JNDI_LDAP_READ_TIMEOUT)
  - [oracle.net.ldap.ssl.walletLocation](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_WALLET_LOCATION)
  - [oracle.net.ldap.ssl.walletPassword](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_WALLET_PASSWORD)
  - [oracle.net.ldap.ssl.keyStoreType](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE_TYPE)
  - [oracle.net.ldap.ssl.keyStore](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE)
  - [oracle.net.ldap.ssl.keyStorePassword](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYSTORE_PASSWORD)
  - [oracle.net.ldap.ssl.trustStoreType](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE_TYPE)
  - [oracle.net.ldap.ssl.trustStore](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE)
  - [oracle.net.ldap.ssl.trustStorePassword](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTSTORE_PASSWORD)
  - [oracle.net.ldap.ssl.supportedCiphers](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_CIPHER_SUITES)
  - [oracle.net.ldap.ssl.supportedVersions](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_VERSIONS)
  - [oracle.net.ldap.ssl.keyManagerFactory.algorithm](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_KEYMANAGER_FACTORY_ALGORITHM)
  - [oracle.net.ldap.ssl.trustManagerFactory.algorithm](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_TRUSTMANAGER_FACTORY_ALGORITHM)
  - [oracle.net.ldap.ssl.ssl_context_protocol](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html#CONNECTION_PROPERTY_THIN_LDAP_SSL_CONTEXT_PROTOCOL)

### Thread Safety
Oracle R2DBC's `ConnectionFactory` and `ConnectionFactoryProvider` are the only 
classes that have a thread safe implementation. All other classes implemented 
by Oracle R2DBC are not thread safe. For instance, it is not safe for multiple
threads to concurrently access a single instance of `Result`.
> It is recommended to use a Reactive Streams library such as Project Reactor
> or RxJava to manage the consumption of non-thread safe objects

While it is not safe for multiple threads to concurrently access the _same_ 
object, it is safe from them to do so with _different_ objects from the _same_
`Connection`. For example, two threads can concurrently subscribe to two
`Statement` objects from the same `Connection`. When this happens, the two 
statements are executed in a "pipeline". Pipelining will be covered in the next
section.

### Pipelining
Pipelining allows Oracle R2DBC to send a call without having to wait for a previous call
to complete. [If all requirements are met](#requirements-for-pipelining), then
pipelining will be activated by concurrently subscribing to publishers 
from the same connection. For example, the following code concurrently
subscribes to two statements:
```java
Flux.merge(
  connection.createStatement(
    "INSERT INTO example (id, value) VALUES (0, 'X')")
    .execute(),
  connection.createStatement(
    "INSERT INTO example (id, value) VALUES (1, 'Y')")
    .execute())
```
When the `Publisher` returned by `merge` is subscribed to, both INSERTs are 
immediately sent to the database. The network traffic can be visualized as:
```
TIME | ORACLE R2DBC     | NETWORK | ORACLE DATABASE
-----+------------------+---------+-----------------
   0 | Send INSERT-X    | ------> | WAITING
   0 | Send INSERT-Y    | ------> | WAITING
   1 | WAITING          | <------ | Send Result-X
   1 | WAITING          | <------ | Send Result-Y
   2 | Receive Result-X |         | WAITING
   2 | Receive Result-Y |         | WAITING

```
In this visual, 1 unit of TIME is required to transfer data over the
network. The TIME column is only measuring network latency. It does not include 
computational time spent on executing the INSERTs.

The key takeaway from this visual is that the INSERTs are sent and 
received _concurrently_, rather than _sequentially_. Both INSERTs are sent at
TIME=0, and both are received at TIME=1. And, the results are both sent at TIME=1,
and are received at TIME=2. 

> Recall that TIME is not measuring computational time. If each action by Oracle
> R2DBC and Oracle Database requires 0.1 units of computational TIME, then we 
> can say:
> 
> INSERTs are sent at TIME=0.1 and TIME=0.2, and are received at TIME=1.1 and
> TIME=1.2. And, the results are sent at TIME=1.2 and
> TIME=1.3, and are received at TIME=2.2 and TIME=2.3. 
> 
> This is a bit more complicated to think about, but it is important to keep in 
> mind. All database calls will require at least some computational time.

Below is another visual of the network traffic, but in this case the INSERTs are
sent and received _without pipelining_:
```
TIME | ORACLE R2DBC     | NETWORK | ORACLE DATABASE
-----+------------------+---------+-----------------
   0 | Send INSERT-X    | ------> | WAITING
   1 | WAITING          | <------ | Send Result-X
   2 | Receive Result-X |         | WAITING
   2 | Send INSERT-Y    | ------> | WAITING
   3 | WAITING          | <------ | Send Result-Y
   4 | Receive Result-Y |         | WAITING

```
This visual shows a _sequential_ process of sending and receiving. It can be
compared to the _concurrent_ process seen in the previous visual. In both cases,
Oracle R2DBC and Oracle Database have the same number of WAITING actions. These
actions are waiting for network transfers. And in both cases, each network 
transfer requires 1 unit of TIME.

So if network latency is the same, and the number of
WAITING actions are the same (,and the
computational times are the same), then how are these INSERTs completing in less
TIME with pipelining? The answer is that _pipelining allowed the
network transfer times to be waited for __concurrently___.

In the first visual, with pipelining, the database waits for _both_ INSERT-X and
INSERT-Y at TIME=0. Compare that to the second visual, without pipelining, where
the database waits for INSERT-X at TIME=0, and then _waits again_ for INSERT-Y
at TIME=2. That's 1 additional unit of TIME when compared to pipelining. The 
other additional unit of TIME happens on the Oracle R2DBC side. Without 
pipelining, it waits for Result-X at TIME=1, and then _waits again_ for Result-Y
at TIME=3. With pipelining, it _waits for both results concurrently_ at TIME=1.

### Requirements for Pipelining

There are some requirements which must be met in order to use pipelining. As
explained in the previous section, the availability of pipelining can have a
significant impact on performance. Users should review the requirements listed
in this section when developing applications that rely on this performance gain.

In terms of functional behavior, the availability of pipelining will have no
impact: With or without it, the same database calls are going be executed. Users
who are not relying on pipelining performance do not necessarily need to review 
the requirements listed in this section. Oracle JDBC is designed to 
automatically check for these requirements, and it will fallback to using
sequential network transfers if any requirement is not met.

#### Product Versions
Pipelining is only available with Oracle Database version 23.4 or newer. It also
requires an Oracle JDBC version of 23.4 or newer, but this is already a 
transitive dependency of Oracle R2DBC.

#### Out Of Band Breaks
Pipelining requires out-of-band (OOB) breaks (ie: TCP urgent data) for cancelling 
statement execution. The Oracle JDBC Driver automatically checks if OOB is 
available, and will disable pipelining if it is not. The availability of OOB may 
depend on the operating system where Oracle R2DBC is running. Notably, _OOB is 
not available on Mac OS_ (or at least not available in the way which Oracle JDBC
needs it to be for sending TCP urgent data to Oracle Database).

__For experimentation only__, Mac OS users can choose to by-pass the OOB 
requirement by setting a JVM system property:
```
-Doracle.jdbc.disablePipeline=false
```
Bypassing the OOB requirement on Mac OS will result in non-functional 
implementations of `Connection.setStatementTimeout(Duration)`, and 
`Subscription.cancel()` for a `Subscription` from `Statement.execute()`.

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

### Errors and Warnings
Oracle R2DBC creates R2dbcExceptions having the same ORA-XXXXX error codes 
used by Oracle Database and Oracle JDBC. The
[Database Error Messages](https://docs.oracle.com/en/database/oracle/oracle-database/23/errmg/ORA-00000.html#GUID-27437B7F-F0C3-4F1F-9C6E-6780706FB0F6)
document provides a reference for all ORA-XXXXX error codes.

Warning messages from Oracle Database are emitted as 
`oracle.r2dbc.OracleR2dbcWarning` segments. These segments may be consumed using
`Result.flatMap(Function)`:
```java
result.flatMap(segment -> {
  if (segment instanceof OracleR2dbcWarning) {
    logWarning(((OracleR2dbcWarning)segment).getMessage());
    return emptyPublisher();
  }
  else if (segment instanceof Result.Message){
    ... handle an error ...
  }
  else {
    ... handle other segment types ...
  }
})
```
Unlike the errors of standard `Result.Message` segments, if a warning is not
consumed by `flatMap`, then it will be silently discarded when a `Result` is 
consumed using the `map` or `getRowsUpdated` methods.

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
[ROWID](https://docs.oracle.com/en/database/oracle/oracle-database/23/cncpt/tables-and-table-clusters.html#GUID-0258C4C2-2BF2-445F-B1E1-F282A57A6859)
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
> https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/INSERT.html
> 
> For the UPDATE syntax, see:
> https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/UPDATE.html

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

| Oracle SQL Type                                                                                                                                         | Java Type                                                     |
|---------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------|
| [JSON](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html#GUID-E441F541-BA31-4E8C-B7B4-D2FB8C42D0DF)                   | `javax.json.JsonObject` or `oracle.sql.json.OracleJsonObject` |
| [DATE](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html#GUID-5405B652-C30E-4F4F-9D33-9A4CB2110F1B)                   | `java.time.LocalDateTime`                                     |
| [INTERVAL DAY TO SECOND](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html#GUID-B03DD036-66F8-4BD3-AF26-6D4433EBEC1C) | `java.time.Duration`                                          |
| [INTERVAL YEAR TO MONTH](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html#GUID-ED59E1B3-BA8D-4711-B5C8-B0199C676A95) | `java.time.Period`                                            |
| [SYS_REFCURSOR](https://docs.oracle.com/en/database/oracle/oracle-database/23/lnpls/static-sql.html#GUID-470A7A99-888A-46C2-BDAF-D4710E650F27)          | `io.r2dbc.spi.Result`                                         |
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
[oracle.jdbc.defaultLobPrefetchSize](https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleConnection.html?is-external=true#CONNECTION_PROPERTY_DEFAULT_LOB_PREFETCH_SIZE)
option, and the LOB can be consumed as a stream. By mapping LOB columns to 
`Blob` or `Clob` objects, the content can be consumed as a reactive stream. 

### ARRAY
Oracle Database supports `ARRAY` as a user defined type only. A `CREATE TYPE` 
command is used to define an `ARRAY` type:
```sql
CREATE TYPE MY_ARRAY AS ARRAY(8) OF NUMBER
```
Oracle R2DBC defines `oracle.r2dbc.OracleR2dbcType.ArrayType` as a `Type` for
representing user defined `ARRAY` types. A `Parameter` with a type of
`ArrayType` must be used when binding array values to a `Statement`.
```java
Publisher<Result> arrayBindExample(Connection connection) {
  Statement statement =
    connection.createStatement("INSERT INTO example VALUES (:array_bind)");

  // Use the name defined for an ARRAY type:
  // CREATE TYPE MY_ARRAY AS ARRAY(8) OF NUMBER
  ArrayType arrayType = OracleR2dbcTypes.arrayType("MY_ARRAY");
  Integer[] arrayValues = {1, 2, 3};
  statement.bind("arrayBind", Parameters.in(arrayType, arrayValues));

  return statement.execute();
}
```
A `Parameter` with a type of `ArrayType` must also be used when binding OUT
parameters of a PL/SQL call.
```java
Publisher<Result> arrayOutBindExample(Connection connection) {
  Statement statement =
    connection.createStatement("BEGIN; exampleCall(:array_bind); END;");

  // Use the name defined for an ARRAY type:
  // CREATE TYPE MY_ARRAY AS ARRAY(8) OF NUMBER
  ArrayType arrayType = OracleR2dbcTypes.arrayType("MY_ARRAY");
  statement.bind("arrayBind", Parameters.out(arrayType));

  return statement.execute();
}
```
`ARRAY` values may be consumed from a `Row` or `OutParameter` as a Java array.
The element type of the Java array may be any Java type that is supported as
a mapping for the SQL type of the `ARRAY`. For instance, if the `ARRAY` type is
`NUMBER`, then a `Integer[]` mapping is supported:
```java
Publisher<Integer[]> arrayMapExample(Result result) {
  return result.map(readable -> readable.get("arrayValue", Integer[].class));
}
```

### OBJECT
Oracle Database supports `OBJECT` as a user defined type. A `CREATE TYPE`
command is used to define an `OBJECT` type:
```sql
CREATE TYPE PET AS OBJECT(
  name VARCHAR(128),
  species VARCHAR(128),
  weight NUMBER,
  birthday DATE)
```
Oracle R2DBC defines `oracle.r2dbc.OracleR2dbcType.ObjectType` as a `Type` for
representing user defined `OBJECT` types. A `Parameter` with a type of
`ObjectType` may be used to bind `OBJECT` values to a `Statement`.

Use an `Object[]` to bind the attribute values of an `OBJECT` by index:
```java
Publisher<Result> objectArrayBindExample(Connection connection) {
  Statement statement =
    connection.createStatement("INSERT INTO petTable VALUES (:petObject)");

  // Bind the attributes of the PET OBJECT defined above
  ObjectType objectType = OracleR2dbcTypes.objectType("PET");
  Object[] attributeValues = {
    "Derby",
    "Dog",
    22.8,
    LocalDate.of(2015, 11, 07)
  };
  statement.bind("petObject", Parameters.in(objectType, attributeValues));

  return statement.execute();
}
```

Use a `Map<String,Object>` to bind the attribute values of an `OBJECT` by name:
```java
Publisher<Result> objectMapBindExample(Connection connection) {
  Statement statement =
    connection.createStatement("INSERT INTO petTable VALUES (:petObject)");

  // Bind the attributes of the PET OBJECT defined above
  ObjectType objectType = OracleR2dbcTypes.objectType("PET");
  Map<String,Object> attributeValues = Map.of(
    "name", "Derby",
    "species", "Dog",
    "weight", 22.8,
    "birthday", LocalDate.of(2015, 11, 07));
  statement.bind("petObject", Parameters.in(objectType, attributeValues));

  return statement.execute();
}
```
A `Parameter` with a type of `ObjectType` must be used when binding OUT
parameters of `OBJECT` types for a PL/SQL call:
```java
Publisher<Result> objectOutBindExample(Connection connection) {
  Statement statement =
    connection.createStatement("BEGIN; getPet(:petObject); END;");

  ObjectType objectType = OracleR2dbcTypes.objectType("PET");
  statement.bind("petObject", Parameters.out(objectType));

  return statement.execute();
}
```
`OBJECT` values may be consumed from a `Row` or `OutParameter` as an
`oracle.r2dbc.OracleR2dbcObject`. The `OracleR2dbcObject` interface is a subtype
of `io.r2dbc.spi.Readable`. Attribute values may be accessed using the standard
`get` methods of `Readable`. The `get` methods of `OracleR2dbcObject` support
all SQL to Java type mappings defined by the
[R2DBC Specification](https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/#datatypes.mapping):
```java
Publisher<Pet> objectMapExample(Result result) {
  return result.map(row -> {
    OracleR2dbcObject oracleObject = row.get(0, OracleR2dbcObject.class); 
    return new Pet(
      oracleObject.get("name", String.class),
      oracleObject.get("species", String.class),
      oracleObject.get("weight", Float.class),
      oracleObject.get("birthday", LocalDate.class));
  });
}
```

Instances of `OracleR2dbcObject` may be passed directly to `Statement` bind 
methods:
```java
Publisher<Result> objectBindExample(
  OracleR2dbcObject oracleObject, Connection connection) {
  Statement statement =
    connection.createStatement("INSERT INTO petTable VALUES (:petObject)");
  
  statement.bind("petObject", oracleObject);

  return statement.execute();
}
```
Attribute metadata is exposed by the `getMetadata` method of
`OracleR2dbcObject`:
```java
void printObjectMetadata(OracleR2dbcObject oracleObject) {
  OracleR2dbcObjectMetadata metadata = oracleObject.getMetadata();
  OracleR2dbcTypes.ObjectType objectType = metadata.getObjectType();
  
  System.out.println("Object Type: " + objectType);
  metadata.getAttributeMetadatas()
    .stream()
    .forEach(attributeMetadata -> {
      System.out.println("\tAttribute Name: " + attributeMetadata.getName()));
      System.out.println("\tAttribute Type: " + attributeMetadata.getType()));
    });
}
```

### REF Cursor
Use the `oracle.r2dbc.OracleR2dbcTypes.REF_CURSOR` type to bind `SYS_REFCURSOR` out 
parameters:
```java
Publisher<Result> executeProcedure(Connection connection) {
  connection.createStatement(
    "BEGIN example_procedure(:cursor_parameter); END;")
  .bind("cursor_parameter", Parameters.out(OracleR2dbcTypes.REF_CURSOR))
  .execute()
}
```
A `SYS_REFCURSOR` out parameter can be mapped to an `io.r2dbc.spi.Result`:
```java
Publisher<Result> mapOutParametersResult(Result outParametersResult) {
  return outParametersResult.map(outParameters ->
    outParameters.get("cursor_parameter", Result.class));
}
```
The rows of a `SYS_REFCURSOR` may be consumed from the `Result` it is 
mapped to:
```java
Publisher<ExampleObject> mapRefCursorRows(Result refCursorResult) {
  return refCursorResult.map(row ->
    new ExampleObject(
      row.get("id_column", Long.class),
      row.get("value_column", String.class)));
}
```

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
