/*
  Reactive Relational Database Connectivity
  Copyright 2017-2018 the original author or authors.

  Copyright (c) 2020, 2021, Oracle and/or its affiliates.

  This software is dual-licensed to you under the Universal Permissive License 
  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License
  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
  either license.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package oracle.r2dbc.test;

import io.r2dbc.spi.*;
import io.r2dbc.spi.test.TestKit;
import oracle.jdbc.datasource.OracleDataSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.support.AbstractLobCreatingPreparedStatementCallback;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import static io.r2dbc.spi.ConnectionFactoryOptions.DATABASE;
import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
import static io.r2dbc.spi.ConnectionFactoryOptions.HOST;
import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
import static io.r2dbc.spi.ConnectionFactoryOptions.PORT;
import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
import static oracle.r2dbc.test.DatabaseConfig.host;
import static oracle.r2dbc.test.DatabaseConfig.password;
import static oracle.r2dbc.test.DatabaseConfig.port;
import static oracle.r2dbc.test.DatabaseConfig.serviceName;
import static oracle.r2dbc.test.DatabaseConfig.user;

/**
 * <p>
 * Subclass implementation of the R2DBC {@link TestKit} for Oracle Database.
 * This test kit implementation overrides super class test methods that are
 * fundamentally incompatible with Oracle Database. The javadoc of each
 * overridden test method describes why it must be overriden when interacting
 * with an Oracle Database.
 * </p><p>
 * The developers of the Oracle R2DBC Driver are mindful of the fact that
 * distributing a non-compliant implementation of the SPI would create
 * confusion about what behavior is to be expected from an R2DBC driver. To
 * avoid this confusion, we exercised our best judgement when determining if
 * it would be acceptable to override any test case of the R2DBC SPI TCK. It
 * should only be acceptable to do so when the behavior verified by the
 * overriding method would still be correct according the written specification
 * of the R2DBC SPI.
 * </p><p>
 * If you think that our judgement was incorrect, then we strongly encourage
 * you to bring this to our attention. The easiest way to contact us is through
 * GitHub.
 * </p>
 *
 * @author  harayuanwang, Michael-A-McMahon
 * @since   0.1.0
 */
public class OracleTestKit implements TestKit<Integer> {

  private final JdbcOperations jdbcOperations;
  {
    try {
      OracleDataSource dataSource = new oracle.jdbc.pool.OracleDataSource();
      dataSource.setURL(String.format("jdbc:oracle:thin:@%s:%d/%s",
        host(), port(), serviceName()));
      dataSource.setUser(user());
      dataSource.setPassword(password());
      this.jdbcOperations = new JdbcTemplate(dataSource);
    }
    catch (SQLException sqlException) {
      throw new RuntimeException(sqlException);
    }
  }

  private final ConnectionFactory connectionFactory;
  {
    connectionFactory = ConnectionFactories.get(
      ConnectionFactoryOptions.builder()
        .option(DRIVER, "oracle")
        .option(DATABASE, serviceName())
        .option(HOST, host())
        .option(PORT, port())
        .option(PASSWORD, password())
        .option(USER, user())
        .build());
  }

  public JdbcOperations getJdbcOperations() {
    return jdbcOperations;
  }

  @Override
  public ConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Overrides the standard {@link TestKit} implementation for compatibility
   * with Oracle Database.
   * </p><p>
   * Oracle Database converts unquoted alias values to uppercase. This
   * conflicts with the {@link #rowMetadata()} test where unquoted aliases are
   * expected to retain their original lower case values. To resolve this
   * conflict, the SQL statement returned for
   * {@link TestStatement#SELECT_VALUE_ALIASED_COLUMNS} enquotes the alias
   * names that appear in in the {@code SELECT} statement.
   * </p>
   * @param statement
   * @return
   */
  @Override
  public String doGetSql(TestStatement statement) {
    switch (statement) {
      case SELECT_VALUE_ALIASED_COLUMNS:
        // Enquote alias names to retain their lower case values
        return "SELECT col1 AS \"b\", col1 AS \"c\", col1 AS \"a\"" +
          " FROM test_two_column";
      case SELECT_VALUE:
        // Use ORDER BY to return rows in a consistent order
        return "SELECT value FROM test ORDER BY value";
      case CREATE_TABLE_AUTOGENERATED_KEY:
        return "CREATE TABLE test (" +
          "id NUMBER GENERATED ALWAYS AS IDENTITY, value NUMBER)";
      case INSERT_VALUE_AUTOGENERATED_KEY:
        return "INSERT INTO test(value) VALUES(100)";
      default:
        return statement.getSql();
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Overrides the standard {@link TestKit} implementation for compatibility
   * with Oracle Database.
   * </p><p>
   * Oracle Database does not support the {@code INTEGER} datatype declared
   * with {@link TestStatement#CREATE_TABLE},
   * {@link TestStatement#CREATE_TABLE_AUTOGENERATED_KEY}, and
   * {@link TestStatement#CREATE_TABLE_TWO_COLUMNS}. When the {@code INTEGER}
   * type appears in these {@code CREATE TABLE} statements, Oracle Database
   * creates columns of type {@code NUMBER(38)}, as specified in Oracle's
   * <a href="https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/Data-Types.html#GUID-0BC16006-32F1-42B1-B45E-F27A494963FF">
   * SQL Language Reference
   * </a>. Oracle R2DBC converts NUMBER values into {@link BigDecimal}
   * objects, and this conflicts with the {@code TestKit} verifications that
   * expect the standard mapping of {@code INTEGER} to {@link Integer}. The
   * {@code extractColumn} method is overridden to resolve this conflict by
   * converting {@code BigDecimal} objects to {@code int}.
   * </p>
   */
  @Override
  public Object extractColumn(Row row) {
    return extractColumn("value", row);
  }

  /**
   * Extracts the value of a column identified by {@code name} from a
   * {@code row}. The extracted value is converted as specified by
   * {@link OracleTestKit#extractColumn(Row)}.
   * @param name Column name
   * @return Column value
   */
  private Object extractColumn(String name, Row row) {
    Object value = row.get(name);

    if (value instanceof BigDecimal)
      return ((BigDecimal)value).intValue();
    else
      return value;
  }

  @Override
  public String getPlaceholder(int index) {
    return String.format(":%d", index + 1);
  }

  @Override
  public Integer getIdentifier(int index) {
    return index;
  }


  /**
   * {@inheritDoc}
   * <p>
   * Overrides the default implementation to use
   * {@link #extractColumn(String, Row)} rather than {@link Row#get(String)}.
   * This override is necessary because {@link Row#get(String)} returns
   * an instance of {@link BigDecimal} for columns declared as {@code INTEGER}
   * by {@link TestStatement#INSERT_TWO_COLUMNS}. See
   * {@link OracleTestKit#extractColumn(Row)} for details.
   * </p><p>
   * This override does not prevent this test from verifying a case-insensitive
   * name match is implemented by Row.get(String) when duplicate column names
   * are present.
   */
  @Test
  public void duplicateColumnNames() {
    getJdbcOperations().execute(expand(TestStatement.INSERT_TWO_COLUMNS));

    Mono.from(getConnectionFactory().create())
      .flatMapMany(connection -> Flux.from(connection

        .createStatement(expand(TestStatement.SELECT_VALUE_TWO_COLUMNS))
        .execute())

        .flatMap(result -> result
          .map((row, rowMetadata) -> Arrays.asList(
            extractColumn("value", row), extractColumn("VALUE", row))))
        .flatMapIterable(Function.identity())

        .concatWith(close(connection)))
      .as(StepVerifier::create)
      .expectNext(100).as("value from col1")
      .expectNext(100).as("value from col1 (upper case)")
      .verifyComplete();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Overrides the default implementation to expect ColumnMetadata.getName() to
   * return the column's alias in all UPPERCASE letters. The default
   * implementation of this test in
   * {@link TestKit#columnMetadata()} expects getName() to return the alias
   * as it appears in the SQL command, where it is all lower case.
   * </p><p>
   * This override is necessary because the Oracle Database describes aliased
   * columns with the alias converted to all UPPERCASE characters. This
   * description does not provide enough information for the Oracle R2DBC
   * Driver to determine the case of characters as they appeared in the
   * original SQL command.
   * </p><p>
   * This override does not prevent this test from verifying that
   * ColumnMetadata.getName() returns the alias (except not in the original
   * character case), or from verifying case insensitive name matching with
   * RowMetadata.getColumnMetadata(String) and
   * RowMMetadata.getColumnNames().contains(Object)
   * </p>
   */
  @Test
  @Override
  public void columnMetadata() {
    getJdbcOperations().execute("INSERT INTO test_two_column VALUES (100, 'hello')");

    Mono.from(getConnectionFactory().create())
      .flatMapMany(connection -> Flux.from(connection

        .createStatement("SELECT col1 AS value, col2 AS value FROM test_two_column")
        .execute())
        .flatMap(result -> {
          return result.map((row, rowMetadata) -> {
            Collection<String> columnNames = rowMetadata.getColumnNames();
            return Arrays.asList(rowMetadata.getColumnMetadata("value").getName(), rowMetadata.getColumnMetadata("VALUE").getName(), columnNames.contains("value"), columnNames.contains(
              "VALUE"));
          });
        })
        .flatMapIterable(Function.identity())
        .concatWith(close(connection)))
      .as(StepVerifier::create)
      // Note the overridden behavior below: Expect alias "value" to be ALL CAPS
      .expectNext("VALUE").as("Column label col1")
      .expectNext("VALUE").as("Column label col1 (get by uppercase)")
      .expectNext(true).as("getColumnNames.contains(value)")
      .expectNext(true).as("getColumnNames.contains(VALUE)")
      .verifyComplete();
  }

  /**
   * {@inheritDoc}
   * <p>
   * Overrides the default implementation to expect {@link Clob} as the default
   * Java type mapping for CLOB columns. The default implementation of this
   * test in {@link TestKit#duplicateColumnNames()} expects the R2DBC
   * Specification's default type mapping guideline for CLOB columns, which
   * is java.lang.String.
   * </p><p>
   * Mapping {@code BLOB/CLOB} to {@code ByteBuffer/String} can not be
   * supported because the Oracle Database allows LOBs to store terabytes of
   * data. If the Oracle R2DBC Driver were to fully materialize a LOB
   * prior to emitting this row, the amount of memory necessary to do so
   * might exceed the capacity of {@code ByteBuffer/String}, and could even
   * exceed the amount of memory available to the Java Virtual Machine.
   * </p><p>
   * This override does not prevent this test from verifying the behavior of a
   * Clob returned from a SQL select query.
   * </p>
   */
  @Test
  public void clobSelect() {
    getJdbcOperations().execute("INSERT INTO clob_test VALUES (?)", new AbstractLobCreatingPreparedStatementCallback(new DefaultLobHandler()) {

      @Override
      protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
        lobCreator.setClobAsString(ps, 1, "test-value");
      }

    });

    // CLOB as String is not supported
    Mono.from(getConnectionFactory().create())
      .flatMapMany(connection -> Flux.from(connection

        .createStatement("SELECT * from clob_test")
        .execute())
        .flatMap(result -> result
          // Note the overridden behavior below: String conversion is an error
          .map((row, rowMetadata) -> {
            try {
              row.get("value", String.class);
              return "Returns normally";
            }
            catch (R2dbcException expected) {
              return "Throws R2dbcException";
            }
          }))

        .concatWith(close(connection)))
      .as(StepVerifier::create)
      .expectNext("Throws R2dbcException").as("get CLOB as String")
      .verifyComplete();

    // CLOB consume as Clob
    Mono.from(getConnectionFactory().create())
      .flatMapMany(connection -> Flux.from(connection

        .createStatement("SELECT * from clob_test")
        .execute())
        .flatMap(result -> result
          // Note the overridden behavior below: The default mapping is Clob
          .map((row, rowMetadata) -> (Clob)row.get("value")))
        .flatMap(clob -> Flux.from(clob.stream())
          .reduce(new StringBuilder(), StringBuilder::append)
          .map(StringBuilder::toString)
          .concatWith(TestKit.discard(clob)))

        .concatWith(close(connection)))
      .as(StepVerifier::create)
      .expectNext("test-value").as("value from select")
      .verifyComplete();
  }


  /**
   * {@inheritDoc}
   * <p>
   * Overrides the default implementation to expect Blob as the default Java
   * type mapping for BLOB columns. The default implementation of this
   * test in {@link TestKit#duplicateColumnNames()} expects the R2DBC
   * Specification's default type mapping guideline for BLOB columns,
   * which is java.nio.ByteBuffer.
   * </p><p>
   * Mapping {@code BLOB/CLOB} to {@code ByteBuffer/String} can not be
   * supported because the Oracle Database allows LOBs to store terabytes of
   * data. If the Oracle R2DBC Driver were to fully materialize a LOB
   * prior to emitting this row, the amount of memory necessary to do so
   * might exceed the capacity of {@code ByteBuffer/String}, and could even
   * exceed the amount of memory available to the Java Virtual Machine.
   * </p><p>
   * This override does not prevent this test from verifying the behavior of a
   * Blob returned from a SQL select query.
   * </p>
   */
  @Test
  public void blobSelect() {
    getJdbcOperations().execute("INSERT INTO blob_test VALUES (?)", new AbstractLobCreatingPreparedStatementCallback(new DefaultLobHandler()) {

      @Override
      protected void setValues(PreparedStatement ps, LobCreator lobCreator) throws SQLException {
        lobCreator.setBlobAsBytes(ps, 1, StandardCharsets.UTF_8.encode("test-value").array());
      }

    });

    // BLOB as ByteBuffer is not supported
    Mono.from(getConnectionFactory().create())
      .flatMapMany(connection -> Flux.from(connection

        .createStatement("SELECT * from blob_test")
        .execute())

        .flatMap(result -> result
          // Note the overridden behavior below: ByteBuffer conversion is not
          // supported
          .map((row, rowMetadata) -> {
            try {
              row.get("value", ByteBuffer.class);
              return "Returns normally";
            }
            catch (R2dbcException expected) {
              return "Throws R2dbcException";
            }
          }))
        .concatWith(close(connection)))
      .as(StepVerifier::create)
      .expectNext("Throws R2dbcException").as("get BLOB as ByteBuffer")
      .verifyComplete();

    // BLOB as Blob
    Mono.from(getConnectionFactory().create())
      .flatMapMany(connection -> Flux.from(connection

        .createStatement("SELECT * from blob_test")
        .execute())
        .flatMap(result -> result
          .map((row, rowMetadata) -> row.get("value", Blob.class)))
        .flatMap(blob -> Flux.from(blob.stream())
          .reduce(ByteBuffer::put)
          .concatWith(TestKit.discard(blob)))

        .concatWith(close(connection)))
      .as(StepVerifier::create)
      .expectNextMatches(actual -> {
        ByteBuffer expected = StandardCharsets.UTF_8.encode("test-value");
        return Arrays.equals(expected.array(), actual.array());
      })
      .verifyComplete();
  }

  @Disabled("Compound statements are not supported by Oracle Database")
  @Test
  @Override
  public void compoundStatement() {}

  @Disabled("Disabled until savepoint is implemented")
  @Test
  @Override
  public void savePoint() {}

  @Disabled("Disabled until savepoint is implemented")
  @Test
  @Override
  public void savePointStartsTransaction() {}

  static <T> Mono<T> close(Connection connection) {
    return Mono.from(connection.close())
      .then(Mono.empty());
  }

}

/*
   MODIFIED             (MM/DD/YY)
    Michael-A-McMahon   09/22/20 - Blob and Clob tests
    harayuanwang        05/12/20 - Creation
 */

