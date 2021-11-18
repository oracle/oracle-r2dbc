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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.test.TestKit;
import oracle.jdbc.datasource.OracleDataSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.IntStream;

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
 * overridden test method describes why it must be overridden when interacting
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

  /**
   * {@inheritDoc}
   * <p>
   * Override the default implementation to extract multiple update counts
   * from a single {@code result} and return a {@code Mono} that emits the
   * sum of all update counts.
   * </p>
   */
  @Override
  public Mono<Integer> extractRowsUpdated(Result result) {
    return Flux.from(result.getRowsUpdated())
      .reduce(0, (total, updateCount) -> total + updateCount);
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
   * Overrides the default implementation to expect 10 {@link io.r2dbc.spi.Result.UpdateCount}
   * segments from a single {@code Result}. The default implementation expects
   * 10 {@code Result}s each with a single {@code UpdateCount}. Batch DML
   * execution is a single call to Oracle Database, and so Oracle R2DBC
   * returns a single {@code Result}
   * </p>
   */
  @Override
  @Test
  public void prepareStatement() {
    Flux.usingWhen(getConnectionFactory().create(),
      connection -> {
        Statement statement = connection.createStatement(expand(TestStatement.INSERT_VALUE_PLACEHOLDER, getPlaceholder(0)));

        IntStream.range(0, 10)
          .forEach(i -> {
            TestKit.bind(statement, getIdentifier(0), i);

            if (i != 9) {
              statement.add();
            }
          });

        // The original TestKit implementation is modified below to call
        // Result.getRowsUpdated(), which returns a Publisher of 10
        // UpdateCount segments.
        return Flux.from(statement
          .execute())
          .flatMap(Result::getRowsUpdated);
      },
      Connection::close)
      .as(StepVerifier::create)
      .expectNextCount(10).as("values from insertions")
      .verifyComplete();
  }

  @Disabled("Compound statements are not supported by Oracle Database")
  @Test
  @Override
  public void compoundStatement() {}

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

