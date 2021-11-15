/*
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

package oracle.r2dbc.impl;

import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcRollbackException;
import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.R2dbcTransientException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.util.ArrayList;
import java.util.List;

import static oracle.r2dbc.impl.OracleR2dbcExceptions.fromJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.requireNonNull;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.runJdbc;
import static oracle.r2dbc.impl.OracleR2dbcExceptions.toR2dbcException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that
 * {@link OracleR2dbcExceptions} implements behavior that is specified in it's
 * class and method level javadocs.
 */
public class OracleR2dbcExceptionsTest {

  /**
   * Verifies the implementation of
   * {@link OracleR2dbcExceptions#fromJdbc(OracleR2dbcExceptions.JdbcSupplier)} ()}
   */
  @Test
  public void testGetOrHandleSqlException() {

    // Expect a supplied value when the supplier doesn't throw
    Object expected = new Object();
    assertSame(expected, fromJdbc(() -> expected));

    // Expect a thrown SQLException to be handled by throwing an
    // R2dbcException.
    IOException ioException = new IOException("IO-MESSAGE");
    SQLException sqlException = new SQLException(
      "SQL-MESSAGE", "SQL-STATE", 9, ioException);
    R2dbcException r2dbcException = assertThrows(R2dbcException.class, () ->
      fromJdbc(() -> {
          throw sqlException;
        }));

    // Expect the R2dbcException to have the same message, sql state, and error
    // code as the SQLException
    assertEquals(sqlException.getMessage(), r2dbcException.getMessage());
    assertEquals(sqlException.getSQLState(), r2dbcException.getSqlState());
    assertEquals(sqlException.getErrorCode(), r2dbcException.getErrorCode());

    // Expect the R2dbcException to have the SQLException as it's initial cause
    assertSame(sqlException, r2dbcException.getCause());

    // Expect the R2dbcException to leave the initial cause of the SQLException
    // intact
    assertSame(ioException, r2dbcException.getCause().getCause());
  }

  /**
   * Verifies the implementation of
   * {@link OracleR2dbcExceptions#runJdbc(OracleR2dbcExceptions.JdbcRunnable)}
   */
  @Test
  public void testRunOrHandleSqlException() {

    // Expect a runnable to be ran
    Object expected = new Object();
    List<Object> list = new ArrayList<>();
    runJdbc(() -> list.add(expected));
    assertSame(expected, list.get(0));

    // Expect a thrown SQLException to be handled by throwing an
    // R2dbcException.
    IOException ioException = new IOException("IO-MESSAGE");
    SQLException sqlException = new SQLException(
      "SQL-MESSAGE", "SQL-STATE", 9, ioException);
    R2dbcException r2dbcException = assertThrows(R2dbcException.class, () ->
      runJdbc(() -> {
        throw sqlException;
      }));

    // Expect the R2dbcException to have the same message, sql state, and error
    // code as the SQLException
    assertEquals(sqlException.getMessage(), r2dbcException.getMessage());
    assertEquals(sqlException.getSQLState(), r2dbcException.getSqlState());
    assertEquals(sqlException.getErrorCode(), r2dbcException.getErrorCode());

    // Expect the R2dbcException to have the SQLException as it's initial cause
    assertSame(sqlException, r2dbcException.getCause());

    // Expect the R2dbcException to leave the initial cause of the SQLException
    // intact
    assertSame(ioException, r2dbcException.getCause().getCause());
  }

  /**
   * Verifies the implementation of
   * {@link OracleR2dbcExceptions#newNonTransientException(String, String, Throwable)}
   */
  @Test
  public void testNewNonTransientException() {
    IOException ioException = new IOException("IO-MESSAGE");
    SQLException sqlException = new SQLException(
      "SQL-MESSAGE", "SQL-STATE", 9, ioException);
    String message = "MESSAGE";
    R2dbcNonTransientException r2dbcException =
      OracleR2dbcExceptions.newNonTransientException(
        message, null, sqlException);

    // Expect the R2dbcException to have the same message
    assertSame(message, r2dbcException.getMessage());

    // Expect the R2dbcException to have the SQLException as it's initial cause
    assertSame(sqlException, r2dbcException.getCause());

    // Expect the R2dbcException to leave the initial cause of the SQLException
    // intact
    assertSame(ioException, r2dbcException.getCause().getCause());
  }

  /**
   * Verifies the implementation of
   * {@link OracleR2dbcExceptions#requireNonNull(Object, String)}
   */
  @Test
  public void testRequireNonNull() {
    // Expect the input object to be returned if it is not null
    Object expected = new Object();
    assertSame(expected, requireNonNull(expected, "object can not null"));

    // Expect an IllegalArgumentException having a specified message to be
    // thrown if the input object is null
    String message = "The argument is null!";
    assertSame(message, assertThrows(
        IllegalArgumentException.class, () -> requireNonNull(null, message))
      .getMessage());

  }

  /**
   * Verifies the implementation of
   * {@link OracleR2dbcExceptions#toR2dbcException(SQLException)}
   */  
  @Test
  public void testToR2dbcException() {
    // Expect a SQLSyntaxErrorException to be mapped as an R2dbcBadGrammarException
    verifyR2dbcExceptionMapping(
      new SQLSyntaxErrorException("message", "state", 99,
        new IOException("io failure")),
      R2dbcBadGrammarException.class);

    // Expect a SQLIntegrityConstraintViolationException to be mapped as an
    // R2dbcDataIntegrityViolationException
    verifyR2dbcExceptionMapping(
      new SQLIntegrityConstraintViolationException("message", "state", 99,
        new IOException("io failure")),
      R2dbcDataIntegrityViolationException.class);

    // Expect a SQLNonTransientConnectionException to be mapped as an
    // R2dbcNonTransientResourceException
    verifyR2dbcExceptionMapping(
      new SQLNonTransientConnectionException("message", "state", 99,
        new IOException("io failure")),
      R2dbcNonTransientResourceException.class);

    // Expect a SQLNonTransientException to be mapped as an R2dbcNonTransientException
    verifyR2dbcExceptionMapping(
      new SQLNonTransientException("message", "state", 99,
        new IOException("io failure")),
      R2dbcNonTransientException.class);

    // Expect a SQLTimeoutException to be mapped as an R2dbcTimeoutException
    verifyR2dbcExceptionMapping(
      new SQLTimeoutException("message", "state", 99,
        new IOException("io failure")),
      R2dbcTimeoutException.class);

    // Expect a SQLTransactionRollbackException to be mapped as an
    // R2dbcRollbackException
    verifyR2dbcExceptionMapping(
      new SQLTransactionRollbackException("message", "state", 99,
        new IOException("io failure")),
      R2dbcRollbackException.class);

    // Expect a SQLTransientConnectionException to be mapped as an
    // R2dbcTransientResourceException
    verifyR2dbcExceptionMapping(
      new SQLTransientConnectionException("message", "state", 99,
        new IOException("io failure")),
      R2dbcTransientResourceException.class);

    // Expect a SQLTransientException to be mapped as an R2dbcTransientException
    verifyR2dbcExceptionMapping(
      new SQLTransientException("message", "state", 99,
        new IOException("io failure")),
      R2dbcTransientException.class);

    // Expect a SQLRecoverableException to be mapped as an
    // R2dbcTransientResourceException
    verifyR2dbcExceptionMapping(
      new SQLRecoverableException("message", "state", 99,
        new IOException("io failure")),
      R2dbcTransientResourceException.class);

    // Expect a SQLException to be mapped as an R2dbcException
    verifyR2dbcExceptionMapping(
      new SQLException("message", "state", 99,
        new IOException("io failure")),
      R2dbcException.class);

    // Expect a non-standard SQLException subtype to be mapped as an
    // R2dbcException
    class TestSqlException extends SQLException {
      public TestSqlException(
        String message, String sqlState, int errorCode,
        Throwable cause) {
        super(message, sqlState, errorCode, cause);
      }
    }
    verifyR2dbcExceptionMapping(
      new TestSqlException("message", "state", 99,
        new IOException("io failure")),
      R2dbcException.class);
  }

  /**
   * Verifies that a {@code sqlException} that is input to
   * {@link OracleR2dbcExceptions#toR2dbcException(SQLException)} is mapped to
   * an instance of an {@code expectedMapping} R2dbcException type. The
   * mapped R2dbcException is verified to have the same message, sql state,
   * and error code as the {@code sqlException}, is verified to the {@code
   * sqlException} as it's initial cause.
   * @param sqlException Input to toR2dbcException
   * @param expectedMapping Expected type output by toR2dbcException
   */
  private void verifyR2dbcExceptionMapping(SQLException sqlException,
    Class<? extends R2dbcException> expectedMapping) {

    final Throwable cause = sqlException.getCause();

    // Expect a SQLException to be mapped to the expected R2dbcException type
    R2dbcException r2dbcException = toR2dbcException(sqlException);
    assertTrue(expectedMapping.isInstance(r2dbcException),
      String.format("%s is not an instance of %s",
        r2dbcException.getClass(), expectedMapping));

    // Expect the R2dbcException to have the same message, sql state, and error
    // code as the SQLException
    assertEquals(sqlException.getMessage(), r2dbcException.getMessage());
    assertEquals(sqlException.getSQLState(), r2dbcException.getSqlState());
    assertEquals(sqlException.getErrorCode(), r2dbcException.getErrorCode());

    // Expect the R2dbcException to have the SQLException as it's initial cause
    assertSame(sqlException, r2dbcException.getCause());

    // Expect the R2dbcException to leave the initial cause of the SQLException
    // intact
    assertSame(cause, r2dbcException.getCause().getCause());
  }

}
