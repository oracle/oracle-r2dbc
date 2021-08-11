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
import java.util.function.Supplier;

/**
 * <p>
 * Static utility methods and functional types that are used across the
 * Oracle R2DBC code base to generate and handle exceptions in conformance
 * with R2DBC standards.
 * </p>
 * @since 0.1.0
 * @author michael-a-mcmahon, harayuanwang
 */
final class OracleR2dbcExceptions {

  /**
   * This class has no instance methods, so this constructor should never be
   * called.
   */
  private OracleR2dbcExceptions() { }

  /**
   * Returns the specified {@code obj} if it is not null, or throws an
   * {@code IllegalArgumentException} with the specified {@code message} if
   * {@code obj} is null. Throwing {@code IllegalArgumentException} conforms
   * to the R2DBC SPI standards for handling {@code null} value arguments that
   * are not allowed to be {@code null}.
   * @param obj Object to check
   * @param message A detail message for an {@code IllegalArgumentException}
   * that is thrown if the check fails.
   * @param <T> The type of {@code obj}
   * @return {@code obj} if it is not null.
   * @throws IllegalArgumentException If {@code obj} is null.
   */
  static <T> T requireNonNull(T obj, String message) {
    if (obj == null)
      throw new IllegalArgumentException(message);
    else
      return obj;
  }

  /**
   * Checks if a {@code jdbcConnection} is open, and throws an exception if the
   * check fails.
   * @throws IllegalStateException If {@code jdbcConnection} is closed
   */
  static void requireOpenConnection(java.sql.Connection jdbcConnection) {
    if (fromJdbc(jdbcConnection::isClosed))
      throw new IllegalStateException("Connection is closed");
  }

  /**
   * <p>
   * Converts a {@link SQLException} into an {@link R2dbcException}. This
   * method a provides error mapping for Oracle R2DBC code that must handle
   * {@code SQLExceptions} thrown from JDBC APIs. Oracle R2DBC needs handle a
   * {@code SQLException} by throwing a corresponding {@code R2dbcException}.
   * </p><p>
 *   The returned exception has the same
   * {@linkplain SQLException#getMessage() message},
   * {@linkplain SQLException#getSQLState() SQLState}, and
   * {@linkplain SQLException#getErrorCode() error code}
   * as the specified {@code sqlException}. The
   * {@linkplain R2dbcException#getCause() cause} of the returned exception
   * is the {@code sqlException}.
   * </p><p>
   * The subtype of {@code R2dbcException} returned by this method is
   * determined based on the {@code SQLException} subtype of the specified
   * {@code sqlException}:
   * </p><table border="1">
   *   <caption>Mapping SQLException to R2dbcException</caption>
   *   <tr>
   *     <th>SQLException Subtype</th>
   *     <th>R2dbcException Subtype</th>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLSyntaxErrorException}</td>
   *     <td>{@link R2dbcBadGrammarException}</td>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLIntegrityConstraintViolationException}</td>
   *     <td>{@link R2dbcDataIntegrityViolationException}</td>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLNonTransientConnectionException}</td>
   *     <td>{@link R2dbcNonTransientResourceException}</td>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLNonTransientException}</td>
   *     <td>{@link R2dbcNonTransientException}</td>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLTimeoutException}</td>
   *     <td>{@link R2dbcTimeoutException}</td>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLTransactionRollbackException}</td>
   *     <td>{@link R2dbcRollbackException}</td>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLTransientConnectionException}</td>
   *     <td>{@link R2dbcTransientResourceException}</td>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLTransientException}</td>
   *     <td>{@link R2dbcTransientException}</td>
   *   </tr>
   *   <tr>
   *     <td>{@link SQLRecoverableException}</td>
   *     <td>{@link R2dbcTransientResourceException}</td>
   *   </tr>
   * </table><p>
   * If the specified {@code sqlException} is not an instance of any of the
   * {@code SQLException} subtypes listed above, then an instance of the
   * {@code R2dbcException} super type is returned.
   * </p>
   *
   * @param sqlException A {@code SQLException} to convert. Not null.
   * @return an {@code R2dbcException} that indicates the same error conditions
   * as the specified {@code sqlException}. Not null.
   */
  static R2dbcException toR2dbcException(SQLException sqlException) {
    assert sqlException != null : "sqlException is null";

    final String message = sqlException.getMessage();
    final String sqlState = sqlException.getSQLState();
    final int errorCode = sqlException.getErrorCode();

    if (sqlException instanceof SQLNonTransientException) {
      if (sqlException instanceof SQLSyntaxErrorException) {
        return new R2dbcBadGrammarException(
          message, sqlState, errorCode, sqlException);
      }
      else if (sqlException instanceof SQLIntegrityConstraintViolationException) {
        return new R2dbcDataIntegrityViolationException(
          message, sqlState, errorCode, sqlException);
      }
      else if (sqlException instanceof SQLNonTransientConnectionException) {
        return new R2dbcNonTransientResourceException(
          message, sqlState, errorCode, sqlException);
      }
      else {
        return new OracleR2dbcNonTransientException(
          message, sqlState, errorCode, sqlException);
      }
    }
    else if (sqlException instanceof SQLTransientException) {
      if (sqlException instanceof SQLTimeoutException) {
        return new R2dbcTimeoutException(
          message, sqlState, errorCode, sqlException);
      }
      else if (sqlException instanceof SQLTransactionRollbackException) {
        return new R2dbcRollbackException(
          message, sqlState, errorCode, sqlException);
      }
      else if (sqlException instanceof SQLTransientConnectionException) {
        return new R2dbcTransientResourceException(
          message, sqlState, errorCode, sqlException);
      }
      else {
        return new OracleR2dbcTransientException(
          message, sqlState, errorCode, sqlException);
      }
    }
    else if (sqlException instanceof SQLRecoverableException) {
      // This mapping is less obvious than the others. The rationale is that a
      // SQLRecoverableException indicates that a retry may succeed, but that
      // the connection is no longer valid. The R2dbcTransientResourceException
      // expresses the same conditions.
      return new R2dbcTransientResourceException(
        message, sqlState, errorCode, sqlException);
    }
    else {
      return new OracleR2dbcException(
        message, sqlState, errorCode, sqlException);
    }
  }

  /**
   * Runs the the specified {@code runnable} to completion, or throws an
   * {@link R2dbcException} if the runnable throws a {@link SQLException}. This
   * method serves to improve code readability. For instance:
   * <pre>
   *   try {
   *     preparedStatement.addBatch();
   *   }
   *   catch (SQLException sqlException) {
   *     throw OracleR2dbcExceptions.toR2dbcException(sqlException);
   *   }
   * </pre>
   * Can be expressed more concisely as:
   * <pre>
   *   OracleR2dbcExceptions.runJdbc(preparedStatement::addBatch);
   * </pre>
   *
   * @param runnable Runs to completion or throws a {@code SQLException}. Not
   *   null.
   * @throws R2dbcException If the supplier throws a {@code SQLException}.
   */
  static void runJdbc(ThrowingRunnable runnable)
    throws R2dbcException {
    try {
      runnable.runOrThrow();
    }
    catch (SQLException sqlException) {
      throw toR2dbcException(sqlException);
    }
  }

  /**
   * Returns the specified {@code supplier}'s output, or throws a
   * {@link R2dbcException} if the function throws a {@link SQLException}. This
   * method serves to improve code readability. For instance:
   * <pre>
   *   try {
   *     return resultSet.getMetaData();
   *   }
   *   catch (SQLException sqlException) {
   *     throw OracleR2dbcExceptions.toR2dbcException(sqlException);
   *   }
   * </pre>
   * Can be expressed more concisely as:
   * <pre>
   *   return fromJdbc(resultSet::getMetaData);
   * </pre>
   *
   * @param supplier Returns a value or throws a {@code SQLException}. Not
   * null.
   * @param <T> The output type of the supplier
   * @return The output of the specified {@code supplier}.
   * @throws R2dbcException If the supplier throws a {@code SQLException}.
   */
  static <T> T fromJdbc(ThrowingSupplier<T> supplier)
    throws R2dbcException {
    try {
      return supplier.getOrThrow();
    }
    catch (SQLException sqlException) {
      throw toR2dbcException(sqlException);
    }
  }

  /**
   * Creates a new R2DBC exception for an error that is non-transient.
   * An error is non-transient if it reoccurs every time the operation that
   * caused the error is repeated, and stops reoccurring only if the
   * condition that caused the failure is corrected.
   * @param message A descriptive message that helps another programmer
   *                understand the cause of failure. Not null.
   * @param cause An error thrown by other code to indicate a failure, if any.
   *             May be null.
   * @return A new non-transient exception.
   */
  static R2dbcNonTransientException newNonTransientException(
    String message, Throwable cause) {
    return new OracleR2dbcNonTransientException(message, null, 0, cause);
  }

  /**
   * <p>
   * Function type that returns no value or throws a {@link SQLException}.
   * This functional interface can reference JDBC methods that throw
   * {@code SQLExceptions}. The standard {@link Runnable} interface cannot
   * reference methods that throw checked exceptions.
   * </p>
   */
  @FunctionalInterface
  interface ThrowingRunnable extends Runnable {
    /**
     * Runs to completion and returns normally, or throws a {@code SQLException}
     * if an error is encountered.
     * @throws SQLException If the run does not complete due to an error.
     */
    void runOrThrow() throws SQLException;

    /**
     * Runs to completion and returns normally, or throws an {@code
     * R2dbcException} if an error is encountered.
     * @throws R2dbcException If the run does not complete due to an error.
     * @implNote The default implementation invokes
     * {@link #runJdbc(ThrowingRunnable)} with this {@code
     * ThrowingRunnable}.
     */
    @Override
    default void run() throws R2dbcException {
      runJdbc(this);
    }
  }

  /**
   * <p>
   * Function type that returns a value or throws a {@link SQLException}. This
   * functional interface can reference JDBC methods that throw
   * {@code SQLExceptions}. The standard {@link Supplier} interface cannot
   * reference methods that throw checked exceptions.
   * </p>
   * @param <T> the type of values supplied by this supplier.
   */
  @FunctionalInterface
  interface ThrowingSupplier<T> extends Supplier<T> {
    /**
     * Returns a value, or throws a {@code SQLException} if an error is
     * encountered.
     * @return the supplied value
     * @throws SQLException If a value is not returned due to an error.
     */
    T getOrThrow() throws SQLException;

    /**
     * Returns a value, or throws an {@code R2dbcException} if an error is
     * encountered.
     * @throws R2dbcException If a value is not returned due to an error.
     * @implNote The default implementation invokes
     * {@link #fromJdbc(ThrowingSupplier)} (ThrowingRunnable)}
     * with this {@code ThrowingSupplier}.
     */
    @Override
    default T get() throws R2dbcException {
      return fromJdbc(this);
    }
  }

  /**
   * <p>
   * Subclass of {@link R2dbcException}. {@code R2dbcException} is an
   * abstract class. In order to instantiate an instance of this type, a
   * concrete subclass must be defined. This subclass does not implement any
   * behavior that is specific to the Oracle driver.
   * </p><p>
   * This subclass is defined so that {@link #toR2dbcException(SQLException)}
   * can throw an instance of {@code R2dbcException} when mapping a
   * {@link SQLException}.
   * </p>
   */
  private static final class OracleR2dbcException
    extends R2dbcException {
    private OracleR2dbcException(
      String message, String sqlState, int errorCode,
      SQLException sqlException) {
      super(message, sqlState, errorCode, sqlException);
    }
  }

  /**
   * <p>
   * Subclass of {@link R2dbcTransientException}.
   * {@code R2dbcTransientException} is an abstract class. In order to
   * instantiate an instance of this type, a concrete subclass must be defined.
   * This subclass does implement any behavior that is specific to the
   * Oracle driver.
   * </p><p>
   * This subclass is defined so that {@link #toR2dbcException(SQLException)}
   * can throw an instance of {@code R2dbcTransientException} when mapping a
   * {@link SQLTransientException}.
   * </p>
   */
  private static final class OracleR2dbcTransientException
    extends R2dbcTransientException {
    private OracleR2dbcTransientException(
      String message, String sqlState, int errorCode,
      SQLException sqlException) {
      super(message, sqlState, errorCode, sqlException);
    }
  }

  /**
   * <p>
   * Subclass of {@link R2dbcNonTransientException}.
   * {@code R2dbcNonTransientException} is an abstract class. In order to
   * instantiate an instance of this type, a concrete subclass must be defined.
   * This subclass does implement any behavior that is specific to the
   * Oracle driver.
   * </p><p>
   * This subclass is defined so that {@link #toR2dbcException(SQLException)}
   * can throw an instance of {@code R2dbcNonTransientException} when mapping a
   * {@link SQLNonTransientException}.
   * </p>
   */
  private static final class OracleR2dbcNonTransientException
    extends R2dbcNonTransientException {
    private OracleR2dbcNonTransientException(
      String message, String sqlState, int errorCode,
      Throwable cause) {
      super(message, sqlState, errorCode, cause);
    }
  }

}
