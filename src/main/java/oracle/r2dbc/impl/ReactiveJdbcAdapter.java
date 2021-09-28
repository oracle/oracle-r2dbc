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

import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * <p>
 * Interface that is implemented to adapt the non-blocking capabilities of a
 * JDBC driver to conform with the Reactive Streams and R2DBC standards.
 * </p><p>
 * The JDBC 4.3 Specification does not include a <i>standard</i> API for
 * asynchronous database access, so a JDBC driver may instead offer
 * <i>non-standard</i> APIs that implement asynchronous functionality. This
 * interface provides a common API that can be implemented using any vendor
 * specific APIs.
 * </p><p>
 * <i>
 * This adapter API is only intended for use within the code base of the Oracle
 * R2DBC Driver.
 * </i>
 * It's purpose is to allow the code base to be written without referencing any
 * vendor specific extensions of the standard JDBC API.
 * </p><p>
 * In version 0.1.0 of the Oracle R2DBC Driver, the only requirement is to
 * support the 21c version of the Oracle JDBC Driver. Later Oracle R2DBC
 * releases may include support for additional JDBC drivers.
 * </p>
 *
 * @author  michael-a-mcmahon
 * @since   0.1.0
 */
interface ReactiveJdbcAdapter {

  /**
   * <p>
   * Returns an adapter for Oracle Database, or throws an exception if no
   * suitable JDBC driver can be found.
   * </p><p>
   * The adapter returned by this method requires an Oracle JDBC Driver of
   * version 21c or newer to be registered with the
   * {@link java.sql.DriverManager}
   * </p>
   * @return An adapter for the Oracle JDBC Driver. Not null.
   * @throws R2dbcException If a 21c or newer Oracle JDBC Driver is not
   *   registered with the driver manager.
   */
  static ReactiveJdbcAdapter getOracleAdapter() throws R2dbcException {
    try {
      int oracleDriverVersion =
        DriverManager
          .getDriver("jdbc:oracle:thin:")
          .getMajorVersion();

      if (oracleDriverVersion < 21)
        throw new R2dbcNonTransientResourceException(
          "Unsupported Oracle JDBC Driver version: " + oracleDriverVersion);
    }
    catch (SQLException getDriverException) {
      throw OracleR2dbcExceptions.newNonTransientException(
        "Failed to locate the Oracle JDBC Driver", getDriverException);
    }

    return OracleReactiveJdbcAdapter.getInstance();
  }

  /**
   * <p>
   * Returns a JDBC {@code DataSource} configured with the values specified in
   * the {@code options} parameter. Adapters implementing this method return a
   * {@code DataSource} that is supported as an argument to their
   * implementation of
   * {@link ReactiveJdbcAdapter#publishConnection(DataSource)}.
   * </p><p>
   * Adapters implementing this method <i>must</i> specify each supported value
   * of {@link ConnectionFactoryOptions}, how the value of each option is
   * applied to the JDBC driver, and whether the option is required or not.
   * </p>
   * @param options Set of options that configure a {@code DataSource}.
   * @return A {@code DataSource} configured with the specified {@code options}.
   * @throws IllegalStateException If a required {@code options} value is not
   * specified
   */
  DataSource createDataSource(ConnectionFactoryOptions options);

  /**
   * <p>
   * Publishes a single JDBC {@link Connection} to a single subscriber. The
   * connection is established as if by invoking
   * {@link DataSource#getConnection()} on the specified {@code dataSource}.
   * The {@code Connection} is configured to execute asynchronous tasks using
   * the provided {@code executor}.
   * </p><p>
   * The {@code dataSource} is retained by the returned publisher, meaning
   * that any mutable state of the {@code dataSource} can affect the behavior
   * of the returned publisher.
   * </p><p>
   * The returned publisher initiates connection establishment only when a
   * subscriber has subscribed and <i>signalled demand</i>. Once a
   * subscriber has subscribed, the returned publisher signals
   * {@code onError} with {@link IllegalStateException} to all subsequent
   * subscribers.
   * </p><p>
   * The returned publisher releases any resources allocated for a connection
   * if a subscriber {@linkplain Subscription#cancel() cancels} it's
   * subscription, <i>before</i> a connection is emitted.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if connection establishment fails.
   * </p>
   * @param dataSource JDBC data source that is configured to establish a
   *   connection. Not null.
   * @param executor Executor to use for executing asynchronous tasks. Not null.
   * @return A publisher that emits a JDBC connection. Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<? extends Connection> publishConnection(
    DataSource dataSource, Executor executor)
    throws R2dbcException;

  /**
   * <p>
   * Publishes the result type of a SQL statement as a single {@link Boolean}
   * value to a single subscriber. SQL is executed for a boolean result as if
   * by invoking {@link PreparedStatement#execute()} on the specified {@code
   * sqlStatement}.
   * </p><p>
   * The {@code sqlStatement} is retained by the returned publisher, meaning
   * that any mutable state of {@code sqlStatement} can affect the
   * behavior of the returned publisher.
   * </p><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if SQL execution fails.
   * </p>
   * @param sqlStatement JDBC prepared statement that is configured to execute
   *  a SQL statement. Not null.
   * @return A publisher that emits a SQL execution result. Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Boolean> publishSQLExecution(PreparedStatement sqlStatement)
    throws R2dbcException;

  /**
   * <p>
   * Publishes the result of a batch SQL update as one or more {@link Long}
   * values to a single subscriber. SQL is executed for update counts as if
   * by invoking {@link PreparedStatement#executeLargeBatch()} on the specified
   * {@code batchUpdateStatement}.
   * </p><p>
   * The {@code batchUpdateStatement} is retained by the returned publisher,
   * meaning that any mutable state of {@code batchUpdateStatement} can affect
   * the behavior of the returned publisher.
   * </p><p>
   * The returned publisher initiates SQL execution <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if SQL execution fails.
   * </p>
   * @param batchUpdateStatement JDBC prepared statement that is configured to
   *                             execute a batch SQL update. Not null.
   * @return A publisher that emits a SQL execution result. Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Long> publishBatchUpdate(PreparedStatement batchUpdateStatement)
    throws R2dbcException;

  /**
   * <p>
   * Publishes the rows of a result set as one or more {@code T} type objects
   * to a single subscriber.
   * </p><p>
   * {@code T} type objects are output from the specified
   * {@code rowMappingFunction} when a {@link JdbcReadable} is applied as input.
   * Each row in the table of data represented by the {@code resultSet} is
   * applied as input in the form of a {@link JdbcReadable}. If the function
   * returns a {@code null} value, then a {@link NullPointerException} is
   * emitted to the subscriber as an {@code onError} signal. If the function
   * throws an unchecked exception, then that exception is emitted to the
   * subscriber as an {@code onError} signal.
   * </p><p>
   * Rows of data are fetched from the result set as if by invoking 
   * {@link ResultSet#next()} on the specified {@code resultSet}, with the
   * invocation repeated a number of times equal to the
   * {@link Subscription#request(long)} size or until the invocation returns
   * {@code false}.
   * </p><p>
   * The {@code resultSet} is retained by the returned publisher, meaning
   * that any mutable state of the {@code resultSet} can affect the behavior
   * of the returned publisher.
   * </p><p>
   * The returned publisher initiates a row fetch only when a subscriber has
   * subscribed and has <i>unfilled demand.</i>
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if a failure occurs when fetching row data from
   * the database.
   * </p>
   *
   * @implSpec Adapters that implement this method may publish an
   *   {@link R2dbcException} if the specified {@code resultSet} is not
   *   {@link ResultSet#TYPE_FORWARD_ONLY} or is not 
   *   {@link ResultSet#CONCUR_READ_ONLY}
   *
   * @implSpec Adapters that implement this method may signal {@code onError}
   *   with an {@link R2dbcException} if the specified {@code resultSet}'s
   *   cursor has been ever been moved by a synchronous call, such as
   *   {@link ResultSet#next()}.
   *
   * @implSpec Adapters that implement this method may place the specified
   *   {@code resultSet} into a logically closed state where all method calls
   *   will throw a {@code SQLException} indicating that the result set is
   *   closed.
   *
   * @implSpec Adapters that implement this method <i>must</i> release any
   *   resources allocated to the specified {@code resultSet}, as if by
   *   invoking {@link ResultSet#close()}, when the returned publisher emits
   *   a terminal signal, or when the subscriber has cancelled it's
   *   subscription.
   *
   * @param resultSet JDBC result set that represents a table of data.
   *                  Not null.
   * @param rowMappingFunction Function that maps row data into an output type.
   *                           Not null.
   * @param <T> The output type of the {@code rowMappingFunction}
   * @return A publisher that emits row data as the mapped output type.
   *   Not null.
   * @throws R2dbcException If a database access error occurs.
   *
   */
  <T> Publisher<T> publishRows(
    ResultSet resultSet, Function<JdbcReadable, T> rowMappingFunction)
    throws R2dbcException;

  /**
   * <p>
   * Publishes the result of committing a transaction to one or more
   * subscribers. The transaction is committed as if by invoking
   * {@link Connection#commit()} on the specified {@code connection}, without
   * throwing a {@code SQLException} if auto-commit is enabled. If
   * auto-commit is enabled when a subscriber initiates the commit, then the
   * returned publisher only emits {@code onComplete}.
   * </p><p>
   * The {@code connection} is retained by the returned publisher, meaning
   * that any mutable state of {@code connection} can affect the
   * behavior of the returned publisher.
   * </p><p>
   * The returned publisher initiates a commit <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal. All subsequent subscribers receive the same signals in the same
   * order as the first subscriber.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if a transaction commit fails.
   * </p>
   * @param connection A JDBC connection that has an active transaction.
   *                   Not null.
   * @return A publisher that emits the result of committing the transaction.
   * Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Void> publishCommit(Connection connection) throws R2dbcException;

  /**
   * <p>
   * Publishes the result of rolling back a transaction to one or more
   * subscribers. The transaction is rolled back as if by invoking
   * {@link Connection#rollback()} on the specified {@code connection},
   * without throwing a {@code SQLException} if auto-commit is
   * enabled. If auto-commit is enabled when a subscriber initiates the
   * rollback, then the returned publisher only emits {@code onComplete}.
   * </p><p>
   * The {@code connection} is retained by the returned publisher, meaning
   * that any mutable state of {@code connection} can affect the
   * behavior of the returned publisher.
   * </p><p>
   * The returned publisher initiates a rollback <i>the first time</i> a
   * subscriber subscribes, before the subscriber emits a {@code request}
   * signal. All subsequent subscribers receive the same signals in the same
   * order as the first subscriber.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if a transaction rollback fails.
   * </p>
   * @param connection A JDBC connection that has an active transaction.
   *                   Not null.
   * @return A publisher that emits the result of rolling back the transaction.
   * Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Void> publishRollback(Connection connection) throws R2dbcException;

  /**
   * <p>
   * Publishes the result of closing a connection to one or more subscribers.
   * The connection is closed as if by invoking {@link Connection#close()} on
   * the specified {@code connection}.
   * </p><p>
   * The {@code connection} is retained by the returned
   * publisher, meaning that any mutable state of {@code connection} can affect
   * the behavior of the returned publisher.
   * </p><p>
   * The returned publisher initiates a connection close <i>the first
   * time</i> a subscriber subscribes, before the subscriber emits a {@code
   * request} signal. All subsequent subscribers receive the same signals in
   * the same order as the first subscriber.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if a connection close fails.
   * </p>
   * @param connection A JDBC connection. Not null.
   * @return A publisher that emits the result of closing the connection.
   * Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Void> publishClose(Connection connection) throws R2dbcException;

  /**
   * <p>
   * Publishes the contents of a Binary Large Object (BLOB) to a single
   * subscriber. The contents are emitted as if by invoking
   * {@link Blob#getBinaryStream()} on the specified {@code blob}, and then
   * repeatedly invoking {@link InputStream#read(byte[])} on the returned
   * {@code InputStream} until {@code -1} is returned.
   * </p><p>
   * The {@code blob} is retained by the returned publisher, meaning that any
   * mutable state of the {@code blob} can affect the behavior of the
   * returned publisher.
   * </p><p>
   * Emitted {@code ByteBuffers} are not retained by the returned publisher,
   * meaning that any mutable state of the buffer has no effect on the
   * behavior of the returned publisher.
   * </p><p>
   * The returned publisher initiates a content reading only when a
   * subscriber has subscribed and has <i>unfilled demand.</i>
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if content publishing fails.
   * </p>
   * @param blob A JDBC blob. Not null.
   * @return A publisher that emits the content of the blob. Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<ByteBuffer> publishBlobRead(Blob blob) throws R2dbcException;

  /**
   * <p>
   * Publishes the contents of a Character Large Object (CLOB) to a single
   * subscriber. The contents are emitted as if by invoking
   * {@link Clob#getCharacterStream()} on the specified {@code clob}, and
   * then repeatedly invoking {@link Reader#read(char[])} on the returned
   * {@code Reader} until {@code -1} is returned.
   * </p><p>
   * The {@code clob} is retained by the returned publisher, meaning that any
   * mutable state of the {@code clob} can affect the behavior of the
   * returned publisher.
   * </p><p>
   * Emitted {@code CharSequences} are not retained by the returned publisher,
   * meaning that any mutable state of the character sequence has no
   * effect on the behavior of the returned publisher.
   * </p><p>
   * The returned publisher initiates a content reading only when a
   * subscriber has subscribed and has <i>unfilled demand.</i>
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if content publishing fails.
   * </p>
   * @param clob A JDBC clob. Not null.
   * @return A publisher that emits the content of the clob. Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<? extends CharSequence> publishClobRead(Clob clob)
    throws R2dbcException;

  /**
   * <p>
   * Publishes the result of writing the contents of a Binary Large Object
   * (BLOB) to a single subscriber. The result is emitted as if by invoking
   * {@link Blob#setBinaryStream(long)} on the specified {@code blob}, and
   * then repeatedly invoking {@link java.io.OutputStream#write(byte[])} on
   * the returned {@code OutputStream} with bytes emitted by the specified
   * {@code contentPublisher} until it terminates.
   * </p><p>
   * The {@code blob} is retained by the returned publisher, meaning that any
   * mutable state of the {@code blob} can affect the behavior of the
   * returned publisher.
   * </p><p>
   * {@code ByteBuffers} emitted by the {@code contentPublisher} are not
   * retained by the returned publisher, meaning that any mutable state of the
   * buffers has no effect on the behavior of the returned publisher.
   * The returned publisher does not mutate the position or limit of emitted
   * {@code ByteBuffers}.
   * </p><p>
   * The returned publisher initiates a content writing <i>the first
   * time</i> a subscriber subscribes, before the subscriber emits a {@code
   * request} signal.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if content writing fails.
   * </p>
   * @param contentPublisher Binary content publisher
   * @param blob A JDBC blob. Not null.
   * @return A publisher that emits the result of writing content to the blob.
   *   Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Void> publishBlobWrite(
    Publisher<ByteBuffer> contentPublisher, Blob blob) throws R2dbcException;

  /**
   * <p>
   * Publishes the result of writing the contents of a Character Large Object
   * (CLOB) to a single subscriber. The result is emitted as if by invoking
   * {@link Clob#setCharacterStream(long)} on the specified {@code clob}, and
   * then repeatedly invoking {@link java.io.Writer#write(String)} on the
   * returned {@code Writer} with CharSequences emitted by the specified
   * {@code contentPublisher} until it terminates.
   * </p><p>
   * The {@code clob} is retained by the returned publisher, meaning that any
   * mutable state of the {@code clob} can affect the behavior of the
   * returned publisher.
   * </p><p>
   * {@code CharSequences} emitted by the {@code contentPublisher} are not
   * retained by the returned publisher, meaning that any mutable state of the
   * character sequences has no effect on the behavior of the returned
   * publisher.
   * </p><p>
   * The returned publisher initiates a content writing <i>the first
   * time</i> a subscriber subscribes, before the subscriber emits a {@code
   * request} signal.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if content writing fails.
   * </p>
   * @param contentPublisher Character content publisher
   * @param clob A JDBC clob. Not null. Retained.
   * @return A publisher that emits the result of writing content to the clob.
   *   Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Void> publishClobWrite(
    Publisher<? extends CharSequence> contentPublisher, Clob clob)
    throws R2dbcException;

  /**
   * <p>
   * Publishes the result of releasing the resources of a Binary Large Object
   * (BLOB) to one or more subscribers. The result is emitted as if by
   * invoking {@link Blob#free()} on the specified {@code blob}.
   * </p><p>
   * The specified {@code blob} is retained by the returned
   * publisher, meaning that any mutable state of {@code blob} that
   * affects the releasing of resources also affects the returned publisher.
   * </p><p>
   * The returned publisher initiates a resource release <i>the first
   * time</i> a subscriber subscribes, before the subscriber emits a {@code
   * request} signal. All subsequent subscribers receive the same signals in
   * the same order as the first subscriber.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if the resource release fails.
   * </p>
   * @param blob A JDBC blob. Retained. Not null.
   * @return A publisher that emits the result of releasing the blob's
   * resources. Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Void> publishBlobFree(Blob blob) throws R2dbcException;

  /**
   * <p>
   * Publishes the result of releasing the resources of a Character Large Object
   * (CLOB) to one or more subscribers. The result is emitted as if by
   * invoking {@link Clob#free()} on the specified {@code clob}.
   * </p><p>
   * The specified {@code clob} is retained by the returned
   * publisher, meaning that any mutable state of {@code clob} that
   * affects the releasing of resources also affects the returned publisher.
   * </p><p>
   * The returned publisher initiates a resource release <i>the first
   * time</i> a subscriber subscribes, before the subscriber emits a {@code
   * request} signal. All subsequent subscribers receive the same signals in
   * the same order as the first subscriber.
   * </p><p>
   * The returned publisher signals {@code onError} with an
   * {@link R2dbcException} if the resource release fails.
   * </p>
   * @param clob A JDBC clob. Retained. Not null.
   * @return A publisher that emits the result of releasing resources of a
   * {@code clob}. Not null.
   * @throws R2dbcException If a database access error occurs.
   */
  Publisher<Void> publishClobFree(Clob clob) throws R2dbcException;

  /** 
   * Accessor of column values within a single row from a table of data that
   * a {@link ResultSet} represents. Instances of {@code JdbcRow} are
   * supplied as input to row mapping functions, and each instance is valid
   * only within the scope of a row mapping function's call. Usage outside of
   * a row mapping function's scope results in an {@code IllegalStateException}.
   */
  interface JdbcReadable {

    /**
     * Returns the value of this row for the specified {@code index} as
     * the specified {@code type}. The value is returned as if by invoking
     * {@link ResultSet#getObject(int, Class)} on a result set with a cursor
     * positioned on the table row that this object represents.
     * @param index 0-based column index. (The first column's index is 0)
     * @param type The type of object to return. Not null.
     * @param <T> The returned type
     * @return The column value as the specified type.
     * @throws R2dbcException If the {@code index} is invalid
     * @throws IllegalArgumentException If conversion to the specified {@code
     * type} is not supported.
     * @throws IllegalStateException If this method is invoked outside of a
     * row mapping function.
     */
     <T> T getObject(int index, Class<T> type);
  }

}