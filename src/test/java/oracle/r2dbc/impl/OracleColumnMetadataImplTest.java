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

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleType;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.RowId;
import java.sql.SQLType;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;

import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.databaseVersion;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitUpdate;
import static oracle.r2dbc.util.Awaits.tryAwaitNone;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that
 * {@link OracleColumnMetadataImpl} implements behavior that is specified in
 * it's class and method level javadocs.
 */
public class OracleColumnMetadataImplTest {
  
  /**
   * Verifies the implementation of {@link OracleColumnMetadataImpl} for
   * character type columns.
   */
  @Test
  public void testCharacterTypes() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect CHAR and String to map
      verifyColumnMetadata(
        connection, "CHAR(1)", JDBCType.CHAR, R2dbcType.CHAR, 1, null,
        String.class, "Y");

      // Expect VARCHAR and String to map
      verifyColumnMetadata(
        connection, "VARCHAR(999)", JDBCType.VARCHAR, R2dbcType.VARCHAR, 999,
        null, String.class, "test");

      // Expect NCHAR and String to map
      verifyColumnMetadata(
        connection, "NCHAR(2)", JDBCType.NCHAR, R2dbcType.NCHAR, 2, null,
        String.class, "NN");

      // Expect NVARCHAR and String to map.
      verifyColumnMetadata(
        connection, "NVARCHAR2(888)", JDBCType.NVARCHAR, R2dbcType.NVARCHAR,
        888, null, String.class, "test");

      // Expect CLOB and io.r2dbc.spi.Clob to map. String can be used as a
      // bind value, but Clob is the default mapping for Row values.
      verifyColumnMetadata(
        connection, "CLOB", JDBCType.CLOB, OracleR2dbcTypes.CLOB, null, null,
        Clob.class, "test");

      // Expect NCLOB and io.r2dbc.spi.Clob to map
      verifyColumnMetadata(
        connection, "NCLOB", JDBCType.NCLOB, OracleR2dbcTypes.NCLOB, null, null,
        Clob.class, "test");

      // Expect LONG and String to map.
      verifyColumnMetadata(
        connection, "LONG", JDBCType.LONGVARCHAR, OracleR2dbcTypes.LONG,
        Integer.MAX_VALUE, null, String.class, "test");

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleColumnMetadataImpl} for
   * binary type columns.
   */
  @Test
  public void testBinaryTypes() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect VARBINARY and ByteBuffer to map.
      verifyColumnMetadata(
        connection, "RAW(10)", JDBCType.VARBINARY, R2dbcType.VARBINARY, 10,
        null, ByteBuffer.class, ByteBuffer.wrap(new byte[3]));

      // Expect LONG RAW and ByteBuffer to map.
      verifyColumnMetadata(
        connection, "LONG RAW", JDBCType.LONGVARBINARY,
        OracleR2dbcTypes.LONG_RAW, Integer.MAX_VALUE, null, ByteBuffer.class,
        ByteBuffer.wrap(new byte[3]));

      // Expect BLOB and io.r2dbc.spi.Blob to map. ByteBuffer can be used as
      // a bind value, but Blob is the default mapping for Row values.
      verifyColumnMetadata(
        connection, "BLOB", JDBCType.BLOB, OracleR2dbcTypes.BLOB, null, null,
        Blob.class, ByteBuffer.wrap(new byte[3]));

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleColumnMetadataImpl} for
   * numeric type columns.
   */
  @Test
  public void testNumericTypes() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect NUMBER and BigDecimal to map.
      verifyColumnMetadata(
        connection, "NUMBER(5,3)", JDBCType.NUMERIC, R2dbcType.NUMERIC, 5, 3,
        BigDecimal.class, BigDecimal.valueOf(12.345));

      // Expect FLOAT and Double to map.
      verifyColumnMetadata(
        connection, "FLOAT(6)", JDBCType.FLOAT, R2dbcType.FLOAT, 6, null,
        Double.class, 123.456D);

      // Expect BINARY_DOUBLE and Double to map.
      verifyColumnMetadata(
        connection, "BINARY_DOUBLE", OracleType.BINARY_DOUBLE,
        OracleR2dbcTypes.BINARY_DOUBLE,  null, null, Double.class,
        1234567.89012345D);

      // Expect BINARY_FLOAT and Float to map.
      verifyColumnMetadata(
        connection, "BINARY_FLOAT", OracleType.BINARY_FLOAT,
        OracleR2dbcTypes.BINARY_FLOAT, null, null, Float.class, 123.456F);

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleColumnMetadataImpl} for
   * datetime type columns.
   */
  @Test
  public void testDateTimeTypes() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect Oracle's "DATE" and LocalDateTime to map. Expect precision to be
      // the maximum String length returned by LocalDateTime.toString(). The
      // Oracle type named "DATE" is equivalent to the standard TIMESTAMP
      // type with 0 digits of fractional second precision; Expect Oracle
      // R2DBC to describe Oracle's "DATE" as TIMESTAMP
      verifyColumnMetadata(
        connection, "DATE", JDBCType.TIMESTAMP, R2dbcType.TIMESTAMP, 29, 0,
        LocalDateTime.class, LocalDateTime.parse("1977-06-16T09:00:00"));

      // Expect TIMESTAMP and LocalDateTime to map. Expect precision to be
      // the maximum String length returned by LocalDateTime.toString().
      // Expect scale to be the number of decimal digits in the fractional
      // seconds component.
      verifyColumnMetadata(
        connection, "TIMESTAMP(2)", JDBCType.TIMESTAMP, R2dbcType.TIMESTAMP, 29,
        2, LocalDateTime.class, LocalDateTime.parse("1977-06-16T09:00:00.12"));

      // Expect TIMESTAMP WITH TIME ZONE and OffsetDateTime to map. Expect
      // precision to be the maximum String length returned by
      // OffsetDateTime.toString(). Expect scale to be the number of decimal
      // digits in the fractional seconds component.
      System.out.println("SKIPPING TIMESTAMP WITH TIME ZONE TEST");
      /*
      TODO: Uncomment when this fix is released:
      https://github.com/r2dbc/r2dbc-spi/commit/a86562421a312df2d8a3ae187553bf6c2b291aad
      verifyColumnMetadata(
        connection, "TIMESTAMP(3) WITH TIME ZONE",
        JDBCType.TIMESTAMP_WITH_TIMEZONE, R2dbcType.TIMESTAMP_WITH_TIME_ZONE,
        35, 3, OffsetDateTime.class,
        OffsetDateTime.parse("1977-06-16T09:00:00.123+01:23"));
        *
       */

      // Expect TIMESTAMP WITH LOCAL TIME ZONE and LocalDateTime to map.
      verifyColumnMetadata(
        connection, "TIMESTAMP(4) WITH LOCAL TIME ZONE",
        OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        OracleR2dbcTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        29, 4, LocalDateTime.class,
        LocalDateTime.parse("1977-06-16T09:00:00.1234"));

      // Expect INTERVAL YEAR TO MONTH and Period to map.
      verifyColumnMetadata(
        connection, "INTERVAL YEAR(9) TO MONTH",
        OracleType.INTERVAL_YEAR_TO_MONTH,
        OracleR2dbcTypes.INTERVAL_YEAR_TO_MONTH, 9, null, Period.class,
        Period.parse("+P123456789Y11M00W00D"));

      // Expect INTERVAL DAY TO SECOND and Duration to map.
      verifyColumnMetadata(
        connection, "INTERVAL DAY(9) TO SECOND(9)",
        OracleType.INTERVAL_DAY_TO_SECOND,
        OracleR2dbcTypes.INTERVAL_DAY_TO_SECOND, 9, 9, Duration.class,
        Duration.parse("+P123456789DT23H59M59.123456789S"));

      // Expect ROWID and String to map.
      // Expect UROWID and String to map.
      // Expect JSON and OracleJsonObject to map.

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleColumnMetadataImpl} for
   * row ID type columns.
   */
  @Test
  public void testRowIdTypes() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // The "dual" table is queried to retrieve a valid row ID.
      RowId rowId = awaitOne(Mono.from(connection.createStatement(
        "SELECT rowid FROM dual")
        .execute())
        .flatMap(result ->
          Mono.from(result.map((row, metadata) ->
            row.get(0, RowId.class)))
        ));

      // Expect ROWID and String to map.
      verifyColumnMetadata(
        connection, "ROWID", JDBCType.ROWID, OracleR2dbcTypes.ROWID, null,
        null,
        RowId.class, rowId);

      // Expect UROWID and String to map.
      verifyColumnMetadata(
        connection, "UROWID", JDBCType.ROWID, OracleR2dbcTypes.ROWID, null,
        null, RowId.class, rowId);

      // Expect JSON and OracleJsonObject to map.

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleColumnMetadataImpl} for
   * JSON type columns. When the test database older than version 21c, this test
   * is expected to fail with an ORA-00902 error indicating that JSON is not
   * a valid data type. The JSON type was added in 21c.
   */
  @Test
  public void testJsonType() {

    // The JSON data type was introduced in Oracle Database version 21c, so this
    // test is a no-op if the version is older than 21c.
    if (databaseVersion() < 21)
      return;

    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      OracleJsonObject oracleJson = new OracleJsonFactory().createObject();
      oracleJson.put("species", "cat");
      oracleJson.put("birthday", OffsetDateTime.of(
        LocalDate.of(2009, 7, 12), LocalTime.NOON, ZoneOffset.UTC));
      oracleJson.put("weight", 9.2);

      // Expect JSON and OracleJsonObject to map.
      verifyColumnMetadata(
        connection, "JSON", OracleType.JSON, OracleR2dbcTypes.JSON, null, null,
        OracleJsonObject.class, oracleJson);
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Calls
   * {@link #verifyColumnMetadata(Connection, String, SQLType, Integer, Integer, Nullability, Class, Object)}
   * with the nullable and not nullable variants of the {@code columnTypeDdl}
   */
  private void verifyColumnMetadata(
    Connection connection, String columnDdl, SQLType sqlType, Type r2dbcType,
    Integer precision, Integer scale, Class<?> javaType, Object javaValue) {
    verifyColumnMetadata(
      connection, columnDdl, sqlType, r2dbcType, precision, scale,
      Nullability.NULLABLE, javaType, javaValue);
    verifyColumnMetadata(
      connection, columnDdl + " NOT NULL",  sqlType, r2dbcType, precision,
      scale, Nullability.NON_NULL, javaType, javaValue);
  }

  /**
   * <p>
   * Verifies the {@link ColumnMetadata} object generated by the Oracle R2DBC
   * Driver for a column of the specified {@code sqlType}, {@code precision},
   * and {@code scale}.
   * </p><p>
   * This method creates a table having a column of the specified SQL type
   * and attributes. A {@code javaValue} is inserted into the column, and
   * selected back in order to obtain metadata. The metadata is verified to
   * produce name, type, precision, and scale values that match the values
   * specified in the column's DDL. The metadata's Java type is verified to
   * match the {@link Class} of the {@code javaValue}.
   * </p>
   * @param jdbcType The JDBC SQL Language data type of a column. Not null.
   * @param r2dbcType The R2DBC SQL Language data type of a column. Not null.
   * @param precision The precision of a column. May be {@code null}.
   * @param scale The scale of a column. May be {@code null}.
   * @param nullability The nullability of a column. Not {@code null}, may be
   * {@link Nullability#NULLABLE}.
   * @param javaType Class of the column's Java Language type. Not null.
   * @param javaValue A value that can be inserted into the column.
   */
  private void verifyColumnMetadata(
    Connection connection, String columnDdl, SQLType jdbcType,
    Type r2dbcType, Integer precision, Integer scale, Nullability nullability,
    Class<?> javaType, Object javaValue) {
    String columnName = "test_123";
    String tableName = "verify_" + columnDdl.replaceAll("[^\\p{Alnum}]", "_");
    awaitExecution(connection.createStatement(
      String.format("CREATE TABLE "+tableName+"(%s %s)",
        columnName, columnDdl)
    ));
    try {
      awaitUpdate(1, connection.createStatement(
        "INSERT INTO "+tableName+" VALUES (?)")
        .bind(0, javaValue));

      ColumnMetadata metadata = awaitOne(Flux.from(connection.createStatement(
        "SELECT * FROM "+tableName)
        .execute())
        .flatMap(result ->
          result.map((row, rowMetadata) -> rowMetadata.getColumnMetadata(0))
        ));

      assertEquals(javaType, metadata.getJavaType());
      // Don't expect Oracle R2DBC to match the column name's case.
      assertEquals(columnName.toUpperCase(), metadata.getName().toUpperCase());
      assertEquals(jdbcType, metadata.getNativeTypeMetadata());
      assertEquals(r2dbcType, metadata.getType());
      assertEquals(nullability, metadata.getNullability());
      assertEquals(precision, metadata.getPrecision());
      assertEquals(scale, metadata.getScale());
    }
    finally {
      awaitExecution(connection.createStatement("DROP TABLE "+tableName));
    }
  }

}
