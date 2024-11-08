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
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.Type;
import oracle.r2dbc.OracleR2dbcObject;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.r2dbc.OracleR2dbcTypes.ArrayType;
import oracle.r2dbc.OracleR2dbcTypes.ObjectType;
import oracle.r2dbc.test.TestUtils;
import oracle.sql.VECTOR;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.RowId;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.r2dbc.spi.R2dbcType.NCHAR;
import static io.r2dbc.spi.R2dbcType.NVARCHAR;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.databaseVersion;
import static oracle.r2dbc.test.DatabaseConfig.jdbcVersion;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.test.DatabaseConfig.sqlTimeout;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitUpdate;
import static oracle.r2dbc.util.Awaits.tryAwaitExecution;
import static oracle.r2dbc.util.Awaits.tryAwaitNone;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <p>
 * This test class verifies the Oracle R2DBC Driver's implementation of SQL
 * to Java and Java to SQL type mappings. The type mapping implementation is
 * exposed through the {@link Row} and {@link Statement} SPIs. The
 * {@link Row#get(int)} method is expected to implement SQL to Java type
 * conversion, and the {@link Statement#bind(int, Object)} method is expected
 * to implement Java to SQL type conversion.
 * </p><p>
 * For SQL types which the Oracle Database supports, the Oracle R2DBC Driver is
 * expected to implement all type conversions listed in
 * <a href="https://r2dbc.io/spec/0.8.2.RELEASE/spec/html/#datatypes.mapping">
 * Section 12 of the R2DBC 0.8.2 Specification.
 * </a>
 * </p><p>
 * SQL types which the Oracle Database does not support, such as BOOLEAN, are
 * not verified by this test class. Non-standard types which the Oracle
 * Database does support, like JSON, are verified by this test class.
 * </p>
 */
public class TypeMappingTest {

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for character data types. The Oracle R2DBC Driver is expected to
   * implement the type mappings listed in
   * <a href="https://r2dbc.io/spec/0.8.3.RELEASE/spec/html/#datatypes.mapping">
   * Table 4 of Section 12 of the R2DBC 0.8.3 Specification.
   * </a>
   * </p><p>
   * This test method makes use of {@link io.r2dbc.spi.R2dbcType#NCHAR} and
   * {@link io.r2dbc.spi.R2dbcType#NVARCHAR} when binding Strings that contain
   * non-ascii characters. By default, a String bind is mapped to the VARCHAR
   * SQL type. This default mapping has the driver encode the value using the
   * database character set. The database character set may not support
   * non-ascii characters. Binding Strings with the NCHAR/NVARCHAR type
   * configures the driver to encode the string using the national character set
   * of the database. The national character set is either UTF16 or UTF8, and so
   * it must support non-ascii characters.
   * </p>
   */
  @Test
  public void testCharacterTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {

      // Expect CHAR and String to map
      verifyTypeMapping(connection,
        format("%100s", "Hello, Oracle"), "CHAR(100)");

      // Expect VARCHAR and String to map
      verifyTypeMapping(connection, "Bonjour, Oracle", "VARCHAR(100)");

      // Expect NCHAR and String to map
      verifyTypeMapping(connection,
        Parameters.in(NCHAR, format("%100s", "你好, Oracle")),
        "NCHAR(100)",
        (expected, actual) ->
          assertEquals(expected.getValue(), actual));

      // Expect NVARCHAR and String to map. The Oracle type named "NVARCHAR2" is
      // equivalent to the standard type named "NVARCHAR"
      verifyTypeMapping(connection,
        Parameters.in(NVARCHAR, "नमस्कार, Oracle"),
        "NVARCHAR2(100)",
        (expected, actual) ->
          assertEquals(expected.getValue(), actual));

      // Expect CLOB and String to map
      verifyTypeMapping(connection, "Hola, Oracle", "CLOB");

      // Expect CLOB and io.r2dbc.spi.Clob to map
      verifyTypeMapping(connection,
        Clob.from(Mono.just("Hola, Oracle")), "CLOB",
        row -> row.get(0, Clob.class),
        (expected, actual) ->
          assertEquals("Hola, Oracle", clobToString(actual)));

      // Expect NCLOB and String to map for bind values, but not for row values.
      // For row values, expect Oracle CLOB to be mapped to io.r2dbc.spi.Clob
      verifyTypeMapping(connection,
        Parameters.in(NVARCHAR, "こんにちは, Oracle"),
        "NCLOB",
        (expected, actual) ->
          assertEquals(expected.getValue(), actual));

      // Expect NCLOB and io.r2dbc.spi.Clob to map
      verifyTypeMapping(connection,
        Clob.from(Mono.just("こんにちは, Oracle")), "NCLOB",
        row -> row.get(0, Clob.class),
        (expected, actual) ->
          assertEquals("こんにちは, Oracle", clobToString(actual)));

      // Expect LONG and String to map. LONG is variable length character
      // data, equivalent to VARCHAR
      verifyTypeMapping(connection,
        Stream.generate(() -> "Aloha, Oracle")
          .limit(1_000)
          .collect(Collectors.joining()),
        "LONG");
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for binary data types. The Oracle R2DBC Driver is expected to implement
   * the type mappings listed in
   * <a href="https://r2dbc.io/spec/0.8.3.RELEASE/spec/html/#datatypes.mapping">
   * Table 6 of Section 12 of the R2DBC 0.8.3 Specification.
   * </a>
   * </p><p>
   * The Oracle Database does not support a fixed length binary type, like
   * BINARY, so the Oracle R2DBC Driver is not expected to support this
   * mapping.
   *</p>
   */
  @Test
  public void testBinaryTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Buffer holds the set of all 8 bit values. Use allocateDirect to
      // catch any code that assumes ByteBuffer.hasArray().
      ByteBuffer byteBuffer = ByteBuffer.allocateDirect(0x100);
      for (int value = 0; value <= 0xFF; value++)
        byteBuffer.put((byte)value);

      // Expect Oracle R2DBC to never mutate the buffer's position, limit,
      // or mark.
      byteBuffer.clear();

      // Expect VARBINARY and ByteBuffer to map. The Oracle type named
      // "RAW" is equivalent to the standard type named "VARBINARY".
      verifyTypeMapping(connection, byteBuffer, "RAW(1000)");

      // Expect LONG RAW and ByteBuffer to map. The Oracle type named
      // "LONG RAW" is equivalent to the standard type named "VARBINARY".
      verifyTypeMapping(connection, byteBuffer, "LONG RAW");

      // Expect BLOB and ByteBuffer to map for bind values, but not for row
      // values. For row values, expect Oracle BLOB to be mapped to
      // io.r2dbc.spi.Blob
      verifyTypeMapping(connection, byteBuffer, "BLOB");

      // Expect BLOB and io.r2dbc.spi.Blob to map
      verifyTypeMapping(connection,
        Blob.from(Mono.just(byteBuffer)), "BLOB",
        row -> row.get(0, Blob.class),
        (expected, actual) ->
          assertEquals(byteBuffer, blobToByteBuffer(actual)));

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for numeric data types. The Oracle R2DBC Driver is expected to implement
   * the type mappings listed in
   * <a href="https://r2dbc.io/spec/0.8.3.RELEASE/spec/html/#datatypes.mapping">
   * Table 7 of Section 12 of the R2DBC 0.8.3 Specification.
   * </a>
   * </p><p>
   * The Oracle Database does not support INTEGER, TINYINT, SMALLINT, BIGINT,
   * REAL, or DOUBLE PRECISION so the Oracle R2DBC Driver is not expected to
   * support these mappings.
   *</p>
   */
  @Test
  public void testNumericTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // This value has 39 decimal digits, which is the maximum precision of
      // Oracle's NUMBER type.
      BigDecimal pi =
        new BigDecimal("3.14159265358979323846264338327950288419");

      // Expect DECIMAL/NUMERIC and BigDecimal to map. The Oracle type named
      // "NUMBER" is equivalent to the standard types named "DECIMAL" and
      // "NUMERIC".
      verifyTypeMapping(connection, pi, "NUMBER");

      if (jdbcVersion() == 23) {
        // Bug #34545424 is present in the 23.3.0.23.09 release of Oracle JDBC.
        // This bug causes a loss of precision when binding a double with more
        // than 14 significant decimal digits after the decimal point.
        pi = new BigDecimal("3.14159265358979");
      }

      // Expect FLOAT and Double to map.
      verifyTypeMapping(connection, pi.doubleValue(), "FLOAT");

      // Expect BINARY_FLOAT and Float to map.
      verifyTypeMapping(connection, pi.floatValue(), "BINARY_FLOAT");

      // Expect BINARY_DOUBLE and Double to map.
      verifyTypeMapping(connection, pi.doubleValue(), "BINARY_DOUBLE");

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for datetime data types. The Oracle R2DBC Driver is expected to implement
   * the type mappings listed in
   * <a href="https://r2dbc.io/spec/0.8.3.RELEASE/spec/html/#datatypes.mapping">
   * Table 8 of Section 12 of the R2DBC 0.8.3 Specification.
   * </a>
   * </p><p>
   * The Oracle Database does not support the standard SQL types named DATE,
   * TIME, or TIME WITH TIME ZONE so the Oracle R2DBC Driver is not expected
   * to support mappings for the standard SQL types with these names.
   * </p><p>
   * Note that Oracle Database does support a type which is named "DATE", but
   * this type is not equivalent to the standard SQL type of the same name. An
   * Oracle DATE stores a value of years, months, days, hours, minutes, and
   * seconds. The standard SQL DATE does not store hours, minutes, or
   * seconds, so the standard type is not equivalent to the Oracle type. The
   * Oracle R2DBC Driver is expected to map Oracle DATE with LocalDateTime,
   * which is the expected mapping for the TIMESTAMP type, which is
   * equivalent to the Oracle DATE type.
   *</p>
   */
  @Test
  public void testDatetimeTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      OffsetDateTime dateTimeValue =
        OffsetDateTime.of(2038, 10, 23, 9, 42, 1, 1, ZoneOffset.ofHours(-5));

      // Expect Oracle's "DATE" and LocalDateTime to map.
      verifyTypeMapping(connection,
        dateTimeValue.toLocalDateTime().truncatedTo(ChronoUnit.SECONDS),
        "DATE");

      // Expect TIMESTAMP and LocalDateTime to map.
      verifyTypeMapping(connection,
        dateTimeValue.toLocalDateTime(), "TIMESTAMP(9)");

      // Expect TIMESTAMP WITH TIME ZONE and OffsetDateTime to map.
      verifyTypeMapping(connection,
        dateTimeValue, "TIMESTAMP(9) WITH TIME ZONE");

      // Expect TIMESTAMP WITH LOCAL TIME ZONE and LocalDateTime to map.
      verifyTypeMapping(connection,
        dateTimeValue.toLocalDateTime(),
        "TIMESTAMP(9) WITH LOCAL TIME ZONE");

      // Expect INTERVAL YEAR TO MONTH and Period to map. Note that Period
      // binds must have a zero number of days
      verifyTypeMapping(connection, Period.between(
        LocalDate.of(1977, Month.JUNE, 16), dateTimeValue.toLocalDate())
        .withDays(0),
        "INTERVAL YEAR(4) TO MONTH");

      // Expect INTERVAL DAY TO SECOND and Duration to map.
      verifyTypeMapping(connection,
        Duration.ofDays(1).withSeconds(9),
        "INTERVAL DAY TO SECOND");

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for row ID data types. The R2DBC 0.8.3 Specification does not contain
   * mapping guidelines for row ID data types. The Oracle R2DBC Driver is
   * expected to map these types to {@link java.sql.RowId}.
   *</p>
   */
  @Test
  public void testRowIdMappings() {
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

      // Expect ROWID and RowId to map.
      verifyTypeMapping(connection, rowId, "ROWID");

      // Expect UROWID and RowId to map.
      verifyTypeMapping(connection, rowId, "UROWID");
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for the JSON data type. The R2DBC 0.8.3 Specification does not contain
   * mapping guidelines for the JSON data type. The Oracle R2DBC Driver is
   * expected to map JSON to a {@link oracle.sql.json.OracleJsonObject} value.
   *</p>
   */
  @Test
  public void testJsonMapping() {

    // The JSON data type was introduced in Oracle Database version 21c, so this
    // test is skipped if the version is older than 21c.
    assumeTrue(databaseVersion() >= 21,
      "JSON columns are not supported by database versions older than 21");

    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      OracleJsonObject oracleJson = new OracleJsonFactory().createObject();
      oracleJson.put("species", "cat");
      oracleJson.put("birthday", OffsetDateTime.of(
        LocalDate.of(2009, 7, 12), LocalTime.NOON, ZoneOffset.UTC));
      oracleJson.put("weight", 9.2);

      // Expect JSON and OracleJsonObject to map.
      verifyTypeMapping(connection, oracleJson, "JSON");
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }
  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * where the Java type is {@link Boolean} and the SQL type is a numeric type.
   * The R2DBC 1.0.0 Specification only requires that Java {@code Boolean} be
   * mapped to the SQL BOOLEAN type, however Oracle Database did not support a
   * BOOLEAN column type until the 23ai release. To allow the use of the
   * {@code Boolean} bind values, Oracle JDBC supports binding the Boolean as a
   * NUMBER. Oracle R2DBC is expected to expose this functionality as well.
   *</p>
   */
  @Test
  public void testBooleanNumericMapping() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect NUMBER and Boolean to map, with Row.get(...) mapping the
      // NUMBER column value to BigDecimal
      verifyTypeMapping(connection, true, "NUMBER",
        (expected, actual) -> assertEquals(BigDecimal.ONE, actual));
      verifyTypeMapping(connection, false, "NUMBER",
        (expected, actual) -> assertEquals(BigDecimal.ZERO, actual));

      // Expect NUMBER and Boolean to map, with Row.get(..., Boolean.class)
      // mapping the NUMBER column value to Boolean
      verifyTypeMapping(connection, true, "NUMBER",
        row -> row.get(0, Boolean.class),
        (expected, actual) -> assertTrue(actual));
      verifyTypeMapping(connection, false, "NUMBER",
        row -> row.get(0, Boolean.class),
        (expected, actual) -> assertFalse(actual));
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * where the Java type is {@link Boolean} and the SQL type is BOOLEAN. Oracle
   * Database added support for a BOOLEAN column type in the 23ai release.
   *</p>
   */
  @Test
  public void testBooleanMapping() {
    assumeTrue(databaseVersion() >= 23,
      "BOOLEAN requires Oracle Database 23ai or newer");

    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect BOOLEAN and Boolean to map
      verifyTypeMapping(connection, true, "BOOLEAN",
        (expected, actual) -> assertEquals(Boolean.TRUE, actual));
      verifyTypeMapping(connection, false, "BOOLEAN",
        (expected, actual) -> assertEquals(Boolean.FALSE, actual));

      // Expect NUMBER and Boolean to map, with Row.get(..., Boolean.class)
      // mapping the NUMBER column value to Boolean
      verifyTypeMapping(connection, true, "BOOLEAN",
        row -> row.get(0, Boolean.class),
        (expected, actual) -> assertTrue(actual));
      verifyTypeMapping(connection, false, "BOOLEAN",
        row -> row.get(0, Boolean.class),
        (expected, actual) -> assertFalse(actual));
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * where the Java type is a {@code byte} array and the SQL type is RAW or
   * BLOB. The R2DBC 0.9.0 Specification does not require drivers to
   * support byte array mapping, however this mapping is required by
   * the JDBC 4.3 specification. Oracle R2DBC is expected to expose mappings
   * supported by Oracle JDBC.
   *</p>
   */
  @Test
  public void testByteArrayMapping() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      byte[] byteArray = new byte[100];

      for (int i = 0; i < byteArray.length; i++)
        byteArray[i] = (byte) i;

      // Expect RAW and byte[] to map, with Row.get(...) mapping the
      // RAW column value to ByteBuffer
      verifyTypeMapping(connection, byteArray, "RAW(100)",
        (expected, actual) -> assertEquals(ByteBuffer.wrap(byteArray), actual));

      // Expect RAW and byte[] to map, with Row.get(..., byte[].class)
      // mapping the RAW column value to byte[]
      verifyTypeMapping(connection, byteArray, "RAW(100)",
        row -> row.get(0, byte[].class),
        (expected, actual) -> assertArrayEquals(byteArray, actual));
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for ARRAY types. The Oracle R2DBC Driver is expected to implement the type
   * mapping listed in
   * <a href="https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/#datatypes.mapping.collection">
   * Table 9 of Section 14 of the R2DBC 1.0.0 Specification.
   * </a>
   * </p><p>
   * An ARRAY is expected to map to the Java type mapping of it's element type.
   * An ARRAY of VARCHAR should map to a Java array of String.
   * </p>
   */
  @Test
  public void testArrayCharacterTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      ArrayType arrayType1D = OracleR2dbcTypes.arrayType("CHARACTER_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE CHARACTER_ARRAY" +
            " AS ARRAY(10) OF VARCHAR(100)")
        .execute());
      ArrayType arrayType2D = OracleR2dbcTypes.arrayType("CHARACTER_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE CHARACTER_ARRAY_2D" +
            " AS ARRAY(10) OF CHARACTER_ARRAY")
        .execute());
      ArrayType arrayType3D = OracleR2dbcTypes.arrayType("CHARACTER_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE CHARACTER_ARRAY_3D" +
            " AS ARRAY(10) OF CHARACTER_ARRAY_2D")
        .execute());

      // Expect ARRAY of VARCHAR and String[] to map
      String[] strings = {"Hello", "Bonjour", "你好", null};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, String[].class,
        i ->
          Arrays.stream(strings)
            .map(string -> string == null ? null : i + "-" + string)
            .toArray(String[]::new),
        true,
        Assertions::assertArrayEquals);
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE CHARACTER_ARRAY_3D"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE CHARACTER_ARRAY_2D"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE CHARACTER_ARRAY"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for ARRAY types. The Oracle R2DBC Driver is expected to implement the type
   * mapping listed in
   * <a href="https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/#datatypes.mapping.collection">
   * Table 9 of Section 14 of the R2DBC 1.0.0 Specification.
   * </a>
   * </p><p>
   * An ARRAY is expected to map to the Java type mapping of it's element type.
   * An ARRAY of NUMBER should map to a Java array of BigDecimal, Byte, Short,
   * Integer, Long, Float, and Double.
   * </p>
   */
  @Test
  public void testArrayNumericTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {

      ArrayType arrayType1D =
        OracleR2dbcTypes.arrayType("NUMBER_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE NUMBER_ARRAY" +
            " AS ARRAY(10) OF NUMBER")
        .execute());

      ArrayType arrayType2D =
        OracleR2dbcTypes.arrayType("NUMBER_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE NUMBER_ARRAY_2D" +
            " AS ARRAY(10) OF NUMBER_ARRAY")
        .execute());

      ArrayType arrayType3D =
        OracleR2dbcTypes.arrayType("NUMBER_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE NUMBER_ARRAY_3D" +
            " AS ARRAY(10) OF NUMBER_ARRAY_2D")
        .execute());

      // Expect ARRAY of NUMBER and BigDecimal to map
      BigDecimal[] bigDecimals = new BigDecimal[] {
        BigDecimal.ZERO,
        new BigDecimal("1.23"),
        new BigDecimal("4.56"),
        new BigDecimal("7.89"),
        null
      };
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, BigDecimal[].class,
        i ->
          Arrays.stream(bigDecimals)
            .map(bigDecimal ->
              bigDecimal == null
                ? null
                :bigDecimal.add(BigDecimal.valueOf(i)))
            .toArray(BigDecimal[]::new),
        true,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and byte[] to map
      byte[] bytes = new byte[]{1,2,3,4,5};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, byte[].class,
        i -> {
          byte[] moreBytes = new byte[bytes.length];
          for (int j = 0; j < bytes.length; j++)
            moreBytes[j] = (byte)(bytes[j] + i);
          return moreBytes;
        },
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and short[] to map
      short[] shorts = new short[]{1,2,3,4,5};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, short[].class,
        i -> {
          short[] moreShorts = new short[shorts.length];
          for (int j = 0; j < shorts.length; j++)
            moreShorts[j] = (short)(shorts[j] + i);
          return moreShorts;
        },
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and int[] to map
      int[] ints = {1,2,3,4,5};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, int[].class,
        i -> {
          int[] moreInts = new int[ints.length];
          for (int j = 0; j < ints.length; j++)
            moreInts[j] = (ints[j] + i);
          return moreInts;
        },
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and long[] to map
      long[] longs = {1,2,3,4,5};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, long[].class,
        i -> {
          long[] moreLongs = new long[longs.length];
          for (int j = 0; j < longs.length; j++)
            moreLongs[j] = longs[j] + i;
          return moreLongs;
        },
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and float[] to map
      float[] floats = {1.1f,2.2f,3.3f,4.4f,5.5f};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, float[].class,
        i -> {
          float[] moreFloats = new float[floats.length];
          for (int j = 0; j < floats.length; j++)
            moreFloats[j] = (floats[j] + (float)i);
          return moreFloats;
        },
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and double[] to map
      double[] doubles = {1.1,2.2,3.3,4.4,5.5};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, double[].class,
        i -> {
          double[] moreDoubles = new double[doubles.length];
          for (int j = 0; j < doubles.length; j++)
            moreDoubles[j] = (doubles[j] + (double)i);
          return moreDoubles;
        },
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and Byte[] to map
      Byte[] byteObjects = new Byte[]{1,2,3,4,5,null};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, Byte[].class,
        i ->
          Arrays.stream(byteObjects)
            .map(byteObject ->
              byteObject == null ? null : (byte)(byteObject + i))
            .toArray(Byte[]::new),
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and Short[] to map
      Short[] shortObjects = new Short[]{1,2,3,4,5,null};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, Short[].class,
        i ->
          Arrays.stream(shortObjects)
            .map(shortObject ->
              shortObject == null ? null : (short)(shortObject + i))
            .toArray(Short[]::new),
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and Integer[] to map
      Integer[] intObjects = {1,2,3,4,5,null};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, Integer[].class,
        i ->
          Arrays.stream(intObjects)
            .map(intObject ->
              intObject == null ? null : intObject + i)
            .toArray(Integer[]::new),
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and Long[] to map
      Long[] longObjects = {1L,2L,3L,4L,5L,null};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, Long[].class,
        i ->
          Arrays.stream(longObjects)
            .map(longObject ->
              longObject == null ? null : longObject + i)
            .toArray(Long[]::new),
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and Float[] to map
      Float[] floatObjects = {1.1f,2.2f,3.3f,4.4f,5.5f,null};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, Float[].class,
        i ->
          Arrays.stream(floatObjects)
            .map(floatObject ->
              floatObject == null ? null : floatObject + i)
            .toArray(Float[]::new),
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and Double[] to map
      Double[] doubleObjects = {1.1,2.2,3.3,4.4,5.5,null};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D, Double[].class,
        i ->
          Arrays.stream(doubleObjects)
            .map(doubleObject ->
              doubleObject == null ? null : doubleObject + i)
            .toArray(Double[]::new),
        false,
        Assertions::assertArrayEquals);
    }
    finally {
      tryAwaitExecution(connection.createStatement("DROP TYPE NUMBER_ARRAY_3D"));
      tryAwaitExecution(connection.createStatement("DROP TYPE NUMBER_ARRAY_2D"));
      tryAwaitExecution(connection.createStatement("DROP TYPE NUMBER_ARRAY"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for ARRAY types. The Oracle R2DBC Driver is expected to implement the type
   * mapping listed in
   * <a href="https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/#datatypes.mapping.collection">
   * Table 9 of Section 14 of the R2DBC 1.0.0 Specification.
   * </a>
   * </p><p>
   * An ARRAY is expected to map to the Java type mapping of it's element type.
   * An ARRAY of NUMBER should map to a Java array of boolean.
   * </p>
   */
  @Test
  public void testArrayBooleanTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      ArrayType arrayType1D = OracleR2dbcTypes.arrayType("BOOLEAN_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE BOOLEAN_ARRAY" +
            " AS ARRAY(10) OF NUMBER")
        .execute());
      ArrayType arrayType2D = OracleR2dbcTypes.arrayType("BOOLEAN_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE BOOLEAN_ARRAY_2D" +
            " AS ARRAY(10) OF BOOLEAN_ARRAY")
        .execute());
      ArrayType arrayType3D = OracleR2dbcTypes.arrayType("BOOLEAN_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE BOOLEAN_ARRAY_3D" +
            " AS ARRAY(10) OF BOOLEAN_ARRAY_2D")
        .execute());

      // Expect ARRAY of NUMBER and boolean[] to map
      boolean[] booleans = {true, false, false, true};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D,
        boolean[].class,
        i -> {
          boolean[] moreBooleans = new boolean[booleans.length];
          for (int j = 0; j < booleans.length; j++)
            moreBooleans[j] = booleans[i % booleans.length];
          return moreBooleans;
        },
        false,
        Assertions::assertArrayEquals);

      // Expect ARRAY of NUMBER and Boolean[] to map
      Boolean[] booleanObjects = {true, false, false, true, null};
      verifyArrayTypeMapping(
        connection, arrayType1D, arrayType2D, arrayType3D,
        Boolean[].class,
        i -> {
          Boolean[] moreBooleans = new Boolean[booleanObjects.length];
          for (int j = 0; j < booleanObjects.length; j++)
            moreBooleans[j] = booleanObjects[i % booleanObjects.length];
          return moreBooleans;
        },
        false,
        Assertions::assertArrayEquals);
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE BOOLEAN_ARRAY_3D"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE BOOLEAN_ARRAY_2D"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE BOOLEAN_ARRAY"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for ARRAY types. The Oracle R2DBC Driver is expected to implement the type
   * mapping listed in
   * <a href="https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/#datatypes.mapping.collection">
   * Table 9 of Section 14 of the R2DBC 1.0.0 Specification.
   * </a>
   * </p><p>
   * An ARRAY is expected to map to the Java type mapping of it's element type.
   * An ARRAY of RAW should map to a Java array of ByteBuffer.
   * </p>
   */
  @Test
  public void testArrayBinaryTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      ArrayType arrayType1D =  OracleR2dbcTypes.arrayType("BINARY_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE BINARY_ARRAY" +
            " AS ARRAY(10) OF RAW(100)")
        .execute());
      ArrayType arrayType2D =  OracleR2dbcTypes.arrayType("BINARY_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE BINARY_ARRAY_2D" +
            " AS ARRAY(10) OF BINARY_ARRAY")
        .execute());
      ArrayType arrayType3D =  OracleR2dbcTypes.arrayType("BINARY_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE BINARY_ARRAY_3D" +
            " AS ARRAY(10) OF BINARY_ARRAY_2D")
        .execute());

      // Expect ARRAY of RAW and ByteBuffer[] to map
      ByteBuffer[] byteBuffers = {
        ByteBuffer.allocate(3 * Integer.BYTES)
          .putInt(0).putInt(1).putInt(2).clear(),
        ByteBuffer.allocate(3 * Integer.BYTES)
          .putInt(3).putInt(4).putInt(5).clear(),
        ByteBuffer.allocate(3 * Integer.BYTES)
          .putInt(6).putInt(7).putInt(8).clear(),
        null
      };
      verifyArrayTypeMapping(connection,
        arrayType1D, arrayType2D, arrayType3D,
        ByteBuffer[].class,
        i -> Arrays.stream(byteBuffers)
          .map(byteBuffer ->
            byteBuffer == null
              ? null
              : ByteBuffer.allocate(3 * Integer.BYTES)
                  .putInt(byteBuffer.get(0) + i)
                  .putInt(byteBuffer.get(1) + i)
                  .putInt(byteBuffer.get(2) + i)
                  .clear())
          .toArray(ByteBuffer[]::new),
        true,
        Assertions::assertArrayEquals);
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE BINARY_ARRAY_3D"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE BINARY_ARRAY_2D"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE BINARY_ARRAY"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for ARRAY types. The Oracle R2DBC Driver is expected to implement the type
   * mapping listed in
   * <a href="https://r2dbc.io/spec/1.0.0.RELEASE/spec/html/#datatypes.mapping.collection">
   * Table 9 of Section 14 of the R2DBC 1.0.0 Specification.
   * </a>
   * </p><p>
   * An ARRAY is expected to map to the Java type mapping of it's element type.
   * An ARRAY of DATE should map to a Java array of LocalDateTime (Oracle DATE
   * values have a time component).
   * An ARRAY of TIMESTAMP should map to a Java array of LocalDateTime
   * An ARRAY of TIMESTAMP WITH TIME ZONE should map to a Java array of
   * OffsetDateTime.
   * An ARRAY of TIMESTAMP WITH LOCAL TIME ZONE should map to a Java array of
   * LocalDateTime.
   * An ARRAY of INTERVAL YEAR TO MONTH should map to a Java array of Period
   * An ARRAY of INTERVAL DAY TO SECOND should map to a Java array of Duration
   * </p>
   */
  @Test
  public void testArrayDatetimeTypeMappings() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      OffsetDateTime dateTimeValue =
        OffsetDateTime.of(2038, 10, 23, 9, 42, 1, 1, ZoneOffset.ofHours(-5));

      ArrayType dateArrayType1D = OracleR2dbcTypes.arrayType("DATE_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE DATE_ARRAY" +
            " AS ARRAY(10) OF DATE")
        .execute());
      ArrayType dateArrayType2D = OracleR2dbcTypes.arrayType("DATE_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE DATE_ARRAY_2D" +
            " AS ARRAY(10) OF DATE_ARRAY")
        .execute());
      ArrayType dateArrayType3D = OracleR2dbcTypes.arrayType("DATE_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE DATE_ARRAY_3D" +
            " AS ARRAY(10) OF DATE_ARRAY_2D")
        .execute());
      try {

        // Expect ARRAY of DATE and LocalDateTime[] to map
        LocalDateTime[] localDateTimes = {
          dateTimeValue.minus(Period.ofDays(7))
            .toLocalDateTime()
            .truncatedTo(ChronoUnit.SECONDS),
          dateTimeValue.toLocalDateTime()
            .truncatedTo(ChronoUnit.SECONDS),
          dateTimeValue.plus(Period.ofDays(7))
            .toLocalDateTime()
            .truncatedTo(ChronoUnit.SECONDS),
          null
        };
        verifyArrayTypeMapping(
          connection, dateArrayType1D, dateArrayType2D, dateArrayType3D,
          LocalDateTime[].class,
          i ->
            Arrays.stream(localDateTimes)
              .map(localDateTime ->
                localDateTime == null
                  ? null
                  : localDateTime.plus(Period.ofDays(i)))
              .toArray(LocalDateTime[]::new),
          true,
          Assertions::assertArrayEquals);

        // Expect ARRAY of DATE and LocalDate[] to map
        LocalDate[] localDates =
          Arrays.stream(localDateTimes)
            .map(localDateTime ->
              localDateTime == null ? null : localDateTime.toLocalDate())
            .toArray(LocalDate[]::new);
        verifyArrayTypeMapping(
          connection, dateArrayType1D, dateArrayType2D, dateArrayType3D,
          LocalDate[].class,
          i ->
            Arrays.stream(localDates)
              .map(localDate ->
                localDate == null
                  ? null
                  : localDate.plus(Period.ofDays(i)))
              .toArray(LocalDate[]::new),
          false,
          Assertions::assertArrayEquals);

        // Expect ARRAY of DATE and LocalTime[] to map
        LocalTime[] localTimes =
          Arrays.stream(localDateTimes)
            .map(localDateTime ->
              localDateTime == null ? null : localDateTime.toLocalTime())
            .toArray(LocalTime[]::new);
        verifyArrayTypeMapping(
          connection, dateArrayType1D, dateArrayType2D, dateArrayType3D,
          LocalTime[].class,
          i ->
            Arrays.stream(localTimes)
              .map(localTime ->
                localTime == null
                  ? null
                  : localTime.plus(Duration.ofMinutes(i)))
              .toArray(LocalTime[]::new),
          false,
          Assertions::assertArrayEquals);
      }
      finally {
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE DATE_ARRAY_3D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE DATE_ARRAY_2D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE DATE_ARRAY"));
      }


      // Expect ARRAY of TIMESTAMP and LocalDateTime[] to map
      ArrayType timestampArrayType1D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_ARRAY" +
            " AS ARRAY(10) OF TIMESTAMP")
        .execute());
      ArrayType timestampArrayType2D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_ARRAY_2D" +
            " AS ARRAY(10) OF TIMESTAMP_ARRAY")
        .execute());
      ArrayType timestampArrayType3D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_ARRAY_3D" +
            " AS ARRAY(10) OF TIMESTAMP_ARRAY_2D")
        .execute());
      try {

        // Expect ARRAY of TIMESTAMP and LocalDateTime[] to map
        LocalDateTime[] localDateTimes = {
          dateTimeValue.minus(Duration.ofMillis(100))
            .toLocalDateTime(),
          dateTimeValue.toLocalDateTime(),
          dateTimeValue.plus(Duration.ofMillis(100))
            .toLocalDateTime(),
          null
        };
        verifyArrayTypeMapping(
          connection,
          timestampArrayType1D,
          timestampArrayType2D,
          timestampArrayType3D,
          LocalDateTime[].class,
          i ->
            Arrays.stream(localDateTimes)
              .map(localDateTime ->
                localDateTime == null
                  ? null
                  : localDateTime.plus(Period.ofDays(i)))
              .toArray(LocalDateTime[]::new),
          true,
          Assertions::assertArrayEquals);

        // Expect ARRAY of TIMESTAMP and LocalDate[] to map
        LocalDate[] localDates =
          Arrays.stream(localDateTimes)
            .map(localDateTime ->
              localDateTime == null ? null : localDateTime.toLocalDate())
            .toArray(LocalDate[]::new);
        verifyArrayTypeMapping(
          connection, timestampArrayType1D, timestampArrayType2D, timestampArrayType3D,
          LocalDate[].class,
          i ->
            Arrays.stream(localDates)
              .map(localDate ->
                localDate == null
                  ? null
                  : localDate.plus(Period.ofDays(i)))
              .toArray(LocalDate[]::new),
          false,
          Assertions::assertArrayEquals);

        // Expect ARRAY of TIMESTAMP and LocalTime[] to map
        LocalTime[] localTimes =
          Arrays.stream(localDateTimes)
            .map(localDateTime ->
              localDateTime == null ? null : localDateTime.toLocalTime())
            .toArray(LocalTime[]::new);
        verifyArrayTypeMapping(
          connection, timestampArrayType1D, timestampArrayType2D, timestampArrayType3D,
          LocalTime[].class,
          i ->
            Arrays.stream(localTimes)
              .map(localTime ->
                localTime == null
                  ? null
                  : localTime.plus(Duration.ofMinutes(i)))
              .toArray(LocalTime[]::new),
          false,
          Assertions::assertArrayEquals);
      }
      finally {
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_ARRAY_3D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_ARRAY_2D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_ARRAY"));
      }

      ArrayType timestampWithTimeZoneArrayType1D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_WITH_TIME_ZONE_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_WITH_TIME_ZONE_ARRAY" +
            " AS ARRAY(10) OF TIMESTAMP WITH TIME ZONE")
        .execute());
      ArrayType timestampWithTimeZoneArrayType2D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_WITH_TIME_ZONE_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_WITH_TIME_ZONE_ARRAY_2D" +
            " AS ARRAY(10) OF TIMESTAMP_WITH_TIME_ZONE_ARRAY")
        .execute());
      ArrayType timestampWithTimeZoneArrayType3D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_WITH_TIME_ZONE_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_WITH_TIME_ZONE_ARRAY_3D" +
            " AS ARRAY(10) OF TIMESTAMP_WITH_TIME_ZONE_ARRAY_2D")
        .execute());
      try {
        // Expect ARRAY of TIMESTAMP WITH TIME ZONE and OffsetDateTime[] to map
        OffsetDateTime[] offsetDateTimes = {
          dateTimeValue.minus(Duration.ofMillis(100))
            .toLocalDateTime()
            .atOffset(ZoneOffset.ofHours(-8)),
          dateTimeValue.toLocalDateTime()
            .atOffset(ZoneOffset.ofHours(0)),
          dateTimeValue.plus(Duration.ofMillis(100))
            .toLocalDateTime()
            .atOffset(ZoneOffset.ofHours(8)),
          null
        };
        verifyArrayTypeMapping(
          connection,
          timestampWithTimeZoneArrayType1D,
          timestampWithTimeZoneArrayType2D,
          timestampWithTimeZoneArrayType3D,
          OffsetDateTime[].class,
          i ->
            Arrays.stream(offsetDateTimes)
              .map(offsetDateTime ->
                offsetDateTime == null
                  ? null
                  : offsetDateTime.plus(Duration.ofMinutes(i)))
              .toArray(OffsetDateTime[]::new),
          true,
          Assertions::assertArrayEquals);
      }
      finally {
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_WITH_TIME_ZONE_ARRAY_3D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_WITH_TIME_ZONE_ARRAY_2D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_WITH_TIME_ZONE_ARRAY"));
      }

      ArrayType timestampWithLocalTimeZoneArrayType1D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY" +
            " AS ARRAY(10) OF TIMESTAMP WITH LOCAL TIME ZONE")
        .execute());
      ArrayType timestampWithLocalTimeZoneArrayType2D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY_2D" +
            " AS ARRAY(10) OF TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY")
        .execute());
      ArrayType timestampWithLocalTimeZoneArrayType3D =
        OracleR2dbcTypes.arrayType("TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY_3D" +
            " AS ARRAY(10) OF TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY_2D")
        .execute());
      try {
        // Expect ARRAY of TIMESTAMP WITH LOCAL TIME ZONE and LocalDateTime[] to
        // map
        LocalDateTime[] localDateTimes = {
          dateTimeValue.minus(Duration.ofMillis(100))
            .toLocalDateTime(),
          dateTimeValue.toLocalDateTime(),
          dateTimeValue.plus(Duration.ofMillis(100))
            .toLocalDateTime(),
          null
        };
        verifyArrayTypeMapping(
          connection,
          timestampWithLocalTimeZoneArrayType1D,
          timestampWithLocalTimeZoneArrayType2D,
          timestampWithLocalTimeZoneArrayType3D,
          LocalDateTime[].class,
          i ->
            Arrays.stream(localDateTimes)
              .map(localDateTime ->
                localDateTime == null
                  ? null
                  : localDateTime.plus(Period.ofDays(i)))
              .toArray(LocalDateTime[]::new),
          true,
          Assertions::assertArrayEquals);
      }
      finally {
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY_3D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY_2D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE TIMESTAMP_WITH_LOCAL_TIME_ZONE_ARRAY"));
      }

      ArrayType intervalYearToMonthArrayType1D =
        OracleR2dbcTypes.arrayType("INTERVAL_YEAR_TO_MONTH_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE INTERVAL_YEAR_TO_MONTH_ARRAY" +
            " AS ARRAY(10) OF INTERVAL YEAR TO MONTH")
        .execute());
      ArrayType intervalYearToMonthArrayType2D =
        OracleR2dbcTypes.arrayType("INTERVAL_YEAR_TO_MONTH_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE INTERVAL_YEAR_TO_MONTH_ARRAY_2D" +
            " AS ARRAY(10) OF INTERVAL_YEAR_TO_MONTH_ARRAY")
        .execute());
      ArrayType intervalYearToMonthArrayType3D =
        OracleR2dbcTypes.arrayType("INTERVAL_YEAR_TO_MONTH_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE INTERVAL_YEAR_TO_MONTH_ARRAY_3D" +
            " AS ARRAY(10) OF INTERVAL_YEAR_TO_MONTH_ARRAY_2D")
        .execute());
      try {

        // Expect ARRAY of INTERVAL YEAR TO MONTH and Period[] to map
        Period[] periods = {
          Period.of(1, 2, 0),
          Period.of(3, 4, 0),
          Period.of(5, 6, 0),
          null
        };

        verifyArrayTypeMapping(
          connection,
          intervalYearToMonthArrayType1D,
          intervalYearToMonthArrayType2D,
          intervalYearToMonthArrayType3D,
          Period[].class,
          i ->
            Arrays.stream(periods)
              .map(period ->
                period == null
                  ? null
                  : period.plus(Period.ofYears(i)))
              .toArray(Period[]::new),
          true,
          Assertions::assertArrayEquals);
      }
      finally {
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE INTERVAL_YEAR_TO_MONTH_ARRAY_3D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE INTERVAL_YEAR_TO_MONTH_ARRAY_2D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE INTERVAL_YEAR_TO_MONTH_ARRAY"));
      }

      ArrayType intervalDayToSecondArrayType1D =
        OracleR2dbcTypes.arrayType("INTERVAL_DAY_TO_SECOND_ARRAY");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE INTERVAL_DAY_TO_SECOND_ARRAY" +
            " AS ARRAY(10) OF INTERVAL DAY TO SECOND")
        .execute());
      ArrayType intervalDayToSecondArrayType2D =
        OracleR2dbcTypes.arrayType("INTERVAL_DAY_TO_SECOND_ARRAY_2D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE INTERVAL_DAY_TO_SECOND_ARRAY_2D" +
            " AS ARRAY(10) OF INTERVAL_DAY_TO_SECOND_ARRAY")
        .execute());
      ArrayType intervalDayToSecondArrayType3D =
        OracleR2dbcTypes.arrayType("INTERVAL_DAY_TO_SECOND_ARRAY_3D");
      awaitOne(connection.createStatement(
          "CREATE OR REPLACE TYPE INTERVAL_DAY_TO_SECOND_ARRAY_3D" +
            " AS ARRAY(10) OF INTERVAL_DAY_TO_SECOND_ARRAY_2D")
        .execute());
      try {
        Duration[] durations = {
          Duration.ofDays(1),
          Duration.ofHours(2),
          Duration.ofMinutes(3),
          Duration.ofSeconds(4),
          null
        };
        verifyArrayTypeMapping(
          connection,
          intervalDayToSecondArrayType1D,
          intervalDayToSecondArrayType2D,
          intervalDayToSecondArrayType3D,
          Duration[].class,
          i ->
          Arrays.stream(durations)
            .map(duration ->
              duration == null
                ? null
                : duration.plus(Duration.ofSeconds(i)))
            .toArray(Duration[]::new),
          true,
          Assertions::assertArrayEquals);
      }
      finally {
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE INTERVAL_DAY_TO_SECOND_ARRAY_3D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE INTERVAL_DAY_TO_SECOND_ARRAY_2D"));
        tryAwaitExecution(connection.createStatement(
          "DROP TYPE INTERVAL_DAY_TO_SECOND_ARRAY"));
      }
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for OBJECT types. The current R2DBC SPI does not specify a standard mapping
   * for database OBJECT types. Oracle R2DBC is expected to support Object[]
   * mappings.
   * </p>
   */
  @Test
  public void testObjectTypeMappings() {
    Connection connection = awaitOne(sharedConnection());
    try {

      // Verify default mappings of all SQL types
      verifyObjectTypeMapping(
        "OBJECT_TEST", new String[] {
          "VARCHAR(100)",
          "NUMBER",
          "RAW(100)",
          "DATE",
          "TIMESTAMP(9)",
          "TIMESTAMP(9) WITH TIME ZONE",
          "TIMESTAMP(9) WITH LOCAL TIME ZONE",
          "INTERVAL YEAR TO MONTH",
          "INTERVAL DAY TO SECOND",
          "BLOB",
          "CLOB"
        },
        i -> new Object[]{
          // Expect VARCHAR and String to map
          "你好-" + i,
          // Expect NUMBER and BigDecimal to map
          BigDecimal.valueOf(i),
          // Expect RAW and ByteBuffer to map
          ByteBuffer.allocate(5 * Integer.BYTES)
            .putInt(i)
            .putInt(i + 1)
            .putInt(i + 2)
            .putInt(i + 3)
            .putInt(i + 4)
            .flip(),
          // Expect DATE and LocalDateTime to map
          LocalDateTime.of(2038 + i, 10, 23, 9, 42, 1),
          // Expect TIMESTAMP and LocalDateTime to map
          LocalDateTime.of(2038, 10, 23, 9, 42, 1, 1 + i),
          // Expect TIMESTAMP WITH TIME ZONE and OffsetDateTime to map
          OffsetDateTime.of(
            2038, 10, 23, 9, 42, 1, 1 + i, ZoneOffset.ofHours(-5)),
          // Expect TIMESTAMP WITH LOCAL TIME ZONE and LocalDateTime to map
          LocalDateTime.of(2038, 10, 23, 9, 42, 1, 1 + i),
          // Expect INTERVAL YEAR TO MONTH and Period to map
          Period.of(1 + i, 2, 0),
          // Expect INTERVAL DAY TO SECOND and Duration to map
          Duration.ofDays(1 + i)
            .plus(Duration.ofHours(2))
            .plus(Duration.ofMinutes(3))
            .plus(Duration.ofSeconds(4)),
          // Expect BLOB and ByteBuffer to map.
          IntStream.range(0, 16_000)
            .map(j -> j + i)
            .collect(
              () -> ByteBuffer.allocate(16_000 * Integer.BYTES),
              ByteBuffer::putInt,
              ByteBuffer::put)
            .flip(),
          // Expect CLOB and String to map
          IntStream.range(0, 64_000)
            .map(j -> 'a' + ((j + i) % 26))
            .collect(
              () -> CharBuffer.allocate(64_000),
              (charBuffer, intChar) -> charBuffer.put((char)intChar),
              CharBuffer::put)
            .flip()
            .toString()
        },
        true,
        connection);

      // Verify all SQL types with null values
      verifyObjectTypeMapping(
        "OBJECT_TEST_NULL", new String[] {
          "VARCHAR(100)",
          "NUMBER",
          "RAW(100)",
          "DATE",
          "TIMESTAMP(9)",
          "TIMESTAMP(9) WITH TIME ZONE",
          "TIMESTAMP(9) WITH LOCAL TIME ZONE",
          "INTERVAL YEAR TO MONTH",
          "INTERVAL DAY TO SECOND",
          "BLOB",
          "CLOB"
        },
        i -> new Object[]{
          // Expect VARCHAR and null to map
          null,
          // Expect NUMBER and null to map
          null,
          // Expect RAW and null to map
          null,
          // Expect DATE and null to map
          null,
          // Expect TIMESTAMP and null to map
          null,
          // Expect TIMESTAMP WITH TIME ZONE and null to map
          null,
          // Expect TIMESTAMP WITH LOCAL TIME ZONE and null to map
          null,
          // Expect INTERVAL YEAR TO MONTH and null to map
          null,
          // Expect INTERVAL DAY TO SECOND and null to map
          null,
          // Expect BLOB and io.r2dbc.spi.Blob to map
          null,
          // Expect CLOB and io.r2dbc.spi.Clob to map
          null
        },
        true,
        connection);

      // Verify non-default mappings of all SQL types
      verifyObjectTypeMapping(
        "OBJECT_TEST_NON_DEFAULT", new String[] {
          // "NUMBER", See comment about Boolean to NUMBER mapping below
          "NUMBER",
          "NUMBER",
          "NUMBER",
          "NUMBER",
          "NUMBER",
          "NUMBER",
          "DATE",
          "DATE",
          "TIMESTAMP(9)",
          "TIMESTAMP(9)",
          "TIMESTAMP(9) WITH TIME ZONE",
        },
        i -> new Object[]{

          // Boolean to NUMBER mapping no longer works with the 23.3 JDBC
          // driver, which added support for the BOOLEAN data type. It now
          // converts a Java boolean to a SQL BOOLEAN. But, PL/SQL does not
          // support conversion of BOOLEAN to NUMBER. This would cause:
          //   PLS-00306: wrong number or types of arguments in call to 'OBJECT_TEST_NON_DEFAULT'
          // Expect NUMBER and Boolean to map
          // true,

          // Expect NUMBER and Integer to map
          i,
          // Expect NUMBER and Byte to map
          (byte)i,
          // Expect NUMBER and Short to map
          (short)i,
          // Expect NUMBER and Long to map
          (long)i,
          // Expect NUMBER and Float to map
          (float)i,
          // Expect NUMBER and Double to map
          (double) i,
          // Expect DATE and LocalDate to map
          LocalDate.of(2038 + i, 10, 23),
          // Expect DATE and LocalTime to map
          LocalTime.of(9 + i, 42, 1),
          // Expect TIMESTAMP and LocalDate to map
          LocalDate.of(2038 + i, 10, 23),
          // Expect TIMESTAMP and LocalTime to map
          LocalTime.of(9 + i, 42, 1),
          // Expect TIMESTAMP WITH TIME ZONE and OffsetTime to map
          OffsetTime.of(9 + i, 42, 1, 1, ZoneOffset.ofHours(-5))
        },
        false,
        connection);
    }
    finally {
      tryAwaitNone(connection.close());
    }

  }

  /**
   * <p>
   * Verifies the implementation of Java to SQL and SQL to Java type mappings
   * for the VECTOR data type. The R2DBC 1.0.0 Specification does not contain
   * mapping guidelines for the VECTOR data type. The Oracle R2DBC Driver is
   * expected to map VECTOR to a {@link oracle.sql.VECTOR} value.
   *</p>
   */
  @Test
  public void testVectorMapping() throws SQLException {

    // The VECTOR data type was introduced in Oracle Database version 23ai, so
    // this test is skipped if the version is older than 23.
    assumeTrue(databaseVersion() >= 23,
      "JSON columns are not supported by database versions older than 21");

    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      double[] doubleArray =
        DoubleStream.iterate(-2.0d, previous -> previous + 0.1d)
          .limit(40)
          .toArray();

      // Expect VECTOR and oracle.sql.VECTOR to map.
      VECTOR vector = VECTOR.ofFloat64Values(doubleArray);
      verifyTypeMapping(connection, vector, "VECTOR");

      // Expect VECTOR and double[] to map
      verifyTypeMapping(
        connection,
        Parameters.in(OracleR2dbcTypes.VECTOR, doubleArray),
        "VECTOR",
        row ->
          row.get(0, double[].class),
        (ignored, actualValue) ->
          assertArrayEquals(doubleArray, actualValue));

      float[] floatArray = new float[doubleArray.length];
      for (int i = 0; i < floatArray.length; i++)
        floatArray[i] = (float) doubleArray[i];

      // Expect VECTOR and float[] to map
      verifyTypeMapping(
        connection,
        Parameters.in(OracleR2dbcTypes.VECTOR, floatArray),
        "VECTOR",
        row ->
          row.get(0, float[].class),
        (ignored, actualValue) ->
          assertArrayEquals(floatArray, actualValue));

      byte[] byteArray = new byte[doubleArray.length];
      for (int i = 0; i < byteArray.length; i++)
        byteArray[i] = (byte) doubleArray[i];

      // Expect VECTOR and byte[] to map
      verifyTypeMapping(
        connection,
        Parameters.in(OracleR2dbcTypes.VECTOR, byteArray),
        "VECTOR",
        row ->
          row.get(0, byte[].class),
        (ignored, actualValue) ->
          assertArrayEquals(byteArray, actualValue));

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * For an ARRAY type of a given {@code typeName}, verifies the following
   * cases:
   * <ul>
   *   <li>1 dimensional array mapping to an array of a {@code javaType}</li>
   *   <li>Mapping when array is empty.</li>
   *   <li>
   *     All cases listed above when the ARRAY type is the type of a a two
   *     dimensional and three dimensional array.
   *     </li>
   * </ul>
   */
  private static <T> void verifyArrayTypeMapping(
    Connection connection,
    ArrayType arrayType1D,
    ArrayType arrayType2D,
    ArrayType arrayType3D,
    Class<T> javaArrayClass, IntFunction<T> arrayGenerator,
    boolean isDefaultMapping,
    BiConsumer<T, T> equalsAssertion) {

    // Verify mapping of 1-dimensional array with values
    T javaArray1D = arrayGenerator.apply(0);
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType1D, javaArray1D),
      arrayType1D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, javaArray1D.getClass()),
      (ignored, rowValue) ->
        equalsAssertion.accept(
          javaArray1D,
          assertInstanceOf(javaArrayClass, rowValue)));

    // Verify mapping of an empty 1-dimensional array
    @SuppressWarnings("unchecked")
    T emptyJavaArray = (T)java.lang.reflect.Array.newInstance(
      javaArrayClass.getComponentType(), 0);
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType1D, emptyJavaArray),
      arrayType1D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, emptyJavaArray.getClass()),
      (ignored, rowValue) ->
        assertEquals(
          0,
          java.lang.reflect.Array.getLength(
            assertInstanceOf(javaArrayClass, rowValue))));

    // Create a 2D Java array
    @SuppressWarnings("unchecked")
    T[] javaArray2D =
      (T[]) java.lang.reflect.Array.newInstance(javaArrayClass, 3);
    for (int i = 0; i < javaArray2D.length; i++) {
      javaArray2D[i] = arrayGenerator.apply(i);
    }

    // Verify mapping of 2-dimensional array with values
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType2D, javaArray2D),
      arrayType2D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, javaArray2D.getClass()),
      (ignored, rowValue) ->
        assertArrayEquals(
          javaArray2D,
          assertInstanceOf(javaArray2D.getClass(), rowValue)));

    // Verify mapping of an empty 2-dimensional array
    @SuppressWarnings("unchecked")
    T[] emptyJavaArray2D = (T[]) java.lang.reflect.Array.newInstance(
      javaArrayClass.getComponentType(), 0, 0);
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType2D, emptyJavaArray2D),
      arrayType2D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, emptyJavaArray2D.getClass()),
      (ignored, rowValue) ->
        assertArrayEquals(
          emptyJavaArray2D,
          assertInstanceOf(emptyJavaArray2D.getClass(), rowValue)));

    // Verify of a 2-dimensional array with empty 1-dimensional arrays
    @SuppressWarnings("unchecked")
    T[] empty1DJavaArray2D = (T[]) java.lang.reflect.Array.newInstance(
      javaArrayClass.getComponentType(), 3, 0);
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType2D, empty1DJavaArray2D),
      arrayType2D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, empty1DJavaArray2D.getClass()),
      (ignored, rowValue) ->
        assertArrayEquals(
          empty1DJavaArray2D,
          assertInstanceOf(empty1DJavaArray2D.getClass(), rowValue)));

    // Create a 3D Java array
    @SuppressWarnings("unchecked")
    T[][] javaArray3D =
      (T[][])java.lang.reflect.Array.newInstance(javaArrayClass, 3, 3);
    for (int i = 0; i < javaArray3D.length; i++) {
      for (int j = 0; j < javaArray3D[i].length; j++) {
        javaArray3D[i][j] =
          arrayGenerator.apply((i * javaArray3D[i].length) + j);
      }
    }

    // Verify mapping of 3-dimensional array with values
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType3D, javaArray3D),
      arrayType3D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, javaArray3D.getClass()),
      (ignored, rowValue) ->
        assertArrayEquals(
          javaArray3D,
          assertInstanceOf(javaArray3D.getClass(), rowValue)));

    // Verify mapping of an empty 2-dimensional array
    @SuppressWarnings("unchecked")
    T[][] emptyJavaArray3D = (T[][])java.lang.reflect.Array.newInstance(
      javaArrayClass.getComponentType(), 0, 0, 0);
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType3D, emptyJavaArray3D),
      arrayType3D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, emptyJavaArray3D.getClass()),
      (ignored, rowValue) ->
        assertArrayEquals(
          emptyJavaArray3D,
          assertInstanceOf(emptyJavaArray3D.getClass(), rowValue)));

    // Verify of a 3-dimensional array with empty 2-dimensional arrays
    @SuppressWarnings("unchecked")
    T[][] empty2DJavaArray3D = (T[][])java.lang.reflect.Array.newInstance(
      javaArrayClass.getComponentType(), 3, 0, 0);
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType3D, empty2DJavaArray3D),
      arrayType3D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, empty2DJavaArray3D.getClass()),
      (ignored, rowValue) ->
        assertArrayEquals(
          empty2DJavaArray3D,
          assertInstanceOf(empty2DJavaArray3D.getClass(), rowValue)));

    // Verify of a 3-dimensional array with empty 1-dimensional arrays
    @SuppressWarnings("unchecked")
    T[][] empty1DJavaArray3D = (T[][])java.lang.reflect.Array.newInstance(
      javaArrayClass.getComponentType(), 3, 3, 0);
    verifyTypeMapping(
      connection,
      Parameters.in(arrayType3D, empty1DJavaArray3D),
      arrayType3D.getName(),
      isDefaultMapping
        ? row -> row.get(0)
        : row -> row.get(0, empty1DJavaArray3D.getClass()),
      (ignored, rowValue) ->
        assertArrayEquals(
          empty1DJavaArray3D,
          assertInstanceOf(empty1DJavaArray3D.getClass(), rowValue)));
  }


  private static void verifyObjectTypeMapping(
    String typeName, String[] attributeTypes,
    IntFunction<Object[]> valueArrayGenerator, boolean isDefaultMapping,
    Connection connection) {

    ObjectType objectType1D = OracleR2dbcTypes.objectType(typeName);
    ObjectType objectType2D = OracleR2dbcTypes.objectType(typeName + "_2D");
    ObjectType objectType3D = OracleR2dbcTypes.objectType(typeName + "_3D");
    try {
      String[] attributeNames1D =
        IntStream.range(0, attributeTypes.length)
          .mapToObj(i -> format("value%d", i))
          .toArray(String[]::new);
      createObjectType(
        objectType1D.getName(), attributeNames1D, attributeTypes, connection);

      String[] attributeNames2D =
        IntStream.range(0, 3)
          .mapToObj(i -> format("value2D%d", i))
          .toArray(String[]::new);
       createObjectType(
        objectType2D.getName(),
        attributeNames2D,
        new String[] {typeName, typeName, typeName},
        connection);

      String[] attributeNames3D =
        IntStream.range(0, 3)
          .mapToObj(i -> format("value3D%d", i))
          .toArray(String[]::new);
      createObjectType(
        objectType3D.getName(),
        attributeNames3D,
        new String[] {
          objectType2D.getName(),
          objectType2D.getName(),
          objectType2D.getName()
        },
        connection);

      verifyObjectTypeMapping(
        objectType1D, attributeNames1D, valueArrayGenerator.apply(0),
        isDefaultMapping, connection);

      verifyObjectTypeMapping(
        objectType2D, attributeNames2D, new Object[] {
          Parameters.in(objectType1D, valueArrayGenerator.apply(1)),
          Parameters.in(objectType1D,
            toMap(attributeNames1D, valueArrayGenerator.apply(2))),
          TestUtils.constructObject(
            connection, objectType1D, valueArrayGenerator.apply(3))
        },
        false, connection);

      verifyObjectTypeMapping(
        objectType3D, attributeNames3D, new Object[] {
          Parameters.in(
            objectType2D,
            new Object[] {
              Parameters.in(objectType1D, valueArrayGenerator.apply(4)),
              Parameters.in(objectType1D,
                toMap(attributeNames1D, valueArrayGenerator.apply(5))),
              TestUtils.constructObject(
                connection, objectType1D, valueArrayGenerator.apply(6))
            }),
          Parameters.in(
            objectType2D,
            toMap(attributeNames2D, new Object[] {
              Parameters.in(objectType1D, valueArrayGenerator.apply(7)),
              Parameters.in(objectType1D,
                toMap(attributeNames1D, valueArrayGenerator.apply(8))),
              TestUtils.constructObject(
                connection, objectType1D, valueArrayGenerator.apply(9))
             })),
          TestUtils.constructObject(
            connection, objectType2D,
            Parameters.in(objectType1D, valueArrayGenerator.apply(10)),
            Parameters.in(objectType1D,
              toMap(attributeNames1D, valueArrayGenerator.apply(11))),
            TestUtils.constructObject(
              connection, objectType1D, valueArrayGenerator.apply(12)))
        },
        false, connection);
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE " + objectType3D.getName()));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE " + objectType2D.getName()));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE " + objectType1D.getName()));
    }

  }

  static void verifyObjectTypeMapping(
    ObjectType objectType, String[] attributeNames, Object[] attributeValues,
    boolean isDefaultMapping, Connection connection) {

    // Bind the attributes as an Object[]
    verifyTypeMapping(
      connection,
      Parameters.in(objectType, attributeValues),
      objectType.getName(),
      row -> row.get(0, OracleR2dbcObject.class),
      (ignored, object) ->
        assertObjectEquals(object, attributeValues, isDefaultMapping));

    // Bind the attributes as a Map
    verifyTypeMapping(
      connection,
      Parameters.in(objectType, toMap(attributeNames, attributeValues)),
      objectType.getName(),
      row -> row.get(0, OracleR2dbcObject.class),
      (ignored, object) ->
        assertObjectEquals(object, attributeValues, isDefaultMapping));

    // Bind the attributes as an OracleR2dbcObject
    OracleR2dbcObject objectValue =
      TestUtils.constructObject(connection, objectType, attributeValues);
    verifyTypeMapping(
      connection,
      objectValue,
      objectType.getName(),
      row -> row.get(0, OracleR2dbcObject.class),
      (ignored, object) ->
        assertObjectEquals(object, attributeValues, isDefaultMapping));
  }

  static ObjectType createObjectType(
    String typeName, String[] attributeNames, String[] attributeTypes,
    Connection connection) {

    awaitExecution(connection.createStatement(format(
      "CREATE OR REPLACE TYPE %s AS OBJECT(%s)",
      typeName,
      IntStream.range(0, attributeNames.length)
        .mapToObj(i -> attributeNames[i] + " " + attributeTypes[i])
        .collect(Collectors.joining(",")))));

    return OracleR2dbcTypes.objectType(typeName);
  }

  private static Map<String, Object> toMap(String[] names, Object[] values) {
    Map<String, Object> map = new HashMap<>(values.length);
    for (int i = 0; i < names.length; i++)
      map.put(names[i], values[i]);
    return map;
  }

  /**
   * Asserts that the attributes of an {@code object} are the same as a set
   * of {@code values} provided to bind the object in an INSERT.
   */
  private static void assertObjectEquals(
    OracleR2dbcObject object, Object[] values, boolean isDefaultMapping) {

    for (int i = 0; i < values.length; i++) {
      final Object expected;
      final Object actual;

      if (values[i] instanceof Parameter) {
        expected = ((Parameter)values[i]).getValue();
      }
      else {
        expected = values[i];
      }


      if (isDefaultMapping || expected == null) {
        actual = object.get(i);
      }
      else if (values[i] instanceof Parameter
        && ((Parameter)values[i]).getType() instanceof ObjectType) {

        // Recursively compare Object[] and Map binds with OBJECT attributes
        OracleR2dbcObject objectAttribute =
          object.get(i, OracleR2dbcObject.class);

        if (expected instanceof Object[]) {
          assertObjectEquals(objectAttribute, (Object[]) expected, false);
          continue;
        }
        else if (expected instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> expectedMap = (Map<String, Object>) expected;
          TreeMap<String, Object> treeMap =
            new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
          treeMap.putAll(expectedMap);
          assertObjectEquals(
            objectAttribute,
            objectAttribute.getMetadata()
              .getAttributeMetadatas()
              .stream()
              .map(metadata -> treeMap.get(metadata.getName()))
              .toArray(),
            false);
          continue;
        }
        else {
          actual = objectAttribute;
        }
      }
      else {
        final Class<?> expectedClass;

        // Request a supported super class type, rather than a specific
        // subclass of the expected value: ByteBuffer rather than
        // HeapByteBuffer, for instance.
        if (expected instanceof ByteBuffer)
          expectedClass = ByteBuffer.class;
        else if (expected instanceof OracleR2dbcObject)
          expectedClass = OracleR2dbcObject.class;
        else
          expectedClass = expected.getClass();

        actual = object.get(i, expectedClass);
      }

      String message = "Mismatch at attribute index " + i;

      if (expected instanceof OracleR2dbcObject
        && actual instanceof OracleR2dbcObject) {
        // Need to compare default mappings as OracleR2dbcObject does not
        // implement equals.
        assertArrayEquals(
          toArray((OracleR2dbcObject) expected),
          toArray((OracleR2dbcObject) actual),
          message);
      }
      else if (expected instanceof Object[] && actual instanceof Object[])
        assertArrayEquals((Object[]) expected, (Object[]) actual, message);
      else
        assertEquals(expected, actual, message);
    }
  }

  /** Converts an OBJECT to an Object[] of each attribute's default Java type */
  private static Object[] toArray(OracleR2dbcObject object) {
    Object[] array = new Object[
      object.getMetadata().getAttributeMetadatas().size()];

    for (int i = 0; i < array.length; i++) {
      array[i] = object.get(i);

      if (array[i] instanceof OracleR2dbcObject)
        array[i] = toArray((OracleR2dbcObject) array[i]);
    }

    return array;
  }

  // TODO: More tests for JDBC 4.3 mappings like BigInteger to BIGINT,
  //  java.sql.Date to DATE, java.sql.Blob to BLOB? Oracle R2DBC exposes all
  //  type mappings supported by Oracle JDBC.

  /**
   * <p>
   * Verifies the conversion between a Java Language type and a SQL Language
   * type. The Java Language {@code javaValue} is converted to a SQL Language
   * value of a type specified by {@code sqlTypeDdl}. The SQL Language value is
   * then converted back to a Java Language type and checked for equality
   * with the original {@code javaValue}.
   * </p>
   * @param connection Connection to a database
   * @param javaValue Value to convert. Maybe null.
   * @param sqlTypeDdl SQL Language DDL for a column type, such as
   *                   "NUMBER" or "VARCHAR(100)". Not null.
   */
  private static void verifyTypeMapping(
    Connection connection, Object javaValue, String sqlTypeDdl) {
    verifyTypeMapping(connection, javaValue, sqlTypeDdl,
      Assertions::assertEquals);
  }

  /**
   * <p>
   * Verifies the conversion between a Java Language type and a SQL Language
   * type. The Java Language {@code javaValue} is converted to a SQL Language
   * value of a type specified by {@code sqlTypeDdl}. The SQL Language value is
   * then converted back to a Java Language type and checked for equality
   * with the original {@code javaValue}.
   * </p><p>
   * In case the expected type mapping for {@link Row#get(int)} is different
   * from the {@code javaValue's} type, the {@code verifyEquals} function is
   * specified to convert the {@code Row's} expected mapping into the same type
   * as {@code javaValue}. This function is used when a bind type mapping
   * isn't supported as a row type mapping.
   * </p><p>
   * This method will also INSERT a Java {@code null} bind value, and expect
   * to get a Java {@code null} value from a SELECT of the SQL NULL value.
   * </p>
   * @param connection Connection to a database
   * @param javaValue Value to insert. Maybe null.
   * @param sqlTypeDdl SQL Language DDL for a column type, such as
   *                   "NUMBER" or "VARCHAR(100)". Not null.
   * @param verifyEquals Verifies the expected Java type mapping for
   * {@link Row#get(int)} has a value that is equal to {@code javaValue}.
   */
  private static <T> void verifyTypeMapping(
    Connection connection, T javaValue, String sqlTypeDdl,
    BiConsumer<T, Object> verifyEquals) {
    verifyTypeMapping(connection, javaValue, sqlTypeDdl,
      row -> row.get("javaValue"), verifyEquals);
  }
  
  /**
   * <p>
   * Verifies the conversion between a Java Language type and a SQL Language
   * type. The Java Language {@code javaValue} is converted to a SQL Language
   * value of a type specified by {@code sqlTypeDdl}, and inserted into a table.
   * </p><p>
   * The inserted value is then queried back and the resulting {@code Row} is
   * input to each function the {@code rowVerifiers} array. These functions
   * should verify that the row returns an expected value.
   * </p><p>
   * This method will also INSERT a Java {@code null} bind value, and expect
   * to get a Java {@code null} value from a SELECT of the SQL NULL value.
   * </p>
   * @param connection Connection to a database
   * @param javaValue Value to insert. Maybe null.
   * @param sqlTypeDdl SQL Language DDL for a column type, such as
   *                   "NUMBER" or "VARCHAR(100)". Not null.
   * @param rowMapper Outputs a Java value for an input Row
   * @param verifyEquals Verifies the {@code rowMapper} output is equal to
   * {@code javaValue}.
   */
  private static <T, U> void verifyTypeMapping(
    Connection connection, T javaValue, String sqlTypeDdl,
    Function<Row, U> rowMapper, BiConsumer<T, U> verifyEquals) {
    String table = "verify_" + sqlTypeDdl.replaceAll("[^\\p{Alnum}]", "_");
    try {
      awaitExecution(connection.createStatement(format(
        "CREATE TABLE "+table+" (javaValue %s)", sqlTypeDdl)));

      Statement insert = connection.createStatement(
          "INSERT INTO "+table+"(javaValue) VALUES(:javaValue)")
        .bind("javaValue", javaValue)
        .add();

      if (javaValue instanceof Parameter) {
        Type type = ((Parameter) javaValue).getType();
        insert.bind("javaValue", Parameters.in(type));
      }
      else if (javaValue instanceof OracleR2dbcObject) {
        Type type =
          ((OracleR2dbcObject)javaValue).getMetadata().getObjectType();
        insert.bind("javaValue", Parameters.in(type));
      }
      else {
        insert.bindNull("javaValue", javaValue.getClass());
      }

      awaitUpdate(asList(1,1), insert);

      verifyEquals.accept(javaValue,
        awaitOne(Flux.from(connection.createStatement(
          "SELECT javaValue FROM "+table+" WHERE javaValue IS NOT NULL")
          .execute())
          .flatMap(result ->
            result.map((row, metadata) -> rowMapper.apply(row))
          )));

      awaitOne(true,
        Flux.from(connection.createStatement(
          "SELECT javaValue FROM "+table+" WHERE javaValue IS NULL")
          .execute())
          .flatMap(result ->
            result.map((row, metadata) -> null == row.get("javaValue"))
          ));
    }
    finally {
      try {
        awaitExecution(connection.createStatement("DROP TABLE "+table));
      }
      catch (RuntimeException error) {
        error.printStackTrace();
      }
    }
  }

  /**
   * Returns a {@code String} that stores the character data emitted by a
   * {@code clobValue's} {@link Clob#stream()} publisher.
   * @param clobValue A {@link Clob}
   * @return The {@code clobValue's} content.
   * @throws ClassCastException If {@code clobValue} is not a {@link Clob}
   */
  private static String clobToString(Object clobValue) {
    return Flux.from(((Clob)clobValue).stream())
      .map(CharSequence::toString)
      .reduce(CharBuffer.allocate(1024), (previous, current) ->
        previous.remaining() >= current.length()
          ? previous.put(current)
          : CharBuffer.allocate((int) (
          1.5d * (previous.capacity() + current.length())))
          .put(previous.flip())
          .put(current))
      .block(sqlTimeout())
      .flip()
      .toString();
  }

  /**
   * Returns a {@code ByteBuffer} that stores the binary data emitted by a
   * {@code blobValue's} {@link Blob#stream()} publisher.
   * @param blobValue A {@link Blob}
   * @return The {@code blobValue's} content.
   * @throws ClassCastException If {@code blobValue} is not a {@link Blob}
   */
  private static ByteBuffer blobToByteBuffer(Object blobValue) {
    return Flux.from(((Blob)blobValue).stream())
      .reduce(ByteBuffer.allocate(1024), (previous, current) ->
        previous.remaining() >= current.remaining()
          ? previous.put(current)
          : ByteBuffer.allocate((int) (
              1.5d * (previous.capacity() + current.capacity())))
              .put(previous.flip())
              .put(current))
      .block(sqlTimeout())
      .flip();
  }

}
