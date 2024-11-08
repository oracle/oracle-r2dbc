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

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.ReadableMetadata;
import io.r2dbc.spi.Type;
import oracle.jdbc.OracleType;
import oracle.r2dbc.OracleR2dbcObject;
import oracle.r2dbc.OracleR2dbcObjectMetadata;
import oracle.r2dbc.OracleR2dbcTypes;
import oracle.sql.VECTOR;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static oracle.r2dbc.test.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.test.DatabaseConfig.databaseVersion;
import static oracle.r2dbc.test.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.test.DatabaseConfig.user;
import static oracle.r2dbc.test.TestUtils.constructObject;
import static oracle.r2dbc.util.Awaits.awaitExecution;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static oracle.r2dbc.util.Awaits.awaitQuery;
import static oracle.r2dbc.util.Awaits.awaitUpdate;
import static oracle.r2dbc.util.Awaits.tryAwaitExecution;
import static oracle.r2dbc.util.Awaits.tryAwaitNone;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that
 * {@link OracleReadableMetadataImpl} implements behavior that is specified in
 * it's class and method level javadocs.
 */
public class OracleReadableMetadataImplTest {
  
  /**
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
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

      // Expect CLOB and String to map.
      verifyColumnMetadata(
        connection, "CLOB", JDBCType.CLOB, R2dbcType.CLOB,
        null, null, String.class, "test");

      // Expect NCLOB and String to map
      verifyColumnMetadata(
        connection, "NCLOB", JDBCType.NCLOB, R2dbcType.NCLOB,
        null, null, String.class, "test");

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
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
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

      // Expect BLOB and ByteBuffer to map.
      verifyColumnMetadata(
        connection, "BLOB", JDBCType.BLOB, R2dbcType.BLOB, null, null,
        ByteBuffer.class, ByteBuffer.wrap(new byte[3]));

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
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

      // Expect getScale() to return 0 for NUMBER .
      verifyColumnMetadata(
        connection, "NUMBER(5,0)", JDBCType.NUMERIC, R2dbcType.NUMERIC, 5, 0,
        BigDecimal.class, BigDecimal.valueOf(12345));

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
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
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
      // digits in the fractional seconds component. Expect the
      // OracleType.TIMESTAMP_WITH_TIME_ZONE which has a different type code
      // than JDBCType.TIMESTAMP_WITH_TIMEZONE
      verifyColumnMetadata(
        connection, "TIMESTAMP(3) WITH TIME ZONE",
        OracleType.TIMESTAMP_WITH_TIME_ZONE, R2dbcType.TIMESTAMP_WITH_TIME_ZONE,
        35, 3, OffsetDateTime.class,
        OffsetDateTime.parse("1977-06-16T09:00:00.123+01:23"));

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

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
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

    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
   * JSON type columns. When the test database is older than version 21c, this
   * test is ignored; The JSON type was added in 21c.
   */
  @Test
  public void testJsonType() {
    // The JSON data type was introduced in Oracle Database version 21c, so this
    // test is a no-op if the version is older than 21c.
    Assumptions.assumeTrue(databaseVersion() >= 21);

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
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
   * ARRAY type columns.
   */
  @Test
  public void testArrayTypes() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      awaitExecution(connection.createStatement(
        "CREATE TYPE TEST_ARRAY_TYPE AS ARRAY(3) OF NUMBER"));
      OracleR2dbcTypes.ArrayType arrayType =
        OracleR2dbcTypes.arrayType(user().toUpperCase() + ".TEST_ARRAY_TYPE");

      verifyColumnMetadata(
        connection, arrayType.getName(), JDBCType.ARRAY, arrayType,
        null, null, Object[].class,
        Parameters.in(arrayType, new Integer[]{0, 1, 2}));
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_ARRAY_TYPE"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
   * OBJECT type columns.
   */
  @Test
  public void testObjectTypes() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // A fully qualified type names to match the names returned by RowMetadata
      OracleR2dbcTypes.ObjectType objectType = OracleR2dbcTypes.objectType(
        user().toUpperCase() + ".TEST_OBJECT_TYPE");
      OracleR2dbcTypes.ObjectType objectType2D = OracleR2dbcTypes.objectType(
        user().toUpperCase() + ".TEST_OBJECT_TYPE_2D");
      OracleR2dbcTypes.ArrayType arrayType2D = OracleR2dbcTypes.arrayType(
        user().toUpperCase() + ".TEST_OBJECT_TYPE_ARRAY_TYPE");

      Type[] attributeTypes = new Type[] {
        R2dbcType.CHAR,
        R2dbcType.VARCHAR,
        R2dbcType.NCHAR,
        R2dbcType.NVARCHAR,
        R2dbcType.CLOB,
        R2dbcType.NCLOB,
        R2dbcType.VARBINARY,
        R2dbcType.BLOB,
        R2dbcType.NUMERIC,
        OracleR2dbcTypes.BINARY_FLOAT,
        OracleR2dbcTypes.BINARY_DOUBLE,
        R2dbcType.TIMESTAMP,
        R2dbcType.TIMESTAMP,
        R2dbcType.TIMESTAMP_WITH_TIME_ZONE,
        OracleR2dbcTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        objectType2D,
        arrayType2D,
      };

      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE TYPE TEST_OBJECT_TYPE_2D" +
          " AS OBJECT(value0_2D NUMBER(2, 2))"));
      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE TYPE TEST_OBJECT_TYPE_ARRAY_TYPE" +
          " AS ARRAY(1) OF VARCHAR(10)"));

      String[] attributeNames = IntStream.range(0, attributeTypes.length)
        .mapToObj(i -> "value" + i)
        .toArray(String[]::new);

      awaitExecution(connection.createStatement(
        "CREATE OR REPLACE TYPE TEST_OBJECT_TYPE AS OBJECT(" +
          "value0 CHAR(10)," +
          "value1 VARCHAR(10)," +
          "value2 NCHAR(10)," +
          "value3 NVARCHAR2(10)," +
          "value4 CLOB," +
          "value5 NCLOB," +
          "value6 RAW(10)," +
          "value7 BLOB," +
          "value8 NUMBER(9)," +
          "value9 BINARY_FLOAT," +
          "value10 BINARY_DOUBLE," +
          "value11 DATE," +
          "value12 TIMESTAMP(9)," +
          "value13 TIMESTAMP(8) WITH TIME ZONE," +
          "value14 TIMESTAMP(7) WITH LOCAL TIME ZONE," +
          "value15 TEST_OBJECT_TYPE_2D," +
          "value16 TEST_OBJECT_TYPE_ARRAY_TYPE)"));

      OracleR2dbcObject object = constructObject(
        connection, objectType,
        Arrays.stream(attributeTypes)
          .map(Parameters::in)
          .toArray());

      verifyColumnMetadata(
        connection, "TEST_OBJECT_TYPE", JDBCType.STRUCT, objectType,
        null, null, OracleR2dbcObject.class, object);

      // Verify the attribute metadata too.
      OracleR2dbcObjectMetadata objectMetadata = object.getMetadata();
      assertEquals(objectType, objectMetadata.getObjectType());
      // Oracle JDBC does not support returning the precision of character type
      // attributes. Expect a null precision.
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[0]),
        attributeNames[0], JDBCType.CHAR, attributeTypes[0],
        null, null, Nullability.NULLABLE, String.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[1]),
        attributeNames[1], JDBCType.VARCHAR, attributeTypes[1],
        null, null, Nullability.NULLABLE, String.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[2]),
        attributeNames[2], JDBCType.NCHAR, attributeTypes[2],
        null, null, Nullability.NULLABLE, String.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[3]),
        attributeNames[3], JDBCType.NVARCHAR, attributeTypes[3],
        null, null, Nullability.NULLABLE, String.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[4]),
        attributeNames[4], JDBCType.CLOB, attributeTypes[4],
        null, null, Nullability.NULLABLE, String.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[5]),
        attributeNames[5], JDBCType.NCLOB, attributeTypes[5],
        null, null, Nullability.NULLABLE, String.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[6]),
        attributeNames[6], JDBCType.VARBINARY, attributeTypes[6],
        10, null, Nullability.NULLABLE, ByteBuffer.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[7]),
        attributeNames[7], JDBCType.BLOB, attributeTypes[7],
        null, null, Nullability.NULLABLE, ByteBuffer.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[8]),
        attributeNames[8], JDBCType.NUMERIC, attributeTypes[8],
        9, 0, Nullability.NULLABLE, BigDecimal.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[9]),
        attributeNames[9], OracleType.BINARY_FLOAT, attributeTypes[9],
        null, null, Nullability.NULLABLE, Float.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[10]),
        attributeNames[10], OracleType.BINARY_DOUBLE, attributeTypes[10],
        null, null, Nullability.NULLABLE, Double.class);
      // Oracle JDBC does not support returning the scale for data time types.
      // Expect null.
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[11]),
        attributeNames[11], JDBCType.TIMESTAMP, attributeTypes[11],
        29, null, Nullability.NULLABLE, LocalDateTime.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[12]),
        attributeNames[12], JDBCType.TIMESTAMP, attributeTypes[12],
        29, null, Nullability.NULLABLE, LocalDateTime.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[13]),
        attributeNames[13], OracleType.TIMESTAMP_WITH_TIME_ZONE,
        attributeTypes[13],
        35, null, Nullability.NULLABLE, OffsetDateTime.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[14]),
        attributeNames[14], OracleType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        attributeTypes[14],
        29, null, Nullability.NULLABLE, LocalDateTime.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[15]),
        attributeNames[15], JDBCType.STRUCT, attributeTypes[15],
        null, null, Nullability.NULLABLE, OracleR2dbcObject.class);
      verifyMetadata(
        objectMetadata.getAttributeMetadata(attributeNames[16]),
        attributeNames[16], JDBCType.ARRAY, attributeTypes[16],
        null, null, Nullability.NULLABLE, Object[].class);
    }
    finally {
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_OBJECT_TYPE"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_OBJECT_TYPE_2D"));
      tryAwaitExecution(connection.createStatement(
        "DROP TYPE TEST_OBJECT_TYPE_ARRAY_TYPE"));
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
   * VECTOR type columns. When the test database is older than version 23ai,
   * this test is ignored; The VECTOR type was added in 23ai.
   */
  @Test
  public void testVectorType() throws SQLException {
    Assumptions.assumeTrue(databaseVersion() >= 23,
      "VECTOR requires Oracle Database 23ai or newer");

    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      double[] doubleArray =
        DoubleStream.iterate(0d, previous -> previous + 0.1d)
          .limit(30)
          .toArray();
      VECTOR vector = VECTOR.ofFloat64Values(doubleArray);

      // Expect VECTOR and double[] to map.
      verifyColumnMetadata(
        connection, "VECTOR", OracleType.VECTOR, OracleR2dbcTypes.VECTOR,
        null, null,
        VECTOR.class, vector);
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of {@link OracleReadableMetadataImpl} for
   * BOOLEAN type columns. When the test database is older than version 23ai,
   * this test is ignored; The BOOLEAN type was added in 23ai.
   */
  @Test
  public void testBooleanType() throws SQLException {
    Assumptions.assumeTrue(databaseVersion() >= 23,
      "BOOLEAN requires Oracle Database 23ai or newer");

    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      // Expect BOOLEAN and Boolean to map.
      verifyColumnMetadata(
        connection, "BOOLEAN", JDBCType.BOOLEAN, R2dbcType.BOOLEAN,
        null, null,
        Boolean.class, true);
    }
    finally {
      tryAwaitNone(connection.close());
    }
  }

  /**
   * Calls
   * {@link #verifyColumnMetadata(Connection, String, SQLType, Type, Integer, Integer, Nullability, Class, Object)}
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
      format("CREATE TABLE "+tableName+"(%s %s)",
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

      verifyMetadata(
        metadata, columnName, jdbcType, r2dbcType, precision, scale,
        nullability, javaType);
    }
    finally {
      awaitExecution(connection.createStatement("DROP TABLE "+tableName));
    }
  }

  private void verifyMetadata(
    ReadableMetadata metadata, String name, SQLType jdbcType,
    Type r2dbcType, Integer precision, Integer scale, Nullability nullability,
    Class<?> javaType) {
    assertEquals(javaType, metadata.getJavaType());
    // Don't expect Oracle R2DBC to match the column name's case.
    assertEquals(name.toUpperCase(), metadata.getName().toUpperCase());
    assertEquals(jdbcType, metadata.getNativeTypeMetadata());
    assertEquals(r2dbcType, metadata.getType());
    assertEquals(nullability, metadata.getNullability());
    assertEquals(precision, metadata.getPrecision());
    assertEquals(scale, metadata.getScale());
  }

}
