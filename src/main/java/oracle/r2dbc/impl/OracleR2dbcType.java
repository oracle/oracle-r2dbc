package oracle.r2dbc.impl;

import io.r2dbc.spi.Type;

import java.nio.ByteBuffer;
import java.sql.RowId;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;

enum OracleR2dbcType implements Type {

  BINARY_DOUBLE(Double.class, "BINARY_DOUBLE"),

  BINARY_FLOAT(Float.class, "BINARY_FLOAT"),

  /**
   * A Binary Large Object (BLOB) as implemented by Oracle Database. The default
   * Java type mapping is {@link io.r2dbc.spi.Blob} rather than
   * {@link java.nio.ByteBuffer}, which is the mapping of the standard
   * {@link io.r2dbc.spi.R2dbcType#BLOB}.
   */
  BLOB(io.r2dbc.spi.Blob.class, "BLOB"),

  /**
   * A Character Large Object (BLOB) as implemented by Oracle Database. The
   * default Java type mapping is {@link io.r2dbc.spi.Clob} rather than
   * {@link String}, which is the mapping of the standard
   * {@link io.r2dbc.spi.R2dbcType#CLOB}.
   */
  CLOB(io.r2dbc.spi.Clob.class, "CLOB"),

  INTERVAL_DAY_TO_SECOND(Duration.class, "INTERVAL DAY TO SECOND"),

  INTERVAL_YEAR_TO_MONTH(Period.class, "INTERVAL YEAR TO MONTH"),

  LONG(String.class, "LONG"),

  LONG_RAW(ByteBuffer.class, "LONG RAW"),

  /**
   * A National Character Large Object (NCLOB) as implemented by Oracle
   * Database. The default Java type mapping is {@link io.r2dbc.spi.Clob}
   * rather than {@link String}, which is the mapping of the standard
   * {@link io.r2dbc.spi.R2dbcType#NCLOB}.
   */
  NCLOB(io.r2dbc.spi.Clob.class, "NCLOB"),

  ROW_ID(RowId.class, "ROWID"),

  TIMESTAMP_WITH_LOCAL_TIME_ZONE(
    LocalDateTime.class, "TIMESTAMP WITH LOCAL TIME ZONE"),

  ;

  private final Class<?> javaType;
  private final String sqlName;

  OracleR2dbcType(Class<?> javaType, String sqlName) {
    this.javaType = javaType;
    this.sqlName = sqlName;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the default Java type
   * mapping for values of this SQL type. The Java type returned by this
   * method is the type of {@code Object} returned by {@code Row.get
   * (String/int)} when accessing a value of this SQL type.
   * </p>
   */
  @Override
  public Class<?> getJavaType() {
    return javaType;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Implements the R2DBC SPI method by returning the name of this SQL type.
   * The name returned by this method is recognized in expressions of a SQL
   * command, for instance: A column definition of a {@code CREATE TABLE}
   * command.
   * </p>
   * @return
   */
  @Override
  public String getName() {
    return sqlName;
  }
}
