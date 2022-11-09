package oracle.r2dbc;

public interface OracleR2dbcObject extends io.r2dbc.spi.Readable {

  OracleR2dbcObjectMetadata getMetadata();

}
