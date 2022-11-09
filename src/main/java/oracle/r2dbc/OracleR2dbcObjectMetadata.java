package oracle.r2dbc;

import io.r2dbc.spi.ReadableMetadata;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * Represents the metadata for attributes of an OBJECT. Metadata for attributes
 * can either be retrieved by index or by name. Attribute indexes are
 * {@code 0}-based. Retrieval by attribute name is case-insensitive.
 */
public interface OracleR2dbcObjectMetadata {

  /**
   * Returns the type of the OBJECT which metadata is provided for.
   * @return The type of the OBJECT. Not null.
   */
  OracleR2dbcTypes.ObjectType getObjectType();

  /**
   * Returns the {@link ReadableMetadata} for one attribute.
   *
   * @param index the attribute index starting at 0
   * @return the {@link ReadableMetadata} for one attribute. Not null.
   * @throws IndexOutOfBoundsException if {@code index} is out of range
   * (negative or equals/exceeds {@code getParameterMetadatas().size()})
   */
  ReadableMetadata getAttributeMetadata(int index);

  /**
   * Returns the {@link ReadableMetadata} for one attribute.
   *
   * @param name the name of the attribute. Not null. Parameter names are
   * case-insensitive.
   * @return the {@link ReadableMetadata} for one attribute. Not null.
   * @throws IllegalArgumentException if {@code name} is {@code null}
   * @throws NoSuchElementException if there is no attribute with the
   * {@code name}
   */
  ReadableMetadata getAttributeMetadata(String name);

  /**
   * Returns the {@link ReadableMetadata} for all attributes.
   *
   * @return the {@link ReadableMetadata} for all attributes. Not null.
   */
  List<? extends ReadableMetadata> getAttributeMetadatas();
}
