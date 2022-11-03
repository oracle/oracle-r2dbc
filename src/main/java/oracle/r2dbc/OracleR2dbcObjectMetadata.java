package oracle.r2dbc;

import io.r2dbc.spi.ReadableMetadata;

import java.util.List;
import java.util.NoSuchElementException;

public interface OracleR2dbcObjectMetadata {

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
