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

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Option;
import oracle.r2dbc.OracleR2dbcOptions;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A connection factory having {@link io.r2dbc.spi.ConnectionFactoryOptions}
 * with values provided by a {@link Supplier} or {@link Publisher}. Supplied
 * values are requested when {@link #create()} is called. After all requested
 * values are supplied, this factory delegates to
 * {@link OracleConnectionFactoryProviderImpl#create(ConnectionFactoryOptions)},
 * with {@link ConnectionFactoryOptions} composed of the supplied values.
 */
final class SuppliedOptionConnectionFactory implements ConnectionFactory {

  /**
   * The set of all options recognized by Oracle R2DBC. This set includes
   * the standard options declared by {@link ConnectionFactoryOptions} and
   * the extended options declared by {@link OracleR2dbcOptions}.
   *
   * TODO: This set only includes standard options defined for version 1.0.0 of
   *   the SPI. If a future SPI version introduces new options, those must be
   *   added to this set.
   */
  private static final Set<Option<?>> ALL_OPTIONS =
    Stream.concat(
        // Standard options:
        Stream.of(
          ConnectionFactoryOptions.CONNECT_TIMEOUT,
          ConnectionFactoryOptions.DATABASE,
          ConnectionFactoryOptions.DRIVER,
          ConnectionFactoryOptions.HOST,
          ConnectionFactoryOptions.LOCK_WAIT_TIMEOUT,
          ConnectionFactoryOptions.PASSWORD,
          ConnectionFactoryOptions.PORT,
          ConnectionFactoryOptions.PROTOCOL,
          ConnectionFactoryOptions.SSL,
          ConnectionFactoryOptions.STATEMENT_TIMEOUT,
          ConnectionFactoryOptions.USER),
        // Extended options:
        OracleR2dbcOptions.options().stream())
      .collect(Collectors.toUnmodifiableSet());

  /**
   * Publishers which emit the value of an option. The value may come from a
   * user provided Publisher, from a user provided Supplier, or just a user
   * provided value. So, all the option values below would get turned into a
   * Publisher which is added to this set:
   * <pre>{@code
   * ConnectionFactoryOptions.builder()
   *   .option(DRIVER, "oracle")
   *   .option(supplied(HOST, () -> "database-host")
   *   .option(published(PORT, Mono.just(1521))
   * }</pre>
   */
  private final Set<Publisher<OptionValue>> optionValuePublishers;

  SuppliedOptionConnectionFactory(ConnectionFactoryOptions options) {
    optionValuePublishers = ALL_OPTIONS.stream()
      .map(option -> toOptionValuePublisher(option, options.getValue(option)))
      .filter(Objects::nonNull)
      .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public Publisher<? extends Connection> create() {
    return Flux.merge(optionValuePublishers)
      .collectList()
      .map(SuppliedOptionConnectionFactory::toConnectionFactoryOptions)
      .flatMap(options ->
        Mono.from(new OracleConnectionFactoryProviderImpl()
          .create(options)
          .create()));
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return OracleConnectionFactoryMetadataImpl.INSTANCE;
  }

  /**
   * Converts an {@link Option} and its value to a {@link Publisher} that
   * emits an {@link OptionValue}. If the <code>value</code> passed to this
   * method is a Supplier or Publisher, then the Publisher returned by this
   * method emits an <code>OptionValue</code> with the value supplied by the
   * Supplier or Publisher. If the <code>value</code> passed to this method is
   * not a Supplier or Publisher, then the Publisher returned by this method
   * just emits an <code>OptionValue</code> with the given <code>value</code>.
   *
   * @param option An option. Not null.
   * @param value The value of the option, or null if there is no value, or a
   * <code>Supplier</code> or <code>Publisher</code> which supplies the value of
   * the option.
   * @return A publisher that emits the option and its possibly supplied value,
   * or this method returns null if the given <code>value</code> is null.
   */
  private static Publisher<OptionValue> toOptionValuePublisher(
    Option<?> option, Object value) {
    final Publisher<?> valuePublisher;

    if (value == null)
      return null;

    if (value instanceof Supplier)
      valuePublisher = Mono.fromSupplier((Supplier<?>)value);
    else if (value instanceof Publisher)
      valuePublisher = Mono.from((Publisher<?>)value);
    else
      return Mono.just(new OptionValue(option, value));

    return Mono.from(valuePublisher)
      .map(publishedValue -> new OptionValue(option, publishedValue))
      .onErrorMap(error -> OracleR2dbcExceptions.newNonTransientException(
        "Error when requesting a value of " + option
          + " from a Supplier or Publisher",
        null, // sql
        error));
  }

  /**
   * Converts a collection of options and values into an instance of
   * {@link ConnectionFactoryOptions}.
   *
   * @param optionValues Iterable options and values. Not null.
   * @return <code>ConnectionFactoryOptions</code> configured with the given
   * options and values. Not null.
   */
  private static ConnectionFactoryOptions toConnectionFactoryOptions(
    Iterable<OptionValue> optionValues) {

    ConnectionFactoryOptions.Builder optionsBuilder =
      ConnectionFactoryOptions.builder();

    optionValues.forEach(optionValue ->
      optionValue.configure(optionsBuilder));

    return optionsBuilder.build();
  }

  /**
   * Checks if the value of an option is supplied by a {@link Supplier} or
   * {@link Publisher}. This method is used to check if
   * {@link SuppliedOptionConnectionFactory} should be used to handle any
   * supplied option values. If this method returns false, then there is no
   * reason to use this connection factory.
   *
   * @param options Options that may a value which is a {@link Supplier} or
   * {@link Publisher}.
   * @return true if the value of at least one option is a {@link Supplier} or
   * {@link Publisher}. Returns false otherwise.
   */
  static boolean containsSuppliedValue(
    ConnectionFactoryOptions options) {
    return ALL_OPTIONS.stream()
      .map(options::getValue)
      .anyMatch(value ->
        value instanceof Supplier || value instanceof Publisher);
  }

  /** A record of an {@link Option} and its value */
  private static final class OptionValue {

    final Option<?> option;

    final Object value;

    OptionValue(Option<?> option, Object value) {
      this.option = option;
      this.value = value;
    }

    /**
     * @param builder Builder to configure with the {@link #value} of an
     * {@link #option}. Not null.
     */
    void configure(ConnectionFactoryOptions.Builder builder) {
      @SuppressWarnings("unchecked")
      Option<Object> option = (Option<Object>)this.option;
      builder.option(option, value);
    }

  }
}
