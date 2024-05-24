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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A no-op implementation of {@link AsyncLock} for use with 23.1 and newer
 * versions of Oracle JDBC. All methods are implemented by immediately executing
 * operations without acquiring a lock.
 */
final class NoOpAsyncLock implements AsyncLock {

  @Override
  public void lock(Runnable callback) {
    callback.run();
  }

  @Override
  public Publisher<Void> run(OracleR2dbcExceptions.JdbcRunnable jdbcRunnable) {
    return Mono.fromRunnable(jdbcRunnable);
  }

  @Override
  public <T> Publisher<T> get(
    OracleR2dbcExceptions.JdbcSupplier<T> jdbcSupplier) {
    return Mono.fromSupplier(jdbcSupplier);
  }

  @Override
  public <T> Publisher<T> flatMap(
    OracleR2dbcExceptions.JdbcSupplier<Publisher<T>> publisherSupplier) {
    return Flux.defer(publisherSupplier);
  }

  @Override
  public <T> Publisher<T> lock(Publisher<T> publisher) {
    return publisher;
  }
}
