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

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static java.util.Arrays.asList;
import static oracle.r2dbc.DatabaseConfig.connectTimeout;
import static oracle.r2dbc.DatabaseConfig.sharedConnection;
import static oracle.r2dbc.util.Awaits.awaitMany;
import static oracle.r2dbc.util.Awaits.awaitNone;
import static oracle.r2dbc.util.Awaits.awaitOne;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that
 * {@link OracleBatchImpl} implements behavior that is specified in
 * it's class and method level javadocs.
 */
public class OracleBatchImplTest {

  /**
   * Verifies the implementation of
   * {@link OracleBatchImpl#add(String)}
   */
  @Test
  public void testAdd() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      Batch batch = connection.createBatch();

      // Expect IllegalArgumentException from null SQL
      assertThrows(IllegalArgumentException.class, () -> batch.add(null));

      // Expect add to return the same object
      assertTrue(batch == batch.add("SELECT x FROM dual"));
    }
    finally {
      awaitNone(connection.close());
    }
  }

  /**
   * Verifies the implementation of
   * {@link OracleBatchImpl#execute()}
   */
  @Test
  public void testExecute() {
    Connection connection =
      Mono.from(sharedConnection()).block(connectTimeout());
    try {
      Batch batch = connection.createBatch();

      // Expect empty batch publisher to emit onComplete
      awaitNone(batch.execute());

      // Expect batch of 1 to emit 1 value
      awaitOne(1, Flux.from(batch.add(
        "SELECT 1 FROM dual")
        .execute())
        .flatMap(result ->
          result.map((row, metadata) -> row.get(0, Integer.class))));

      // Expect the Batch to be cleared after execute() is called
      awaitNone(batch.execute());

      // Expect statements to execute in order
      awaitMany(asList(1, 2, 3, 4, 5),
        Flux.from(batch.add("SELECT 1 FROM dual")
          .add("SELECT 2 FROM dual")
          .add("SELECT 3 FROM dual")
          .add("SELECT 4 FROM dual")
          .add("SELECT 5 FROM dual")
          .execute())
          .flatMap(result ->
            result.map((row, metadata) -> row.get(0, Integer.class))));

    }
    finally {
      awaitNone(connection.close());
    }
  }
}
