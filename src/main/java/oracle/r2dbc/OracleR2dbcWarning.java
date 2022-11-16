/*
  Copyright (c) 2020, 2022, Oracle and/or its affiliates.

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
package oracle.r2dbc;

import io.r2dbc.spi.Result;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * <p>
 * A subtype of {@link Result.Message} that provides information on warnings
 * raised by Oracle Database.
 * </p><p>
 * When a SQL command results in a warning, Oracle R2DBC emits a {@link Result}
 * with an {@code OracleR2dbcWarning} segment in addition to any other segments
 * that resulted from the SQL command. For example, if a SQL {@code SELECT}
 * command results in a warning, then an {@code OracleR2dbcWarning} segment is
 * included with the result, along with any {@link Result.RowSegment}s returned
 * by the {@code SELECT}.
 * </p><p>
 * R2DBC drivers typically emit {@code onError} signals for {@code Message}
 * segments that are not consumed by {@link Result#filter(Predicate)} or
 * {@link Result#flatMap(Function)}. Oracle R2DBC does not apply this behavior
 * for warning messages. If an {@code OracleR2dbcWarning}
 * segment is not consumed by the {@code filter} or {@code flatMap} methods of
 * a {@code Result}, then the warning is discarded and the result may be
 * consumed as normal with with the {@code map} or {@code getRowsUpdated}
 * methods.
 * </p><p>
 * Warning messages may be consumed with {@link Result#flatMap(Function)}:
 * </p><pre>{@code
 * result.flatMap(segment -> {
 *   if (segment instanceof OracleR2dbcWarning) {
 *     logWarning(((OracleR2dbcWarning)segment).getMessage());
 *     return emptyPublisher();
 *   }
 *   else {
 *     ... handle other segment types ...
 *   }
 * })
 * }</pre><p>
 * A {@code flatMap} function may also be used to convert a warning into an
 * {@code onError} signal:
 * </p><pre>{@code
 * result.flatMap(segment -> {
 *   if (segment instanceof OracleR2dbcWarning) {
 *     return errorPublisher(((OracleR2dbcWarning)segment).warning());
 *   }
 *   else {
 *     ... handle other segment types ...
 *   }
 * })
 * }</pre>
 * @since 1.1.0
 */
public interface OracleR2dbcWarning extends Result.Message {

}