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

/**
 * <p>
 * Implements the R2DBC SPI for the Oracle Database. Classes in this package are
 * concrete implementations of interfaces defined in the io.r2dbc.spi package.
 * For maximum portability between different R2DBC drivers, application
 * developers should program against the io.r2dbc.spi interfaces only, without
 * referencing any classes defined in this package.
 * </p><p>
 * {@link oracle.r2dbc.impl.ReactiveJdbcAdapter} defines a reactive interface
 * to be implemented using any non-standard APIs that a JDBC driver may provide
 * for asynchronous database access. The Oracle R2DBC Driver relies on an
 * instance of this adapter in order to implement most R2DBC SPI methods.
 * {@link oracle.r2dbc.impl.OracleReactiveJdbcAdapter} is the only concrete
 * implementation of this adapter currently.
 * </p>
 */
package oracle.r2dbc.impl;
