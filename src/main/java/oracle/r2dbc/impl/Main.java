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

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.jar.Manifest;

/**
 * <p>
 * A public class implementing a main method that may be executed from a
 * command line. This class is specified as the Main-Class attribute in
 * the META-INF/MANIFEST.MF of the Oracle R2DBC jar.
 * </p><p><i>
 * The behavior implemented by this class may change between minor and patch
 * version release updates.
 * </i></p><p>
 * The following command, in
 * which "x.y.z" is the semantic version number of the jar,
 * executes the main method of this class:
 * </p><pre>
 *   java -jar oracle-r2dbc-x.y.z.jar
 * </pre><p>
 * Since version 0.1.1, the main method is implemented to exit after printing
 * a message to the standard output stream. The message includes the version
 * numbers of the Oracle R2DBC Driver along with the JDK that compiled it. A
 * timestamp captured at the moment when the jar was compiled is also included.
 * </p>
 *
 * @since 0.1.1
 * @author Michael-A-McMahon
 */
public final class Main {

  private Main() {/*This class has no instance fields*/}

  /**
   * Prints information about this build of Oracle R2DBC. This method attempts
   * to read a "Build-Info" attribute from META-INF/MANIFEST.MF. If the
   * manifest is not available to the class loader, or if the manifest does
   * not contain a Build-Info attribute, then this method prints a message
   * indicating that build information can not be located.
   * @param args ignored
   * @throws IOException If the META-INF/MANIFEST.MF resource can not be read.
   */
  public static void main(String[] args) throws IOException {

    InputStream manifestStream =
      Main.class.getModule().getResourceAsStream("META-INF/MANIFEST.MF");

    if (manifestStream == null) {
      System.out.println("META-INF/MANIFEST.MF not found");
      return;
    }

    try (manifestStream) {
      System.out.println(Objects.requireNonNullElse(
        new Manifest(manifestStream)
          .getMainAttributes()
          .getValue("Build-Info"),
        "Build-Info is missing from the manifest"));
    }
  }
}
