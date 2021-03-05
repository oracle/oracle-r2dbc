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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that
 * {@link oracle.r2dbc.impl.SqlParameterParser} implements behavior that is
 * specified in it's class and method level javadocs. Test cases in this class
 * are not concerned with using valid SQL since the parser is not specified to
 * recognize valid SQL; The parser's specification is to recognize parameter
 * markers that appear in an arbitrary sequence of characters.
 */
public class SqlParameterParserTest {

  /**
   * Verifies the implementation of
   * {@link SqlParameterParser#parse(String)} when the input contains no
   * parameter markers.
   */
  @Test
  public void testNoParameters() {
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "SELECT * FROM table WHERE x = 0"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "delete from dual"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "merge tab into dual"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select {fn locate(bob(carol(),ted(alice,sue)), 'xfy')} from dual"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "ALTER SESSION SET TIME"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "SELECT ename FROM emp WHERE hiredate BETWEEN {ts'1980-12-17'}" +
          " AND {ts '1981-09-28'} "));
  }

  /**
   * Verifies the implementation of
   * {@link SqlParameterParser#parse(String)}  when the input contains unnamed
   * parameter markers, encoded as the question mark character: ?
   */
  @Test
  public void testUnnamedParameters() {
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "SELECT * FROM table WHERE x = ?"));
    assertEquals(
      Arrays.asList(null, null, null),
      SqlParameterParser.parse(
        "INSERT INTO table(val1, val2, val3) VALUES (?, ?, ?)"));
    assertEquals(
      Arrays.asList(null, null),
      SqlParameterParser.parse(
        "SELECT id, name\n"
        +"FROM pets\n"
        +"WHERE species = ? -- 1st parameter\n"
        +"AND date_of_birth > ? -- 2nd parameter\n"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "select ? from dual"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "insert into dual values (?)"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "update dual set dummy = ?"));
    assertEquals(
      Arrays.asList(null, null, null),
      SqlParameterParser.parse(
        " select ? from dual where ? = ?"));
    assertEquals(
      Arrays.asList(null, null, null),
      SqlParameterParser.parse(
        "select ?from dual where?=?for update"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select '?', n'?', q'???', q'{?}', q'{cat's}' from dual"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select'?',n'?',q'???',q'{?}',q'{cat's}'from dual"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "select--line\n? from dual"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "select --line\n? from dual"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "--line\nselect ? from dual"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        " --line\nselect ? from dual"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "--line\n select ? from dual"));
  }

  /**
   * Verifies the implementation of
   * {@link SqlParameterParser#parse(String)} when the input contains named
   * parameter markers, encoded as colon character prefixed names: :name
   */
  @Test
  public void testNamedParameters() {
    assertEquals(
      Arrays.asList("x", "y"),
      SqlParameterParser.parse(
        "SELECT * FROM table WHERE x = :x AND y <> :y"));
    assertEquals(
      Arrays.asList("one", "two", "three"),
      SqlParameterParser.parse(
        "INSERT INTO table(val1, val2, val3) VALUES (:one, :two, :three)"));
    assertEquals(
      Arrays.asList("1", "2", "3"),
      SqlParameterParser.parse(
        "INSERT INTO table(val1, val2, val3) VALUES (:1, :2, :3)"));
    assertEquals(
      Arrays.asList("1uno", "dos2", "tre3s"),
      SqlParameterParser.parse(
        "INSERT INTO table(val1, val2, val3) VALUES (:1uno, :dos2, :tre3s)"));
    assertEquals(
      Arrays.asList("species", "dob"),
      SqlParameterParser.parse(
        "SELECT id, name\n"
          +"FROM pets\n"
          +"WHERE species = :species -- 1st parameter\n"
          +"AND date_of_birth > :dob -- 2nd parameter\n"));
    assertEquals(
      Arrays.asList("x1", "x2", "x3", "x4"),
      SqlParameterParser.parse(
        "begin proc4in4out (:x1, :x2, :x3, :x4); end;"));
    assertEquals(
      Arrays.asList("pin", "pinout", "pout"),
      SqlParameterParser.parse(
        "{CALL tkpjpn01(:pin, :pinout, :pout)}"));
    assertEquals(
      Arrays.asList("NumberBindVar"),
      SqlParameterParser.parse(
        "select :NumberBindVar as the_number from dual"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select : as the_number from dual"));
  }

  /**
   * Verifies the implementation of
   * {@link SqlParameterParser#parse(String)} when the input contains comments,
   * encoded as two consecutive minus characters prefixing a line:
   * -- comment
   * , or as a forward slash star prefixed block terminated by start forward
   * slash:
   * /* comment *\/
   */
  @Test
  public void testComments() {
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "/*\n"
        +"This query will return all questions which don't end with\n"
        +"the proper punctuation symbol, which is the question mark: ?\n"
        +"*/"
        +"SELECT question_text\n"
        +"FROM questions\n"
        +"-- Match any text that doesn't end with a ? character\n"
        +"WHERE question_text NOT LIKE '%?'"));
    assertEquals(
      Arrays.asList(null, "department_id"),
      SqlParameterParser.parse(
        "SELECT /*+ PARALLEL(employees 3) */?, " +
          "e.last_name, d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE  e.department_id=:department_id;"));
    assertEquals(
      Arrays.asList(null, null),
      SqlParameterParser.parse(
        "SELECT /*+ PARALLEL(employees 3) */?, " +
          "e.last_name, d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE  e.department_id=--\n?"));
    assertEquals(
      Arrays.asList("id"),
      SqlParameterParser.parse(
        "SELECT e.last_name, d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE  e.department_id=/**/:id"));
    assertEquals(
      Arrays.asList("id"),
      SqlParameterParser.parse(
        "SELECT e.last_name, d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE  e.department_id=/***/:id"));
    assertEquals(
      Arrays.asList("id"),
      SqlParameterParser.parse(
        "SELECT e.last_name, d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE  e.department_id=/**id**/:id"));
    assertEquals(
      Arrays.asList("id"),
      SqlParameterParser.parse(
        "SELECT e.last_name,:id,d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE e.department_id=/"));
    assertEquals(
      Arrays.asList("id"),
      SqlParameterParser.parse(
        "SELECT e.last_name,:id,d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE e.department_id=/*"));
    assertEquals(
      Arrays.asList("id"),
      SqlParameterParser.parse(
        "SELECT e.last_name,:id,d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE e.department_id=/* "));
    assertEquals(
      Arrays.asList("id"),
      SqlParameterParser.parse(
        "SELECT e.last_name,:id,d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE e.department_id=/* *"));
    assertEquals(
      Arrays.asList("id", null),
      SqlParameterParser.parse(
        "SELECT e.last_name,:id,d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE e.last_name-1=?"));
    assertEquals(
      Arrays.asList("id", null),
      SqlParameterParser.parse(
        "SELECT e.last_name,:id,d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "WHERE i=-?-"));
    assertEquals(
      Arrays.asList("id"),
      SqlParameterParser.parse(
        "SELECT e.last_name,:id,d.department_name\n" +
          "FROM   employees e, departments d\n" +
          "--"));
  }

  /**
   * Verifies the implementation of
   * {@link SqlParameterParser#parse(String)}  when the input contains quoted
   * text literals, encoded as single or double quote enclosed characters:
   * 'text literal' or "text literal".
   */
  @Test
  public void testQuotes() {
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "CREATE USER vijay6 IDENTIFIED BY \"vjay?\""));
    assertEquals(
      Arrays.asList(null, null),
      SqlParameterParser.parse(
        "SELECT ?, 'Quoth the Raven ''Nevermore.''', ? FROM dual"));
    assertEquals(
      Arrays.asList(null, null),
      SqlParameterParser.parse(
        "SELECT ?, 'Quoth the Raven \"Nevermore.\"', ? FROM dual"));
    assertEquals(
      Arrays.asList(null, null),
      SqlParameterParser.parse(
        "SELECT ?, '', ? FROM dual"));
    assertEquals(
      Arrays.asList(null, null),
      SqlParameterParser.parse(
        "SELECT ?, '''', ? FROM dual"));
    assertEquals(
      Arrays.asList(null, null),
      SqlParameterParser.parse(
        "SELECT ?, \"\", ? FROM dual"));
    assertEquals(
      Arrays.asList("x"),
      SqlParameterParser.parse(
        ":x'"));
    assertEquals(
      Arrays.asList("x"),
      SqlParameterParser.parse(
        ":x '"));
  }

  /**
   * Verifies the implementation of
   * {@link SqlParameterParser#parse(String)} when the input contains quoted
   * string literals, encoded as the q or Q characters followed by a single
   * quote and alternative delimiter enclosed characters:
   * q'{text literal}' or Q'?text literal?'
   */
  @Test
  public void testAlternativeQuotes() {
    assertEquals(
      Arrays.asList(null, "y"),
      SqlParameterParser.parse(
        "SELECT * FROM table WHERE x = ? AND y <> :y" +
          " AND z = q'?this is a text literal?'"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select q'(bob' ? )' from dual"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "select q'(bob' ? )' from dual where name=?"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select q'{bob' ? )}' from dual"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select QQQ'[bob' ? ]]' from dual"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select Qq'<> bob' '? >' from dual"));
    assertEquals(
      Arrays.asList(null, "x"),
      SqlParameterParser.parse(
        "select Qq'<> bob' '? >>', ? from dual where x = :x"));
    assertEquals(
      Collections.emptyList(),
      SqlParameterParser.parse(
        "select Q'? bob' '? ?' from dual"));
    assertEquals(
      Arrays.asList("a", "b", null),
      SqlParameterParser.parse(
        "select :a, Q'? bob' '? ?', :b, ? from dual"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y = {\\'0?':y\\\\} AND z = ?"));
    assertEquals(
      Arrays.asList("zzz"),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y = q'''0'?'' AND z = :zzz"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y = q'!name LIKE '%DBMS_%%'!'"
        + " AND z=?"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y =q'<'So,' she said, 'It's finished.'>'"
          + " AND z=?"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE" +
          " y =q'{SELECT * FROM employees WHERE last_name = 'Smith';}'"
          + " AND z=?"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y =nq'?? ??1234 ??'  AND z=?"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y =q'\"name like '['\"' AND z=?"));
    assertEquals(
      Arrays.asList("why"),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y=:why z =q'"));
    assertEquals(
      Arrays.asList("why"),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y=:why z =q' "));
    assertEquals(
      Arrays.asList("why"),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y=:why z =q'?:"));
    assertEquals(
      Arrays.asList("why"),
      SqlParameterParser.parse(
        "SELECT x FROM table WHERE y=:why z =q'??"));
  }

  /**
   * Verifies the implementation of
   * {@link SqlParameterParser#parse(String)}  when the input contains an
   * escaped unnamed parameter marker, encoded as a curly bracket backslash
   * enclosed marker: {\ ? \}
   */
  @Test
  public void testEscaped() {
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "select T.firstW, T.lastZ, ? " +  // use of parameter marker
        "from tkpattern_S11 " +
        "MATCH_RECOGNIZE ( " +
        "    MEASURES A.c1 as firstW, last(Z.c1) as lastZ " +
        "    ALL MATCHES " +
        "    {\\ PATTERN(A? X*? Y+? Z??)\\} " +  // use of escape sequence
        "    DEFINE " +
        "        X as X.c2 > prev(X.c2), " +
        "        Y as Y.c2 < prev(Y.c2), " +
        "        Z as Z.c2 > prev(Z.c2)" +
        ") as T"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "{ ?"));
    assertEquals(
      Arrays.asList("a"),
      SqlParameterParser.parse(
        ":a{\\ ?"));
    assertEquals(
      Arrays.asList("a"),
      SqlParameterParser.parse(
        ":a{\\ ?\\"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "{\\ ?\\}?"));
    assertEquals(
      Arrays.asList(new Object[1]),
      SqlParameterParser.parse(
        "{\\ ?\\\\}?"));
  }
}
/*
   MODIFIED           (MM/DD/YY)
    michael-a-mcmahon 09/20/20 - Creation
*/
