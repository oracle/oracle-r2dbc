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

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Parser that analyzes a character sequence for the purpose of identifying
 * parameter markers. This parser will recognize parameter markers in the
 * following forms:
 * </p><ul>
 *   <li>
 *     A <code>?</code> character is an unnamed parameter marker.
 *   </li>
 *   <li>
 *     A <code>:</code> character followed by one or more alphabetical or
 *     numerical characters is a named parameter marker.
 *   </li>
 * </ul><p>
 * The following SQL statement is an example in which there are two unnamed
 * parameters:
 * </p><pre>
 *   SELECT id, name
 *   FROM pets
 *   WHERE species = ? -- 1st parameter
 *   AND date_of_birth = ? -- 2nd parameter
 * </pre><p>
 * The following SQL statement is the same as the previous example,
 * except the two parameters now have names: "species" and "dob":
 * </p><pre>
 *   SELECT id, name
 *   FROM pets
 *   WHERE species = :species -- 1st parameter
 *   AND date_of_birth = :dob -- 2nd parameter
 * </pre><p>
 * Parameter markers are ignored when they appear in comments or quoted text
 * literals, such as:
 * </p><pre>
 *   /*
 *   This query will return all questions that don't end with
 *   a question mark: ?
 *   *&#47;
 *   SELECT question_text
 *   FROM questions
 *   -- Match any text that doesn't end with a ? character
 *   WHERE question_text NOT LIKE '%?'
 * </pre><p>
 * Unnamed parameter markers are ignored when they appear in a character
 * sequence that begins with {\ and ends with \}
 * such as:
 * </p><pre>
 *   SELECT T.firstW, T.lastZ
 *   FROM tkpattern_S11
 *   MATCH_RECOGNIZE (
 *     MEASURES A.c1 as firstW, last(Z.c1) as lastZ
 *     ALL MATCHES
 *     {\ PATTERN(A? X*? Y+? Z??)\} -- Escaped ? symbols
 *     DEFINE
 *       {@code X as X.c2 > prev(X.c2),}
 *       {@code Y as Y.c2 < prev(Y.c2),}
 *       {@code Z as Z.c2 > prev(Z.c2)}
 *   ) as T
 * </pre><p>
 * Using a notation similar to the one defined in Section 2.4 of The Java
 * Language Specification, Java SE 15 Edition, the grammar recognized by
 * this parser is described as follows.
 * </p><pre>
 *   Sql:
 *     {CommentQuoteCommand}
 *
 *     CommentQuoteCommand:
 *       Comment
 *       Quote
 *       Command
 *
 *       Comment:
 *         CommentLine
 *         CommentBlock
 *
 *           CommentLine:
 *             - - {NotNewline}
 *
 *             NotNewLine:
 *               (Any character that is not equal to \n)
 *
 *           CommentBlock:
 *             / * {AnyCharacter} * /
 *
 *             AnyCharacter:
 *               (Any character)
 *
 *       Quote:
 *         SingleQuote
 *         DoubleQuote
 *         AlternativeQuote
 *
 *         SingleQuote:
 *           ' {NotSingleQuote} '
 *
 *           NotSingleQuote:
 *             (Any character that is not equal to ')
 *             EscapedSingleQuote
 *
 *              EscapedSingleQuote:
 *              ''
 *
 *         DoubleQuote:
 *           " {NotDoubleQuote} "
 *
 *           NotDoubleQuote:
 *             (Any character that is not equal to ")
 *
 *        AlternativeQuote:
 *          AnyCaseQ ' ( {AnyCharacter} ) '
 *          AnyCaseQ ' { {AnyCharacter} } '
 *          AnyCaseQ ' [ {AnyCharacter} ] '
 *          {@code AnyCaseQ ' < {AnyCharacter} > '}
 *          AnyCaseQ ' Delimiter {AnyCharacter} Delimiter '
 *            (The starting and ending Delimiter are the same character)
 *
 *          AnyCaseQ:
 *            q
 *            Q
 *
 *          Delimiter:
 *            (Any character that is not a space, newline, or tab)
 *
 *        Command:
 *          {CommentQuoteEscaped} Parameter {Command}
 *
 *            CommentQuoteEscaped
 *              Comment
 *              Quote
 *              Escaped
 *
 *                Escaped:
 *                  { \ {AnyCharacter} \ }
 *
 *            Parameter:
 *              UnnamedParameter
 *              NamedParameter
 *
 *                UnnamedParameter:
 *                  ?
 *
 *                NamedParameter:
 *                  : {AlphaNumericUnderscore}
 *
 *                  AlphaNumericUnderscore
 *                    (Any character from a to z)
 *                    (Any character from A to Z)
 *                    (Any character from 0 to 9)
 *                    _
 * </pre>
 *
 * @author  michael-a-mcmahon
 * @since   0.1.0
 */
final class SqlParameterParser {

  /**
   * This class has no instance methods, so this constructor should never be
   * called.
   */
  private SqlParameterParser() { }

  /**
   * <p>
   * Parses parameter markers that appear in the text of a {@code sql}
   * statement. The returned list contains an entry for each parameter marker
   * identified during the parse.
   * </p><p>
   * For named parameters, the returned list contains a {@code String} of the
   * alphanumeric characters that follow the {@code :} symbol. For unnamed
   * parameters, the list contains a {@code null} value.
   * </p><p>
   * Entries in the returned list are ordered as they appear in the SQL
   * statement when it is read from left to right.
   * </p>
   * @param sql A SQL statement. Not null.
   * @return A list containing one entry for each parameter. Not null. Not
   * retained.
   */
  static List<String> parse(String sql) {
    ArrayList<String> parameterNames = new ArrayList<>();
    int i = 0;
    while (i < sql.length()) { // CommentQuoteCommand
      switch (sql.charAt(i)) {
        case '-' : // CommentLine
          i = parseCommentLine(sql, i);
          break;
        case '/' : // CommentBlock
          i = parseCommentBlock(sql, i);
          break;
        case '\'' : // SingleQuote
          i = parseSingleQuote(sql, i);
          break;
        case '"' : // DoubleQuote
          i = parseDoubleQuote(sql, i);
          break;
        case 'q':
        case 'Q': // AlternativeQuote
          i = parseAlternativeQuote(sql, i);
          break;
        case '{' : // Escaped
          i = parseEscaped(sql, i);
          break;
        case '?' : // UnnamedParameter
          i++;
          parameterNames.add(null);
          break;
        case ':' : // NamedParameter
          String name = parseNamedParameter(sql, i);
          if (name.length() == 0) {
            i++;
          }
          else {
            i += name.length();
            parameterNames.add(name);
          }
          break;
        default:
          i++;
      }
    }

    return parameterNames;
  }

  /**
   *  Parses the Escaped symbol, which is defined as:
   *  <pre>
   *  { \ {AnyCharacter} \ }
   *  </pre>
   * @param sql A character sequence to parse
   * @param i index within {@code sql} the of first { character in the
   *          Escaped symbol.
   * @return the index of the first character that does not match the symbol,
   *  or the index after the terminating } character, or {@code sql.length()} if
   *  the symbol is not terminated
   */
  private static int parseEscaped(String sql, int i) {
    if (++i == sql.length() || '\\' != sql.charAt(i)) { // \ {AnyCharacter} \ }
      return i;
    }
    else {
      i++;

      do {
        i = sql.indexOf('\\', i); // {AnyCharacter} \ }

        if (i < 0)
          return sql.length();
        else if (++i == sql.length())
          return sql.length();
        else if ('}' == sql.charAt(i)) // }
          return i + 1;
      } while (true);
    }
  }

  /**
   *  Parses the AlternativeQuote symbol, which is defined as:
   *  <pre>
   *  AlternativeQuote:
   *    AnyCaseQ ' ( {AnyCharacter} ) '
   *    AnyCaseQ ' { {AnyCharacter} } '
   *    AnyCaseQ ' [ {AnyCharacter} ] '
   *    {@code AnyCaseQ ' < {AnyCharacter} > '}
   *    AnyCaseQ ' Delimiter {AnyCharacter} Delimiter '
   *      (The starting and ending Delimiter are the same character)
   *
   *    AnyCaseQ:
   *      q
   *      Q
   *
   *    Delimiter:
   *      AnyCharacter
   *  </pre>
   * @param sql A character sequence to parse
   * @param i index within {@code sql} the of first AnyCaseQ character in the
   *          AlternativeQuote symbol.
   * @return the index of the first character that does not match the symbol,
   *  or the index after the terminating ' character, or {@code sql.length()} if
   *  the symbol is not terminated
   */
  private static int parseAlternativeQuote(String sql, int i) {
    // ' Delimiter {AnyCharacter} Delimiter '
    if (++i == sql.length() || '\'' != sql.charAt(i)) {
      return i;
    }
    else if (++i == sql.length()) {
      return sql.length();
    }
    else {
      char delimiter = sql.charAt(i); // Delimiter {AnyCharacter} Delimiter '
      if (delimiter == ' ' || delimiter == '\t' || delimiter == '\n') {
        return i;
      }
      else {
        char terminal = alternateQuoteTerminal(delimiter);

        i++;
        do {
          i = sql.indexOf(terminal, i); // {AnyCharacter} Delimiter '

          if (i < 0 || ++i == sql.length())
            return sql.length();
          else if ('\'' == sql.charAt(i)) // '
            return i + 1;
        } while (true);
      }
    }
  }

  /**
   * Returns the terminal character for a {@code delimiter} of an
   * AlternativeQuote.
   * @param delimiter The delimiter of an AlternativeQuote
   * @return The terminal character which corresponds to the {@code delimiter}.
   */
  private static char alternateQuoteTerminal(char delimiter) {
    switch (delimiter) {
      case '(': // AnyCaseQ ' ( {AnyCharacter} ) '
        return ')';
      case '{': // AnyCaseQ ' { {AnyCharacter} } '
        return '}';
      case '[': // AnyCaseQ ' [ {AnyCharacter} ] '
        return ']';
      case '<': // AnyCaseQ ' < {AnyCharacter} > '
        return '>';
      default: // AnyCaseQ ' Delimiter {AnyCharacter} Delimiter '
        return delimiter;
    }
  }

  /**
   *  Parses the NamedParameter symbol, which is defined as:
   *  <pre>
   *  NamedParameter:
   *    : {AlphaNumericUnderscore}
   *
   *    AlphaNumericUnderscore
   *      (Any character from a to z)
   *      (Any character from A to Z)
   *      (Any character from 0 to 9)
   *      _
   *  </pre>
   * @param sql A character sequence to parse
   * @param i index within {@code sql} the of : character in the
   *          NamedParameter symbol.
   * @return A String value of the AlphaNumericUnderscore character sequence
   * that follows the : character, which may be a sequence of zero length.
   * Not null.
   */
  private static String parseNamedParameter(String sql, int i) {
    final int start = ++i;
    while (i < sql.length()) {
      char next = sql.charAt(i);
      boolean isAlphaNumUnderscore = ('a' <= next && 'z' >= next)
        ||  ('A' <= next && 'Z' >= next)
        || ('0' <= next && '9' >= next)
        || '_' == next;

      if (isAlphaNumUnderscore) // {AlphaNumericUnderscore}
        i++;
      else
        break;
    }
    return sql.substring(start, i);
  }

  /**
   *  Parses the DoubleQuote symbol, which is defined as:
   *  <pre>
   *  DoubleQuote:
   *    " {NotDoubleQuote} "
   *
   *    NotDoubleQuote:
   *      (Any character that is not equal to ")
   *  </pre>
   * @param sql A character sequence to parse
   * @param i index within {@code sql} the of first " character in the
   *          DoubleQuote symbol.
   * @return the index of the first character that does not match the symbol,
   *  or the index after the terminating " character, or {@code sql.length()} if
   *  the symbol is not terminated
   */
  private static int parseDoubleQuote(String sql, int i) {
    i = sql.indexOf('"', i + 1); // {NotDoubleQuote} "
    return i >= 0 ? i + 1 : sql.length();
  }

  /**
   *  Parses the SingleQuote symbol, which is defined as:
   *  <pre>
   *  SingleQuote:
   *    ' {NotSingleQuote} '
   *
   *    NotSingleQuote:
   *      (Any character that is not equal to ')
   *      EscapedSingleQuote
   *
   *      EscapedSingleQuote:
   *        ''
   *  </pre>
   * @param sql A character sequence to parse
   * @param i index within {@code sql} the of first ' character in the
   *          SingleQuote symbol.
   * @return the index of the first character that does not match the symbol,
   *  or the index after the terminating ' character, or {@code sql.length()} if
   *  the symbol is not terminated
   */
  private static int parseSingleQuote(String sql, int i) {
    do {
      i = sql.indexOf('\'', ++i); // {NotSingleQuote} '

      if (i < 0 || ++i == sql.length())
        return sql.length();
      else if ('\'' != sql.charAt(i)) // ' (Not EscapedSingleQuote)
        return i;
    } while (true);
  }

  /**
   *  Parses the CommentBlock symbol, which is defined as:
   *  <pre>
   *  CommentBlock:
   *    / * {AnyCharacter} * /
   *  </pre>
   * @param sql A character sequence to parse
   * @param i index within {@code sql} the of first / character in the
   *          CommentBlock symbol.
   * @return the index of the first character that does not match the symbol,
   *  or the index after the terminating / character, or {@code sql.length()} if
   *  the symbol is not terminated
   */
  private static int parseCommentBlock(String sql, int i) {
    if (++i == sql.length() || '*' != sql.charAt(i)) { // * {AnyCharacter} * /
      return i;
    }
    else {
      i++;
      do {
        i = sql.indexOf('*', i); // {AnyCharacter} * /

        if (i < 0 || ++i == sql.length())
          return sql.length();
        else if ('/' == sql.charAt(i)) // /
          return i + 1;

      } while (true);
    }

  }

  /**
   *  Parses the CommentLine symbol, which is defined as:
   *  <pre>
   *  CommentLine:
   *    - - {NotNewline}
   *  </pre>
   * @param sql A character sequence to parse
   * @param i index within {@code sql} the of first - character in the
   *          CommentLine symbol.
   * @return the index of the first character that does not match the symbol,
   *  or the index after the terminating newline character, or
   *  {@code sql.length()} if the symbol is not terminated
   */
  private static int parseCommentLine(String sql, int i) {
    if (++i == sql.length() || '-' != sql.charAt(i)) { // - {NotNewline}
      return i;
    }
    else {
      i = sql.indexOf('\n', i + 1); // // {NotNewline}
      return i >= 0 ? i + 1: sql.length();
    }
  }

}