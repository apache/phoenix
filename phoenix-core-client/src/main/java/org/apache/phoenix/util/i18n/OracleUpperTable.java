/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util.i18n;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import java.util.Locale;

/**
 * This utility class was partially copied from Salesforce's internationalization utility library
 * (com.salesforce.i18n:i18n-util:1.0.4), which was released under the 3-clause BSD License. The
 * i18n-util library is not maintained anymore, and it was using vulnerable dependencies. For more
 * info, see: https://issues.apache.org/jira/browse/PHOENIX-6818 Generated by
 * i18n.OracleUpperTableGeneratorTest
 * <p>
 * An instance of this enum codifies the difference between executing a {@link #getSqlFormatString()
 * particular PL/SQL expression} in Oracle and executing {@link String#toUpperCase(Locale)} for a
 * {@link #getLocale() particular locale} in Java. These differences (also called exceptions) are
 * expressed by the output of {@link #getUpperCaseExceptions()} and
 * {@link #getUpperCaseExceptionMapping(char)}.
 * <p>
 * The tables are generated by testing a particular set of characters that are known to contain
 * exceptions and {@link #toUpperCase(String) may be used} to compensate for exceptions found and
 * generate output in Java that will be consistent with Oracle for the given (sql expression,
 * locale) pair over all tested values.
 * <p>
 * Characters tested:
 * <ul>
 * <li>U+0069 &#x0069</li>
 * <li>U+00df &#x00df</li>
 * <li>U+0386 &#x0386</li>
 * <li>U+0388 &#x0388</li>
 * <li>U+0389 &#x0389</li>
 * <li>U+038a &#x038a</li>
 * <li>U+038c &#x038c</li>
 * <li>U+038e &#x038e</li>
 * <li>U+038f &#x038f</li>
 * <li>U+03ac &#x03ac</li>
 * <li>U+03ad &#x03ad</li>
 * <li>U+03ae &#x03ae</li>
 * <li>U+03af &#x03af</li>
 * <li>U+03cc &#x03cc</li>
 * <li>U+03cd &#x03cd</li>
 * <li>U+03ce &#x03ce</li>
 * </ul>
 * @see OracleUpper
 */
public enum OracleUpperTable {
  ENGLISH("upper(%s)", "en", "ß"),
  GERMAN("nls_upper(%s, 'nls_sort=xgerman')", "de", ""),
  FRENCH("nls_upper(%s, 'nls_sort=xfrench')", "fr", "ß"),
  ITALIAN("nls_upper(%s, 'nls_sort=italian')", "it", "ß"),
  SPANISH("nls_upper(%s, 'nls_sort=spanish')", "es", "ß"),
  CATALAN("nls_upper(%s, 'nls_sort=catalan')", "ca", "ß"),
  DUTCH("nls_upper(%s, 'nls_sort=dutch')", "nl", "ß"),
  PORTUGUESE("nls_upper(%s, 'nls_sort=west_european')", "pt", "ß"),
  DANISH("nls_upper(%s, 'nls_sort=danish')", "da", "ß"),
  NORWEGIAN("nls_upper(%s, 'nls_sort=norwegian')", "no", "ß"),
  SWEDISH("nls_upper(%s, 'nls_sort=swedish')", "sv", "ß"),
  FINNISH("nls_upper(%s, 'nls_sort=finnish')", "fi", "ß"),
  CZECH("nls_upper(%s, 'nls_sort=xczech')", "cs", "ß"),
  POLISH("nls_upper(%s, 'nls_sort=polish')", "pl", "ß"),
  TURKISH("nls_upper(translate(%s,'i','İ'), 'nls_sort=xturkish')", "tr", "ß"),
  CHINESE_HK("nls_upper(to_single_byte(%s), 'nls_sort=tchinese_radical_m')", "zh", ""),
  CHINESE_HK_STROKE("nls_upper(to_single_byte(%s), 'nls_sort=tchinese_stroke_m')", "zh", ""),
  CHINESE_TW("nls_upper(to_single_byte(%s), 'nls_sort=tchinese_radical_m')", "zh", ""),
  CHINESE_TW_STROKE("nls_upper(to_single_byte(%s), 'nls_sort=tchinese_stroke_m')", "zh", ""),
  CHINESE("nls_upper(to_single_byte(%s), 'nls_sort=schinese_radical_m')", "zh", ""),
  CHINESE_STROKE("nls_upper(to_single_byte(%s), 'nls_sort=schinese_stroke_m')", "zh", ""),
  CHINESE_PINYIN("nls_upper(to_single_byte(%s), 'nls_sort=schinese_pinyin_m')", "zh", ""),
  JAPANESE("nls_upper(to_single_byte(%s), 'nls_sort=japanese_m')", "ja", ""),
  KOREAN("nls_upper(to_single_byte(%s), 'nls_sort=korean_m')", "ko", ""),
  RUSSIAN("nls_upper(%s, 'nls_sort=russian')", "ru", "ß"),
  BULGARIAN("nls_upper(%s, 'nls_sort=bulgarian')", "bg", "ß"),
  INDONESIAN("nls_upper(%s, 'nls_sort=indonesian')", "in", "ß"),
  ROMANIAN("nls_upper(%s, 'nls_sort=romanian')", "ro", "ß"),
  VIETNAMESE("nls_upper(%s, 'nls_sort=vietnamese')", "vi", "ß"),
  UKRAINIAN("nls_upper(%s, 'nls_sort=ukrainian')", "uk", "ß"),
  HUNGARIAN("nls_upper(%s, 'nls_sort=xhungarian')", "hu", ""),
  GREEK("nls_upper(%s, 'nls_sort=greek')", "el", "ßΆΈΉΊΌΎΏάέήίόύώ"),
  HEBREW("nls_upper(%s, 'nls_sort=hebrew')", "iw", "ß"),
  SLOVAK("nls_upper(%s, 'nls_sort=slovak')", "sk", "ß"),
  SERBIAN_CYRILLIC("nls_upper(%s, 'nls_sort=generic_m')", "sr", ""),
  SERBIAN_LATIN("nls_upper(%s, 'nls_sort=xcroatian')", "sh", "ß"),
  BOSNIAN("nls_upper(%s, 'nls_sort=xcroatian')", "bs", "ß"),
  GEORGIAN("nls_upper(%s, 'nls_sort=binary')", "ka", "ß"),
  BASQUE("nls_upper(%s, 'nls_sort=west_european')", "eu", "ß"),
  MALTESE("nls_upper(%s, 'nls_sort=west_european')", "mt", "ß"),
  ROMANSH("nls_upper(%s, 'nls_sort=west_european')", "rm", "ß"),
  LUXEMBOURGISH("nls_upper(%s, 'nls_sort=west_european')", "lb", "ß"),
  IRISH("nls_upper(%s, 'nls_sort=west_european')", "ga", "ß"),
  SLOVENE("nls_upper(%s, 'nls_sort=xslovenian')", "sl", "ß"),
  CROATIAN("nls_upper(%s, 'nls_sort=xcroatian')", "hr", "ß"),
  MALAY("nls_upper(%s, 'nls_sort=malay')", "ms", "ß"),
  ARABIC("nls_upper(%s, 'nls_sort=arabic')", "ar", "ß"),
  ESTONIAN("nls_upper(%s, 'nls_sort=estonian')", "et", "ß"),
  ICELANDIC("nls_upper(%s, 'nls_sort=icelandic')", "is", "ß"),
  LATVIAN("nls_upper(%s, 'nls_sort=latvian')", "lv", "ß"),
  LITHUANIAN("nls_upper(%s, 'nls_sort=lithuanian')", "lt", "ß"),
  KYRGYZ("nls_upper(%s, 'nls_sort=binary')", "ky", "ß"),
  KAZAKH("nls_upper(%s, 'nls_sort=binary')", "kk", "ß"),
  TAJIK("nls_upper(%s, 'nls_sort=russian')", "tg", "ß"),
  BELARUSIAN("nls_upper(%s, 'nls_sort=russian')", "be", "ß"),
  TURKMEN("nls_upper(translate(%s,'i','İ'), 'nls_sort=xturkish')", "tk", "iß"),
  AZERBAIJANI("nls_upper(translate(%s,'i','İ'), 'nls_sort=xturkish')", "az", "ß"),
  ARMENIAN("nls_upper(%s, 'nls_sort=binary')", "hy", "ß"),
  THAI("nls_upper(%s, 'nls_sort=thai_dictionary')", "th", "ß"),
  HINDI("nls_upper(%s, 'nls_sort=binary')", "hi", "ß"),
  URDU("nls_upper(%s, 'nls_sort=arabic')", "ur", "ß"),
  BENGALI("nls_upper(%s, 'nls_sort=bengali')", "bn", "ß"),
  TAMIL("nls_upper(%s, 'nls_sort=binary')", "ta", "ß"),
  ESPERANTO("upper(%s)", "eo", ""),
  XWEST_EUROPEAN("NLS_UPPER(%s,'NLS_SORT=xwest_european')", "en", "");

  private final String sql;
  private final Locale locale;
  private final char[] exceptionChars;

  OracleUpperTable(String sql, String lang, String exceptionChars) {
    this.sql = sql;
    this.locale = new Locale(lang);
    this.exceptionChars = exceptionChars.toCharArray();
  }

  /**
   * Return an array containing characters for which Java's String.toUpperCase method is known to
   * deviate from the result of Oracle evaluating {@link #getSql(String) this expression}.
   * @return an array containing all exceptional characters.
   */
  final char[] getUpperCaseExceptions() {
    return exceptionChars;
  }

  /**
   * For a character, {@code exception}, contained in the String returned from
   * {@link #getUpperCaseExceptions()}, this method returns the anticipated result of upper-casing
   * the character in Oracle when evaluating {@link #getSql(String) this expression}.
   * @return the upper case of {@code exception}, according to what Oracle would do. if the
   *         character is not contained in the String returned by {@link #getUpperCaseExceptions()}.
   */
  final String getUpperCaseExceptionMapping(char exception) {
    switch (exception) {
      case 'i':
        switch (this) {
          case TURKMEN:
            return "İ"; // I
          default: // fall out
        }
        break;
      case 'ß':
        switch (this) {
          case ENGLISH:
            return "ß"; // SS
          case FRENCH:
            return "ß"; // SS
          case ITALIAN:
            return "ß"; // SS
          case SPANISH:
            return "ß"; // SS
          case CATALAN:
            return "ß"; // SS
          case DUTCH:
            return "ß"; // SS
          case PORTUGUESE:
            return "ß"; // SS
          case DANISH:
            return "ß"; // SS
          case NORWEGIAN:
            return "ß"; // SS
          case SWEDISH:
            return "ß"; // SS
          case FINNISH:
            return "ß"; // SS
          case CZECH:
            return "ß"; // SS
          case POLISH:
            return "ß"; // SS
          case TURKISH:
            return "ß"; // SS
          case RUSSIAN:
            return "ß"; // SS
          case BULGARIAN:
            return "ß"; // SS
          case INDONESIAN:
            return "ß"; // SS
          case ROMANIAN:
            return "ß"; // SS
          case VIETNAMESE:
            return "ß"; // SS
          case UKRAINIAN:
            return "ß"; // SS
          case GREEK:
            return "ß"; // SS
          case HEBREW:
            return "ß"; // SS
          case SLOVAK:
            return "ß"; // SS
          case SERBIAN_LATIN:
            return "ß"; // SS
          case BOSNIAN:
            return "ß"; // SS
          case GEORGIAN:
            return "ß"; // SS
          case BASQUE:
            return "ß"; // SS
          case MALTESE:
            return "ß"; // SS
          case ROMANSH:
            return "ß"; // SS
          case LUXEMBOURGISH:
            return "ß"; // SS
          case IRISH:
            return "ß"; // SS
          case SLOVENE:
            return "ß"; // SS
          case CROATIAN:
            return "ß"; // SS
          case MALAY:
            return "ß"; // SS
          case ARABIC:
            return "ß"; // SS
          case ESTONIAN:
            return "ß"; // SS
          case ICELANDIC:
            return "ß"; // SS
          case LATVIAN:
            return "ß"; // SS
          case LITHUANIAN:
            return "ß"; // SS
          case KYRGYZ:
            return "ß"; // SS
          case KAZAKH:
            return "ß"; // SS
          case TAJIK:
            return "ß"; // SS
          case BELARUSIAN:
            return "ß"; // SS
          case TURKMEN:
            return "ß"; // SS
          case AZERBAIJANI:
            return "ß"; // SS
          case ARMENIAN:
            return "ß"; // SS
          case THAI:
            return "ß"; // SS
          case HINDI:
            return "ß"; // SS
          case URDU:
            return "ß"; // SS
          case BENGALI:
            return "ß"; // SS
          case TAMIL:
            return "ß"; // SS
          default: // fall out
        }
        break;
      case 'Ά':
        switch (this) {
          case GREEK:
            return "Α"; // Ά
          default: // fall out
        }
        break;
      case 'Έ':
        switch (this) {
          case GREEK:
            return "Ε"; // Έ
          default: // fall out
        }
        break;
      case 'Ή':
        switch (this) {
          case GREEK:
            return "Η"; // Ή
          default: // fall out
        }
        break;
      case 'Ί':
        switch (this) {
          case GREEK:
            return "Ι"; // Ί
          default: // fall out
        }
        break;
      case 'Ό':
        switch (this) {
          case GREEK:
            return "Ο"; // Ό
          default: // fall out
        }
        break;
      case 'Ύ':
        switch (this) {
          case GREEK:
            return "Υ"; // Ύ
          default: // fall out
        }
        break;
      case 'Ώ':
        switch (this) {
          case GREEK:
            return "Ω"; // Ώ
          default: // fall out
        }
        break;
      case 'ά':
        switch (this) {
          case GREEK:
            return "Α"; // Ά
          default: // fall out
        }
        break;
      case 'έ':
        switch (this) {
          case GREEK:
            return "Ε"; // Έ
          default: // fall out
        }
        break;
      case 'ή':
        switch (this) {
          case GREEK:
            return "Η"; // Ή
          default: // fall out
        }
        break;
      case 'ί':
        switch (this) {
          case GREEK:
            return "Ι"; // Ί
          default: // fall out
        }
        break;
      case 'ό':
        switch (this) {
          case GREEK:
            return "Ο"; // Ό
          default: // fall out
        }
        break;
      case 'ύ':
        switch (this) {
          case GREEK:
            return "Υ"; // Ύ
          default: // fall out
        }
        break;
      case 'ώ':
        switch (this) {
          case GREEK:
            return "Ω"; // Ώ
          default: // fall out
        }
        break;
    }
    throw new IllegalArgumentException(
      "No upper case mapping for char=" + exception + " and this=" + this);
  }

  @SuppressWarnings(value = "EI_EXPOSE_REP", justification = "By design.")
  public final Locale getLocale() {
    return locale;
  }

  public String getSqlFormatString() {
    return sql;
  }

  public String getSql(String expr) {
    return String.format(sql, expr);
  }

  public String toUpperCase(String value) {
    return OracleUpper.toUpperCase(this, value);
  }

  public static OracleUpperTable forLinguisticSort(String sort) {
    return Enum.valueOf(OracleUpperTable.class, sort);
  }
}
