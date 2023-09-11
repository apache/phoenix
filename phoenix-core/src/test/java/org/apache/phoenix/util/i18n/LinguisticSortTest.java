/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.util.i18n;

import static org.apache.phoenix.util.i18n.LinguisticSort.AZERBAIJANI;
import static org.apache.phoenix.util.i18n.LinguisticSort.BASQUE;
import static org.apache.phoenix.util.i18n.LinguisticSort.BENGALI;
import static org.apache.phoenix.util.i18n.LinguisticSort.BOSNIAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.BULGARIAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.CATALAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.CHINESE_HK;
import static org.apache.phoenix.util.i18n.LinguisticSort.CHINESE_HK_STROKE;
import static org.apache.phoenix.util.i18n.LinguisticSort.CHINESE_TW;
import static org.apache.phoenix.util.i18n.LinguisticSort.CHINESE_TW_STROKE;
import static org.apache.phoenix.util.i18n.LinguisticSort.CROATIAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.ESTONIAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.FINNISH;
import static org.apache.phoenix.util.i18n.LinguisticSort.HUNGARIAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.ICELANDIC;
import static org.apache.phoenix.util.i18n.LinguisticSort.JAPANESE;
import static org.apache.phoenix.util.i18n.LinguisticSort.KOREAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.LATVIAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.LITHUANIAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.ROMANIAN;
import static org.apache.phoenix.util.i18n.LinguisticSort.SERBIAN_LATIN;
import static org.apache.phoenix.util.i18n.LinguisticSort.SLOVAK;
import static org.apache.phoenix.util.i18n.LinguisticSort.SLOVENE;
import static org.apache.phoenix.util.i18n.LinguisticSort.TAJIK;
import static org.apache.phoenix.util.i18n.LinguisticSort.TURKISH;
import static org.apache.phoenix.util.i18n.LinguisticSort.TURKMEN;
import static org.apache.phoenix.util.i18n.LinguisticSort.VIETNAMESE;
import static org.apache.phoenix.util.i18n.LinguisticSort.LUXEMBOURGISH;
import static org.apache.phoenix.util.i18n.LinguisticSort.URDU;
import static org.apache.phoenix.util.i18n.LinguisticSort.TAMIL;
import static org.apache.phoenix.util.i18n.LinguisticSort.ESPERANTO;

import com.ibm.icu.text.Normalizer2;

import java.text.CollationKey;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.phoenix.thirdparty.com.google.common.collect.Ordering;

import junit.framework.TestCase;

/**
 * This test class was partially copied from Salesforce's internationalization utility library
 * (com.salesforce.i18n:i18n-util:1.0.4), which was released under the 3-clause BSD License.
 * The i18n-util library is not maintained anymore, and it was using vulnerable dependencies.
 * For more info, see: https://issues.apache.org/jira/browse/PHOENIX-6818
 *
 * This could be expanded significantly.
 */
public class LinguisticSortTest extends TestCase {

    public LinguisticSortTest(String name) {
        super(name);
    }

    public void testThaiBasicSorting() {
        Locale thaiLoc = new Locale("th");

        LinguisticSort thaiSort = LinguisticSort.get(thaiLoc);

        // basic sanity check on thai collator comparisons
        ImmutableList<String> unsorted =
                ImmutableList.of("azw", "Ac", "ab", "21", "zaa", "b\u0e40k", "bk");
        ImmutableList<String> sorted =
                ImmutableList.of("21", "ab", "Ac", "azw", "bk", "b\u0e40k", "zaa");

        assertEquals(sorted,
                Ordering.from(thaiSort.getNonCachingComparator()).sortedCopy(unsorted));
        assertEquals(sorted,
                Ordering.from(thaiSort.getComparator(16)).sortedCopy(unsorted));
    }

    public void testThaiCharactersOfDeath() {
        // This is the original bug report
        Collator c = Collator.getInstance(new Locale("th"));
        String s = "\u0e40";
        // any one of \u0e40,  \u0e41, \u0e42, \u0e43, or \u0e44 will do
        System.out.println(c.compare(s, s));  // In JDK6: runs forever


        // Here's the "real" test
        Locale thaiLoc = new Locale("th");

        LinguisticSort thaiSort = LinguisticSort.get(thaiLoc);
        Collator thaiColl = thaiSort.getCollator();

        String [] oomStrings = {
                "\u0e3f", "\u0e45", "\u0e40k", "\u0e44", "\u0e43", "\u0e42", "\u0e41", "\u0e40"
        };
        String [] srcStrings = oomStrings;
        // Deprecated Patched collator adds space after problematic characters at end of string
        // (because of http://bugs.sun.com/view_bug.do?bug_id=5047314)
        // Otherwise unpatched collator would OOM on these strings
        // String [] srcStrings = {
        //   "\u0e3f", "\u0e45", "\u0e40k", "\u0e44 ", "\u0e43 ", "\u0e42 ", "\u0e41 ", "\u0e40 "
        // };

        for (int i=0; i<oomStrings.length;i++) {
            String oomString = oomStrings[i];
            CollationKey key = thaiColl.getCollationKey(oomString);
            assertEquals("string #"+i, srcStrings[i], key.getSourceString());
        }
    }

    public void testRolodexIndexByChar() throws Exception{
        LinguisticSort englishSort = LinguisticSort.ENGLISH;

        assertEquals(0, englishSort.getRolodexIndexForChar("a"));
        assertEquals(0, englishSort.getRolodexIndexForChar("Á"));
        assertEquals(1, englishSort.getRolodexIndexForChar("b"));
        assertEquals(13, englishSort.getRolodexIndexForChar("N"));
        assertEquals(13, englishSort.getRolodexIndexForChar("Ñ"));
        assertEquals(25, englishSort.getRolodexIndexForChar("z"));
        //А below is the Cyrillic А
        assertOther(Arrays.asList("А", "こ"), englishSort);

        //Spanish
        LinguisticSort spanishSort = LinguisticSort.SPANISH;
        assertEquals(0, spanishSort.getRolodexIndexForChar("a"));
        assertEquals(0, spanishSort.getRolodexIndexForChar("Á"));
        assertEquals(1, spanishSort.getRolodexIndexForChar("b"));
        assertEquals(13, spanishSort.getRolodexIndexForChar("N"));
        assertEquals(14, spanishSort.getRolodexIndexForChar("Ñ"));
        assertEquals(26, spanishSort.getRolodexIndexForChar("z"));
        //А below is the Cyrillic А
        assertOther(Arrays.asList("А", "こ"), spanishSort);

        //Japanese
        LinguisticSort japaneseSort = LinguisticSort.JAPANESE;
        assertEquals(0, japaneseSort.getRolodexIndexForChar("a"));
        assertEquals(0, japaneseSort.getRolodexIndexForChar("Á"));
        assertEquals(1, japaneseSort.getRolodexIndexForChar("b"));
        assertEquals(13, japaneseSort.getRolodexIndexForChar("N"));
        assertEquals(13, japaneseSort.getRolodexIndexForChar("Ñ"));
        assertEquals(25, japaneseSort.getRolodexIndexForChar("z"));
        assertEquals(27, japaneseSort.getRolodexIndexForChar("こ"));
        assertEquals(27, japaneseSort.getRolodexIndexForChar("く"));
        assertEquals(31, japaneseSort.getRolodexIndexForChar("ふ"));
        //А below is the Cyrillic А
        assertOther(Arrays.asList("\u0410"), spanishSort); // А

        //Malay has a rolodex
        LinguisticSort malaySort = LinguisticSort.MALAY;
        assertEquals(0, malaySort.getRolodexIndexForChar("a"));
        assertEquals(25, malaySort.getRolodexIndexForChar("z"));
        assertOther(Arrays.asList("\u0410", "\u304f"), malaySort);  // "А", "く"

        // Thai has a rolodex, all of these should be "other"
        // (Thai has 44 chars, so other is 46)
        LinguisticSort thaiSort = LinguisticSort.THAI;
        assertConstant(Arrays.asList("A", "Á", "b", "\u304f", "\u0410"),
                thaiSort, 46, "had a rolodex index.");

    }

    public void testRolodexComparedToIcu() {
        Set<LinguisticSort> knownDifferences = EnumSet.of(
                CATALAN, FINNISH, TURKISH, CHINESE_HK, CHINESE_HK_STROKE, CHINESE_TW,
                CHINESE_TW_STROKE, JAPANESE, KOREAN, BULGARIAN, ROMANIAN, VIETNAMESE,
                HUNGARIAN, SLOVAK, SERBIAN_LATIN, BOSNIAN, BASQUE, LUXEMBOURGISH, SLOVENE,
                CROATIAN, ESTONIAN, ICELANDIC, LATVIAN, LITHUANIAN, TAJIK, TURKMEN, AZERBAIJANI,
                URDU, BENGALI, TAMIL, ESPERANTO);

        for (LinguisticSort sort : LinguisticSort.values()) {
            if (knownDifferences.contains(sort)) {
                continue;
            }

            String[] alphabet = sort.getAlphabet();
            String[] icuAlphabet = LinguisticSort.getAlphabetFromICU(sort.getLocale());
            String alphaAsString = Arrays.toString(alphabet);
            String icuAlphaAsString = Arrays.toString(icuAlphabet);

            assertEquals("LinguisticSort for " + sort + " doesn't match",
                    icuAlphaAsString, alphaAsString);
            if (!icuAlphaAsString.equals(alphaAsString)) {
                System.out.println(sort + "\n" + icuAlphaAsString + "\n" + alphaAsString);
            } else {
                //System.out.println(sort + ":SAME");
            }
        }
    }

    private void assertOther(Collection<String> chars, LinguisticSort sort){
        assertConstant(chars, sort, sort.getAlphabetLength(), "wasn't in 'Other' category");
    }

    private void assertConstant(Collection<String> chars, LinguisticSort sort,
                                int constant, String message) {
        for (String c : chars){
            assertEquals(c + " " + message, constant, sort.getRolodexIndexForChar(c));
        }
    }

    /**
     * Make sure the upper case collator works equivalently to upper-casing then collating
     */
    public void testUpperCaseCollator() {
        // bump these up for performance testing
        final int repeatTimes = 1;
        final int testSize = 1000;

        testUpperCaseCollator(true, repeatTimes, testSize);
        testUpperCaseCollator(false, repeatTimes, testSize);
    }

    /**
     * Implementation of the testUpperCaseCollator that allows breaking out an ascii only
     * test from a general string test
     */
    private void testUpperCaseCollator(boolean asciiOnly, int repeatTimes, int testSize) {
        final LinguisticSort sort = LinguisticSort.ENGLISH;
        final Collator collator = sort.getCollator();

        final Collator ucCollator = sort.getUpperCaseCollator(false);

        final Random r = new Random();
        final int maxLength = 100;
        for (int iteration = 0; iteration < repeatTimes; iteration++) {
            final boolean lastTime = iteration == repeatTimes - 1;
            final String[] originals = new String[testSize];
            for (int i = 0; i < testSize; i++) {
                switch (i) {
                    case 0:
                        originals[i] = "abß";
                        break;
                    case 1:
                        originals[i] = "abSS";
                        break;
                    case 2:
                        originals[i] = "abß";
                        break;
                    case 3:
                        originals[i] = "ffo";
                        break;
                    case 4:
                        originals[i] = "ﬃ";
                        break;
                    case 5:
                        originals[i] = "FFI";
                        break;
                    case 6:
                        originals[i] = "fred";
                        break;
                    case 7:
                        originals[i] = "FRED";
                        break;
                    case 8:
                        originals[i] = "FREE";
                        break;
                    case 9:
                        originals[i] = "剫";
                        break;
                    case 10:
                        originals[i] = "뻎";
                        break;
                    case 11:
                        originals[i] = "\u1fe3";
                        break;
                    case 12:
                        originals[i] = "\u05d7";
                        break;
                    case 13:
                        originals[i] = "\u1fd3";
                        break;
                    case 14:
                        originals[i] = "\u1441";
                        break;
                    case 15:
                        originals[i] = "\ub9fe";
                        break;
                    case 16:
                        originals[i] = "\u0398";
                        break;
                    case 17:
                        originals[i] = "\u0399";
                        break;
                    case 18:
                        originals[i] = "\u039a";
                        break;
                    case 19:
                        originals[i] = "\u4371";
                        break;
                    case 20:
                        originals[i] = "\ufb06";
                        break;
                    default :
                        originals[i] = randomString(r, maxLength, asciiOnly);
                }
            }

            final int[] upperResults = new int[testSize];
            {
                final long start = System.currentTimeMillis();
                for (int i = 0; i < testSize; i++) {
                    final int next = i + 1 == testSize ? 0 : i + 1;
                    upperResults[i] = collator.compare(sort.getUpperCaseValue(originals[i], false),
                            sort.getUpperCaseValue(originals[next], false));
                }
                if (lastTime) {
                    final long time = System.currentTimeMillis() - start;
                    System.out.println("Compared " + testSize + " " + (asciiOnly ? "ascii " : "") +
                            "strings with upper casing in " + time + "ms");
                }
            }

            final int[] caseResults = new int[testSize];
            {
                final long start = System.currentTimeMillis();
                for (int i = 0; i < testSize; i++) {
                    final int next = i + 1 == testSize ? 0 : i + 1;
                    caseResults[i] = ucCollator.compare(originals[i], originals[next]);
                }
                if (lastTime) {
                    final long time = System.currentTimeMillis() - start;
                    System.out.println("Compared " + testSize + " " + (asciiOnly ? "ascii " : "") +
                            "strings with upper case collator comparison in " + time + "ms");
                }
            }

            final int[] keyResults = new int[testSize];
            {
                final long start = System.currentTimeMillis();
                for (int i = 0; i < testSize; i++) {
                    final int next = i + 1 == testSize ? 0 : i + 1;
                    keyResults[i] = ucCollator.getCollationKey(originals[i])
                            .compareTo(ucCollator.getCollationKey(originals[next]));
                }
                if (lastTime) {
                    final long time = System.currentTimeMillis() - start;
                    System.out.println("Compared " + testSize + " " + (asciiOnly ? "ascii " : "") +
                            "strings with collation keys in " + time + "ms");
                }
            }

            if (lastTime) {
                System.out.println();
            }

            if (lastTime) {
                // normalizing helps see why strings don't compare the same when upper-cased
                final Normalizer2 normalizer = Normalizer2.getNFKDInstance();
                for (int i = 0; i < testSize; i++) {
                    final int next = i + 1 == testSize ? 0 : i + 1;
                    final boolean caseOk = upperResults[i] == caseResults[i];
                    final boolean keyOk = upperResults[i] == keyResults[i];
                    if (!caseOk || !keyOk) {
                        final String message =
                                "Did not get expected result when comparing string " + i + " " +
                                (caseOk ? "" : "using upper case collator comparison ") +
                                (caseOk || keyOk ? "" : "or ") +
                                (keyOk ? ""  : "using collation key comparison ") +
                                "\n" +
                                "'" + escape(originals[i]) + "'\n" +
                                "(" + escape(sort.getUpperCaseValue(originals[i], false)) + ")\n" +
                                "<" + escape(normalizer.normalize(originals[i])) + "> " +
                                "with string " + next + " \n" +
                                "'" + escape(originals[next]) + "'\n" +
                                "(" + escape(sort.getUpperCaseValue(originals[next], false)) +
                                ")\n " +
                                "<" + escape(normalizer.normalize(originals[next])) + ">";
                        assertEquals(message, upperResults[i], caseResults[i]);
                    }
                }
            }
        }
    }

    /**
     * For diagnosis of mismatched strings, dumps a string using standard Java notation
     * for escaping non-printable or non-ascii characters
     */
    private String escape(String string) {
        final StringBuilder sb = new StringBuilder(string.length() * 2);
        int index = 0;
        while (index < string.length()) {
            final int ch = string.codePointAt(index);
            index += Character.charCount(ch);

            escapeCodePoint(sb, ch);
        }
        return sb.toString();
    }

    /**
     * Escapes a single code point so that non-ascii and non-printable characters use
     * their standard Java escape
     */
    private void escapeCodePoint(final StringBuilder sb, final int ch) {
        switch(ch) {
            case '\b' : sb.append("\\b");
                break;
            case '\t' : sb.append("\\t");
                break;
            case '\n' : sb.append("\\n");
                break;
            case '\r' : sb.append("\\r");
                break;
            case '\f' : sb.append("\\f");
                break;
            case '\"' : sb.append("\\\"");
                break;
            case '\\' : sb.append("\\\\");
                break;
            default:
                if (ch < 0x20 || ch > 0x7E) {
                    sb.append(String.format("\\u%04x", ch));
                } else {
                    sb.appendCodePoint(ch);
                }
        }
    }

    /**
     * Generates a random string with between 0 and maxLength characters
     */
    private String randomString(Random r, int maxLength, boolean asciiOnly) {
        final int length = r.nextInt(maxLength);
        return randomFixedLengthString(r, length, asciiOnly);
    }


    /**
     * Generates a random string of the given length
     */
    private String randomFixedLengthString(Random r, int length, boolean asciiOnly) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char c = 0;
            while (!Character.isDefined(c) || Character.isISOControl(c)) {
                c = (char)(asciiOnly ? r.nextInt(128) : r.nextInt());
            }
            sb.append(c);
        }
        return sb.toString();
    }

    public void testUpperCaseExceptionChars() {
        // Sharp s in English
        String[][] enCases = new String[][] {
                // { input, expected output }
                new String[] { "ß", "ß" },
                new String[] { "ßß", "ßß" },
                new String[] { "ßßß", "ßßß" },
                new String[] { "aß", "Aß" },
                new String[] { "aaaß", "AAAß" },
                new String[] { "ßa", "ßA" },
                new String[] { "ßaaa", "ßAAA" },
                new String[] { "aßb", "AßB" },
                new String[] { "aaaßbbb", "AAAßBBB" },
                new String[] { "ßaß", "ßAß" },
                new String[] { "ßaaaß", "ßAAAß" },
                new String[] { "aßbßc", "AßBßC" },
                new String[] { "aaaßbbbßccc", "AAAßBBBßCCC" },
                new String[] { "aßßc", "AßßC" },
                new String[] { "aaaßßccc", "AAAßßCCC" },
        };

        for (String[] c : enCases) {
            assertEquals(c[1], LinguisticSort.ENGLISH.getUpperCaseValue(c[0], false));
        }

        // Omicron in Greek
        String[][] greekCases = new String[][] {
                new String[] { "\u039f", "\u039f" }, // capital omicron
                new String[] { "Ό", "\u039f" }

        };

        for (String[] c : greekCases) {
            assertEquals(c[1], LinguisticSort.GREEK.getUpperCaseValue(c[0], false));
        }
    }

    public void testUsesUpper() {
        assertTrue(LinguisticSort.ENGLISH.usesUpperToGetUpperCase(false));
        assertTrue(LinguisticSort.ESPERANTO.usesUpperToGetUpperCase(false));
        assertTrue(!LinguisticSort.GERMAN.usesUpperToGetUpperCase(false));
    }

    public void testGetUpperCaseCollationKey() {
        assertEquals(LinguisticSort.ENGLISH.getUpperCaseSql("x", false),
                LinguisticSort.ENGLISH.getUpperCollationKeySql("x", false));
    }

    /**
     * I wanted to see the perf impact of doing special-case logic in the EN locale for the German
     * sharp s, &#x00df;. Rename this test (remove the leading _) to run it, e.g. in Eclipse.
     * <p>
     * This method generates two sets of 1000 randomish Strings, one with sharp s and one without.
     * Then it runs 1 million uppercase operations on each bank of strings, using the EN locale
     * (with the special-case logic) and a test locale -- EO, Esperanto -- which does not have
     * any special-case logic.
     * <p>
     * For posterity, when I run this on my machine, I see results like this
     * (averages rounded to nearest 10ms):
     * <p>
     * <table>
     * <tr><td></td><td>ENGLSIH</td><td>ESPERANTO</td><td>GREEK</td></tr>
     * <tr><td>with sharp s</td><td>330ms</td><td>260ms</td><td>370ms</td></tr>
     * <tr><td>without sharp s</td><td>150ms</td><td>130ms</td><td>213ms</td></tr>
     * </table>
     */
    public void _testUpperCasePerf() {
        String[] withSharpS = genStrings(1000, true);
        String[] withoutSharpS = genStrings(1000, false);

        System.out.println("ENGLISH, with ß:");
        runUpperCase(LinguisticSort.ENGLISH, withSharpS);
        System.out.println("ENGLISH, without ß:");
        runUpperCase(LinguisticSort.ENGLISH, withoutSharpS);

        System.out.println("ESPERANTO, with ß:");
        runUpperCase(LinguisticSort.ESPERANTO, withSharpS);
        System.out.println("ESPERANTO, without ß:");
        runUpperCase(LinguisticSort.ESPERANTO, withoutSharpS);

        // Interesting for having a lot of exceptions.
        System.out.println("GREEK, with ß:");
        runUpperCase(LinguisticSort.GREEK, withSharpS);
        System.out.println("GREEK, without ß:");
        runUpperCase(LinguisticSort.GREEK, withoutSharpS);
    }

    private void runUpperCase(LinguisticSort sort, String[] inputs) {
        // Warm up
        for (int i = 0; i < 10000; i++) {
            sort.getUpperCaseValue(inputs[i % inputs.length], false);
        }

        // Run experiment
        for (int i = 0; i < 3; i++) {
            long start = System.currentTimeMillis();
            for (int j = 0; j < 1000000; j++) {
                sort.getUpperCaseValue(inputs[j % inputs.length], false);
            }

            System.out.println("[" + (i + 1) + "] Complete in " +
                    (System.currentTimeMillis() - start) + "ms.");
        }
    }

    /**
     * Return n randomly generated strings, each containing at least
     * one sharp s if useSharpS is true.
     * */
    private String[] genStrings(int n, boolean useSharpS) {
        Random r = new Random();

        String[] inputs = new String[n];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = randomString(r, r.nextInt(12) + 1, r.nextBoolean())
                    + (useSharpS? "ß" : "")
                    + (r.nextBoolean() ?
                    randomString(r, r.nextInt(12) + 1, r.nextBoolean()) + (useSharpS? "ß" : "")
                    : "")
                    + (randomString(r, r.nextInt(12) + 1, r.nextBoolean()));

            if (!useSharpS) assertFalse(inputs[i].contains("ß"));
        }
        return inputs;
    }

    private List<String> cloneAndSort(LinguisticSort sort, List<String> source) {
        List<String> result = new ArrayList<String>(source);
        Collections.sort(result, sort.getCollator());
        return result;
    }

    /**
     * Validate that the sorting of the linguistic sorts for various locales is "correct"
     * The toSort below is in this order.
     * 阿嗄阾啊  : āáǎa
     * 仈㶚 : bā bà
     * 齑: ji
     */
    public void testChineseSorting() {
        final List<String> toSort = ImmutableList.of("\u963f", "\u55c4", "\u963e",
                "\u554a", "\u4ec8", "\u3d9a", "\u9f51");
        assertEquals(ImmutableList.of("\u4ec8", "\u554a", "\u55c4", "\u3d9a", "\u963e",
                "\u963f", "\u9f51"), cloneAndSort(LinguisticSort.CHINESE, toSort));
        assertEquals(ImmutableList.of("\u4ec8", "\u554a", "\u55c4", "\u3d9a", "\u963e",
                "\u963f", "\u9f51"), cloneAndSort(LinguisticSort.CHINESE_HK, toSort));
        assertEquals(ImmutableList.of("\u4ec8", "\u554a", "\u55c4", "\u3d9a", "\u963e",
                "\u963f", "\u9f51"), cloneAndSort(LinguisticSort.CHINESE_TW, toSort));
        assertEquals(ImmutableList.of("\u4ec8", "\u963e", "\u963f", "\u554a", "\u55c4",
                "\u9f51", "\u3d9a"), cloneAndSort(LinguisticSort.CHINESE_STROKE, toSort));
        assertEquals(ImmutableList.of("\u4ec8", "\u963e", "\u963f", "\u554a", "\u55c4",
                "\u9f51", "\u3d9a"), cloneAndSort(LinguisticSort.CHINESE_HK_STROKE, toSort));
        assertEquals(ImmutableList.of("\u4ec8", "\u963e", "\u963f", "\u554a", "\u55c4",
                "\u9f51", "\u3d9a"), cloneAndSort(LinguisticSort.CHINESE_TW_STROKE, toSort));
        assertEquals(ImmutableList.of("\u963f", "\u55c4", "\u554a", "\u4ec8", "\u9f51",
                "\u963e", "\u3d9a"), cloneAndSort(LinguisticSort.CHINESE_PINYIN, toSort));
    }

    public void testChineseLocaleMapping() {
        assertEquals(LinguisticSort.CHINESE,
                LinguisticSort.get(new Locale("zh")));
        assertEquals(LinguisticSort.CHINESE_TW,
                LinguisticSort.get(new Locale("zh","TW")));
        assertEquals(LinguisticSort.CHINESE,
                LinguisticSort.get(new Locale("zh","SG")));
        assertEquals(LinguisticSort.CHINESE_HK,
                LinguisticSort.get(new Locale("zh","HK")));
        assertEquals(LinguisticSort.CHINESE_TW_STROKE,
                LinguisticSort.get(new Locale("zh","TW","STROKE")));
        assertEquals(LinguisticSort.CHINESE_HK_STROKE,
                LinguisticSort.get(new Locale("zh","HK","STROKE")));
        assertEquals(LinguisticSort.CHINESE_STROKE,
                LinguisticSort.get(new Locale("zh","CN","STROKE")));
        assertEquals(LinguisticSort.CHINESE_STROKE,
                LinguisticSort.get(new Locale("zh","SG","STROKE")));
        assertEquals(LinguisticSort.CHINESE_STROKE,
                LinguisticSort.get(new Locale("zh","","STROKE")));
        assertEquals(LinguisticSort.CHINESE_PINYIN,
                LinguisticSort.get(new Locale("zh","CN","PINYIN")));
        assertEquals(LinguisticSort.CHINESE_PINYIN,
                LinguisticSort.get(new Locale("zh","SG","PINYIN")));
        assertEquals(LinguisticSort.CHINESE_PINYIN,
                LinguisticSort.get(new Locale("zh","","PINYIN")));
    }
}
