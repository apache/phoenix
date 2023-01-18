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

import java.text.CollationKey;
import java.text.Collator;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.phoenix.util.DeferredStringBuilder;

import com.ibm.icu.impl.jdkadapter.CollatorICU;
import com.ibm.icu.text.AlphabeticIndex;
import com.ibm.icu.util.ULocale;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;


/**
 * This utility class was partially copied from Salesforce's internationalization utility library
 * (com.salesforce.i18n:i18n-util:1.0.4), which was released under the 3-clause BSD License.
 * The i18n-util library is not maintained anymore, and it was using vulnerable dependencies.
 * For more info, see: https://issues.apache.org/jira/browse/PHOENIX-6818
 *
 * Contains all the information about linguistic sorting.
 * The intent of this is to provide the SQL changes to the RDBMS to ensure
 * that the sorting uses the locale provided in Java, and to make sure that
 * the collation in Java will correspond as much as possible to what is in the
 * DB.
 *
 * Rolodex is a feature in alphabetic/syllabary languages to restrict the set
 * of rows in a list to those that start with a certain letter.  In SQL
 * this is usually LIKE 'A%', which will include different letters.
 *
 * To get the list of valid nls_sorts, run this in oracle
 * select value from v$nls_valid_values where parameter='SORT';
 */
public enum LinguisticSort {
    // English:
    //   Using oracle's upper() function to sort; digits come before letters,
    //   '[' is the lowest character after 'Z'.  //  balance-]
    ENGLISH(Locale.ENGLISH, "[", false, false, LinguisticSort.Alphabets.STRING), //  balance-]

    // German:
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    GERMAN(new Locale("de"), LinguisticSort.Alphabets.GERMAN, "0", true, false,
           "nlssort({0}, ''nls_sort=xgerman'')"),

    // French:
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    FRENCH(new Locale("fr"), "0", false, false, "nlssort({0}, ''nls_sort=xfrench'')"),

    // Italian:
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    ITALIAN(new Locale("it"), "0", false, false, "nlssort({0}, ''nls_sort=italian'')"),

    // Spanish:
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    //   Alphabet consists of A-Z plus N-tilde.  However, CH and LL are not considered
    //   letters, so do not use Oracle's xspanish nlssort.
    SPANISH(new Locale("es"), "0", false, false, "nlssort({0}, ''nls_sort=spanish'')"),

    // Catalan:
    //   Using oracle's nlssort() function to sort; digits come before letters,
    //   nothing sorts after the last legal catalan character.
    CATALAN(new Locale("ca"), LinguisticSort.Alphabets.CATALAN, "0", true, false,
            "nlssort({0}, ''nls_sort=catalan'')"),

    // Dutch:
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    DUTCH(new Locale("nl"), "0", false, false, "nlssort({0}, ''nls_sort=dutch'')"),

    // Portuguese:
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    PORTUGUESE(new Locale("pt"), "0", false, false, "nlssort({0}, ''nls_sort=west_european'')"),

    // Danish:
    //   Alphabet consists of A-Z followed by AE, O-stroke, and A-ring.
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    DANISH(new Locale("da"), "0", false, false, "nlssort({0}, ''nls_sort=danish'')"),

    // Norwegian:
    //   Alphabet consists of A-Z followed by AE, O-stroke, and A-ring.
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    NORWEGIAN(new Locale("no"), "0", false, false,
            "nlssort({0}, ''nls_sort=norwegian'')"),

    // Swedish:
    //   Alphabet consists of A-Z followed by A-ring, A-diaeresis, and O-diaeresis.
    //   Using oracle's nlssort() function to sort; digits come before letters,
    //   nothing sorts after the last legal swedish character.
    SWEDISH(new Locale("sv"), null, false, false,
            "nlssort({0}, ''nls_sort=swedish'')"),

    // Finnish:
    //   Alphabet consists of A-Z, minus W, followed by A-ring, A-diaeresis, and O-diaeresis.
    //   We leave out W so that V's show up properly (bug #151961/W-513969)
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    FINNISH(new Locale("fi"),
            new String[] {
                "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P",
                "Q", "R", "S", "T", "U", "V", "X", "Y", "Z", "\u00C5", "\u00C4", "\u00D6" },
            "0", false, false, "nlssort({0}, ''nls_sort=finnish'')"),

    // Czech:
    //   Alphabet consists of many Czech letters but not all english letters.
    //   Using oracle's nlssort() function to sort; digits come right after letters.
    CZECH(new Locale("cs"), "0", true, false,
            "nlssort({0}, ''nls_sort=xczech'')"),

    // Polish:
    //   Alphabet consists of many Polish letters but not all english letters.
    //   Using oracle's nlssort() function to sort.
    POLISH(new Locale("pl"), "\u00DF", false, false,
            "nlssort({0}, ''nls_sort=polish'')"),

    // Turkish:
    //   Use Turkish alphabet, which also indicates special handling in getUpperCaseValue().
    //   Using oracle's nlssort() function to sort.
    TURKISH(new Locale("tr"), LinguisticSort.Alphabets.TURKISH, null, false, false,
            "nlssort({0}, ''nls_sort=xturkish'')"),

    // Traditional chinese:
    //   Use English alphabet. Using oracle's nlssort() function to sort by stroke.
    CHINESE_HK(new Locale("zh", "HK"), LinguisticSort.Alphabets.ENGLISH, "\u03B1", true, true,
            "nlssort({0}, ''nls_sort=tchinese_radical_m'')"),
    CHINESE_HK_STROKE(new Locale("zh", "HK", "STROKE"), LinguisticSort.Alphabets.ENGLISH, "\u03B1",
            true, true, "nlssort({0}, ''nls_sort=tchinese_stroke_m'')"),

    CHINESE_TW(new Locale("zh", "TW"), LinguisticSort.Alphabets.ENGLISH, "\u03B1", true, true,
            "nlssort({0}, ''nls_sort=tchinese_radical_m'')"),
    CHINESE_TW_STROKE(new Locale("zh", "TW", "STROKE"), LinguisticSort.Alphabets.ENGLISH, "\u03B1",
            true, true, "nlssort({0}, ''nls_sort=tchinese_stroke_m'')"),


    // Simplified chinese:
    //   Use English alphabet. Using oracle's nlssort() function to sort by pinyin.
    CHINESE(new Locale("zh"), LinguisticSort.Alphabets.ENGLISH, "\u03B1", true, true,
            "nlssort({0}, ''nls_sort=schinese_radical_m'')"),
    CHINESE_STROKE(new Locale("zh", "", "STROKE"), LinguisticSort.Alphabets.ENGLISH, "\u03B1",
            true, true,
            "nlssort({0}, ''nls_sort=schinese_stroke_m'')"),
    CHINESE_PINYIN(new Locale("zh", "", "PINYIN"), LinguisticSort.Alphabets.ENGLISH, "\u03B1",
            true, true,
            "nlssort({0}, ''nls_sort=schinese_pinyin_m'')"),


    // Japanese:
    //   Japanese alphabet. Using oracle's nlssort() function to sort. Special rolodex handling
    JAPANESE(new Locale("ja"), LinguisticSort.Alphabets.JAPANESE, null, true, true,
            "nlssort({0}, ''nls_sort=japanese_m'')"),

    // Korean:
    //   Use English alphabet. Using oracle's nlssort() function to sort.
    KOREAN(new Locale("ko"), LinguisticSort.Alphabets.ENGLISH, "\u03B1", true, true,
            "nlssort({0}, ''nls_sort=korean_m'')"),

    // Russian:
    //   Using oracle's nlssort() function to sort.
    RUSSIAN(new Locale("ru"), null, false, false,
            "nlssort({0}, ''nls_sort=russian'')"),

    // Bulgarian:
    //   Using oracle's nlssort() function to sort.
    BULGARIAN(new Locale("bg"), LinguisticSort.Alphabets.BULGARIAN, null, true, false,
            "nlssort({0}, ''nls_sort=bulgarian'')"),

    // Indonesian
    //   Using oracle's nlssort() function to sort.
    INDONESIAN(new Locale("in"), null, true, false, "nlssort({0}, ''nls_sort=indonesian'')"),

    // Romanian:
    //   Using oracle's nlssort() function to sort.
    ROMANIAN(new Locale("ro"),
             new String[] { "A", "\u0102", "\u00c2", "B", "C", "D", "E", "F", "G", "H", "I",
                 "\u00ce", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "\u015e", "T",
                 "\u0162", "U", "V", "W", "X", "Y", "Z" },
            null, true, false, "nlssort({0}, ''nls_sort=romanian'')"),

    // Vietnamese
    //   Using oracle's nlssort() function to sort.
    VIETNAMESE(new Locale("vi"),
               new String[] {
                   "A", "\u0102", "\u00c2", "B", "C", "D", "\u0110", "E", "\u00ca", "G", "H",
                   "I", "K", "L", "M", "N", "O", "\u00d4", "\u01a0", "P", "Q", "R", "S", "T",
                   "U", "\u01af", "V", "X", "Y" },
            null, false, false, "nlssort({0}, ''nls_sort=vietnamese'')"),

    // Ukrainian:
    //   Using oracle's nlssort() function to sort.
    UKRAINIAN(new Locale("uk"), null, false, false, "nlssort({0}, ''nls_sort=ukrainian'')"),

    // Hungarian:
    //   Using oracle's nlssort() function to sort.
    HUNGARIAN(new Locale("hu"), LinguisticSort.Alphabets.HUNGARIAN, null, false, false,
            "nlssort({0}, ''nls_sort=xhungarian'')"),

    // Greek:
    //   Using oracle's nlssort() function to sort.
    GREEK(new Locale("el"), null, false, false, "nlssort({0}, ''nls_sort=greek'')"),

    // Hebrew:
    //   Using oracle's nlssort() function to sort.
    HEBREW(new Locale("iw"), null, true, false, "nlssort({0}, ''nls_sort=hebrew'')"),

    // Slovak:
    //   Using oracle's nlssort() function to sort.
    SLOVAK(new Locale("sk"), LinguisticSort.Alphabets.SLOVAK, null, true, false,
            "nlssort({0}, ''nls_sort=slovak'')"),

    // Serbian (cyrillic):
    //   Using oracle's nlssort() function to sort using it's default
    SERBIAN_CYRILLIC(new Locale("sr"), null, false, false,
            "nlssort({0}, ''nls_sort=generic_m'')"),

    // Serbian (cyrillic):
    //   Using oracle's nlssort() function to sort using it's default
    SERBIAN_LATIN(new Locale("sh"), LinguisticSort.Alphabets.SERBIAN_LATIN, null, false, false,
            "nlssort({0}, ''nls_sort=xcroatian'')"),

    // Serbian (cyrillic):
    //   Using oracle's nlssort() function to sort using it's default
    BOSNIAN(new Locale("bs"), LinguisticSort.Alphabets.SERBIAN_LATIN, null, false, false,
            "nlssort({0}, ''nls_sort=xcroatian'')"),


    // Georgian:
    //   Using oracle's nlssort() function to sort, even though we're using binary for this.
    GEORGIAN(new Locale("ka"), LinguisticSort.Alphabets.GEORGIAN, null, false, false,
            "nlssort({0}, ''nls_sort=binary'')"),

    // BASQUE:
    //   Using oracle's nlssort() function to sort.
    BASQUE(new Locale("eu"),  LinguisticSort.Alphabets.BASQUE, null, false, false,
            "nlssort({0}, ''nls_sort=west_european'')"),

    // MALTESE:
    //   Using oracle's nlssort() function to sort.
    MALTESE(new Locale("mt"), null, false, false, "nlssort({0}, ''nls_sort=west_european'')"),

    // ROMANSH:
    //   Using oracle's nlssort() function to sort.
    ROMANSH(new Locale("rm"), null, false, false, "nlssort({0}, ''nls_sort=west_european'')"),

    // LUXEMBOURGISH:
    //   Using oracle's nlssort() function to sort.
    LUXEMBOURGISH(new Locale("lb"), LinguisticSort.Alphabets.LUXEMBOURGISH, null, false, false,
            "nlssort({0}, ''nls_sort=west_european'')"),

    // IRISH:
    //   Using oracle's nlssort() function to sort.
    IRISH(new Locale("ga"),  null, false, false, "nlssort({0}, ''nls_sort=west_european'')"),

    // Slovenian:
    //   Using oracle's nlssort() function to sort.
    SLOVENE(new Locale("sl"), LinguisticSort.Alphabets.SLOVENE, null, false, false,
            "nlssort({0}, ''nls_sort=xslovenian'')"),

    // Croatian:
    //   Using oracle's nlssort() function to sort.
    CROATIAN(new Locale("hr"), LinguisticSort.Alphabets.SERBIAN_LATIN, null, false, false,
            "nlssort({0}, ''nls_sort=xcroatian'')"),

    // Malay
    //   Using oracle's nlssort() function to sort.
    //   We're assuming people are using the english alphabet,
    //   and not the arabic one (Bahasa Melayu)
    MALAY(new Locale("ms"), null, true, false, "nlssort({0}, ''nls_sort=malay'')"),

    // Arabic:
    //   Using oracle's nlssort() function to sort.
    ARABIC(new Locale("ar"), null, false, false, "nlssort({0}, ''nls_sort=arabic'')"),

    // Estonian:
    //   Using oracle's nlssort() function to sort.
    ESTONIAN(new Locale("et"), LinguisticSort.Alphabets.ESTONIAN, null, true, false,
            "nlssort({0}, ''nls_sort=estonian'')"),

    // Icelandic:
    //   Using oracle's nlssort() function to sort.
    ICELANDIC(new Locale("is"), LinguisticSort.Alphabets.ICELANDIC, null, true, false,
            "nlssort({0}, ''nls_sort=icelandic'')"),

    // Latvian:
    //   Using oracle's nlssort() function to sort.
    LATVIAN(new Locale("lv"), LinguisticSort.Alphabets.LATVIAN, null, false, false,
            "nlssort({0}, ''nls_sort=latvian'')"),

    // Lithuanian:
    //   Using oracle's nlssort() function to sort.
    LITHUANIAN(new Locale("lt"), LinguisticSort.Alphabets.LITHUANIAN, null, false, false,
            "nlssort({0}, ''nls_sort=lithuanian'')"),


    // Languages not supported fully.
    KYRGYZ(new Locale("ky"), LinguisticSort.Alphabets.KYRGYZ, null, true, false,
            "nlssort({0}, ''nls_sort=binary'')"),

    KAZAKH(new Locale("kk"), LinguisticSort.Alphabets.KAZAKH, null, true, false,
            "nlssort({0}, ''nls_sort=binary'')"),

    TAJIK(new Locale("tg"), LinguisticSort.Alphabets.TAJIK, null, true, false,
            "nlssort({0}, ''nls_sort=russian'')"),

    BELARUSIAN(new Locale("be"), null, true, false, "nlssort({0}, ''nls_sort=russian'')"),

    TURKMEN(new Locale("tk"), LinguisticSort.Alphabets.TURKISH, null, false, false,
            "nlssort({0}, ''nls_sort=xturkish'')"),

    AZERBAIJANI(new Locale("az"), LinguisticSort.Alphabets.AZERBAIJANI, null, false, false,
            "nlssort({0}, ''nls_sort=xturkish'')"),

    ARMENIAN(new Locale("hy"), null, true, false, "nlssort({0}, ''nls_sort=binary'')"),

    THAI(new Locale("th"), null, true, false, "nlssort({0}, ''nls_sort=thai_dictionary'')"),

    // Binary?  really
    HINDI(new Locale("hi"), null, true, false, "nlssort({0}, ''nls_sort=binary'')"),

    URDU(new Locale("ur"), LinguisticSort.Alphabets.URDU, null, false, false,
            "nlssort({0}, ''nls_sort=arabic'')"),

    // Bengali
    BENGALI(new Locale("bn"), LinguisticSort.Alphabets.BENGALI, null, true, false,
            "nlssort({0}, ''nls_sort=bengali'')"),

    TAMIL(new Locale("ta"), LinguisticSort.Alphabets.TAMIL, null, true, false,
            "nlssort({0}, ''nls_sort=binary'')"),

    // Unused language for testing; Alphabet and sorting defaults to English
    ESPERANTO(new Locale("eo"), LinguisticSort.Alphabets.ENGLISH, "[", false, false,
            LinguisticSort.Alphabets.STRING);

    private static final Map<Locale, LinguisticSort> BY_LOCALE = getByLocaleInfo();

    /**
     * Create the map that will be stuffed into BY_LOCALE.  We have to fully create an object
     * THEN stuff into a final field in a constructor (as unmodifiableMap does below) in order
     * to get a proper guarantee from Java's memory model.
     *
     * See http://jeremymanson.blogspot.com/2008/07/immutability-in-java-part-2.html
     */
    private static Map<Locale, LinguisticSort> getByLocaleInfo() {
        final Map<Locale, LinguisticSort> byLocaleInfo = new HashMap<Locale, LinguisticSort>(64);
        for (LinguisticSort sort : values()) {
            LinguisticSort duplicated = byLocaleInfo.put(sort.getLocale(), sort);
            assert duplicated == null : "Two linguistic sorts with the same locale: "
                    + sort.getLocale();
        }
        return Collections.unmodifiableMap(byLocaleInfo);
    }

    /**
     * Get sorting info for the given locale.
     */
    public static LinguisticSort get(Locale locale) {
        // For non-UTF8 dbs, we always interpret everything as English.  (We did not set
        // the page encoding to UTF-8, and thus we may have incorrectly encoded data.)
        // On all other instances, look for the language of the user's locale.  This should
        // succeed because every language we support are listed in data.  But just in case,
        // default to english also.
        if (IS_MULTI_LINGUAL /*|| TestContext.isRunningTests()*/) {
            LinguisticSort sort = BY_LOCALE.get(locale);
            if (sort != null) {
                return sort;
            }
            if (locale.getVariant().length() > 0) {
                if ("zh".equals(locale.getLanguage())) {
                    // TW and HK are handled above, this handles SG
                    if (!"".equals(locale.getLanguage())) {
                        // This means it's standard.
                        return get(new Locale(locale.getLanguage(), "", locale.getVariant()));
                    }
                }
                return get(new Locale(locale.getLanguage(), locale.getLanguage()));
            }
            if (locale.getCountry().length() > 0) {
                sort = BY_LOCALE.get(new Locale(locale.getLanguage()));
                if (sort != null) {
                    return sort;
                }
            }
        }
        return ENGLISH;
    }

    /**
     * The locale for this LinguisticSort instance.
     */
    private final Locale locale;

    /**
     * Collator for this LinguisticSort instance.  This may be different than the
     * default collator for its locale.  This is to better match Oracle's nls sort
     * ordering.
     */
    private final Collator collator;

    /**
     * Array of letters (Strings) to show in the rolodex.  An empty array for
     * alphabet means that the rolodex is not supported for the locale.
     */
    private final String[] alphabet;

    /**
     * An optional String that sorts higher than all letters in the alphabet.
     * Used when the generating rolodex sql for the last letter.
     */
    private final String highValue;

    /**
     * True normal secondary sorting is reversed, ie, if lower case letters
     * are sorted before upper case.
     */
    private final boolean reverseSecondary;

    /**
     * True if the locale has double width alphabet, number or symbols,
     * So we use Oracle's to_single_byte to convert into the half width letter.
     */
    private final boolean hasDoubleWidth;

    /**
     * A MessageFormat pattern for generating an oracle sql expression returning the
     * collation key for sorting a sql expression.  Not used by postgres.
     */
    private final String collationKeySql;

    /**
     * For upper-casing Java values and generating SQL to generate the same. Not used by postgres.
     */
    private final OracleUpperTable upper;

    /**
     * Constructor only used when building static data, where ICU should be used to derive the
     * value for the alphabet
     */
    LinguisticSort(Locale locale, String highValue, boolean reverseSecondary,
                   boolean hasDoubleWidth, String collationKeySql) {
        this(locale, getAlphabetFromICU(locale), highValue, reverseSecondary,
             hasDoubleWidth, collationKeySql);
    }

    /**
     * Mapping for locales and ULocale language tags to use for constructing an ICU4J collator.
     * javac complains if we attempt to refer to a static defined inside the same class as an enum,
     * so we need to use an inner class to have such a constant mapping.
     */
    private static final class Icu4jCollatorOverrides {
        static final Map<Locale, String> OVERRIDES = getIcu4jCollatorOverrides();

        /**
         * ICU4J collator overrides
         */
        private static Map<Locale, String> getIcu4jCollatorOverrides() {
            // Map between a Locale and a BCP47 language tag to use when calling ICU4J's
            // Collator.getInstance(ULocale.forLanguageTag()).
            Map<Locale, String> overrides = new HashMap<Locale, String>(7);

            // Built-in JDK collators for Chinese are behind the Unicode standard, so we need to
            // override them. See discussion at
            // https://stackoverflow.com/questions/33672422
            //   /wrong-sorting-with-collator-using-locale-simplified-chinese
            // Also see the following JDK collator bugs:
            // https://bugs.openjdk.java.net/browse/JDK-6415666
            // https://bugs.openjdk.java.net/browse/JDK-2143916
            // https://bugs.openjdk.java.net/browse/JDK-6411864

            // CHINESE_HK:
            overrides.put(new Locale("zh", "HK"), "zh-HK-u-co-unihan");
            // CHINESE_HK_STROKE:
            overrides.put(new Locale("zh", "HK", "STROKE"), "zh-HK-u-co-stroke");
            // CHINESE_TW:
            overrides.put(new Locale("zh", "TW"), "zh-TW-u-co-unihan");
            // CHINESE_TW_STROKE:
            overrides.put(new Locale("zh", "TW", "STROKE"), "zh-TW-u-co-stroke");
            // CHINESE:
            overrides.put(new Locale("zh"), "zh-CN-u-co-unihan");
            // CHINESE_STROKE:
            overrides.put(new Locale("zh", "", "STROKE"), "zh-CN-u-co-stroke");
            // CHINESE_PINYIN:
            overrides.put(new Locale("zh", "", "PINYIN"), "zh-CN-u-co-pinyin");

            return Collections.unmodifiableMap(overrides);
        }
    }

    /**
     * Constructor only used when building static data
     */
    LinguisticSort(Locale locale, String[] alphabet, String highValue, boolean reverseSecondary,
                   boolean hasDoubleWidth, String collationKeySql) {
        this.locale = locale;
        this.alphabet = alphabet;
        this.highValue = highValue;
        assert this.highValue == null || this.highValue.length() == 1;
        this.reverseSecondary = reverseSecondary;
        this.hasDoubleWidth = hasDoubleWidth;
        this.collationKeySql = collationKeySql;
        // Construct collator for this locale
        if (LinguisticSort.Icu4jCollatorOverrides.OVERRIDES.containsKey(this.locale)) {
            // Force ICU4J collators for specific locales so they match Oracle sort
            this.collator = CollatorICU.wrap(com.ibm.icu.text.Collator.getInstance(
                    ULocale.forLanguageTag(LinguisticSort
                            .Icu4jCollatorOverrides.OVERRIDES.get(this.locale))));
        } else if (this.locale.getVariant().length() > 0) {
            // If there's a variant, use ICU4J to figure it out.
            this.collator = CollatorICU.wrap(com.ibm.icu.text.Collator.getInstance(
                    ULocale.forLocale(this.locale)));
        } else {
            this.collator = Collator.getInstance(this.locale);
        }
        this.collator.setStrength(Collator.SECONDARY);
        this.upper = OracleUpperTable.forLinguisticSort(name());
    }

    /**
     * @return a new collator for this LinguisticSort instance.
     */
    public Collator getCollator() {
        // Since RuleBasedCollator.compare() is synchronized, it is not nice to return
        // this.collator here, because that would mean requests for the same language
        // will be waiting for each other.  Instead, return a clone.  And, cloning
        // RuleBasedCollator instances is much more efficient than creating one from
        // the rules.
        return (Collator) this.collator.clone();
    }

    /**
     * @return a new collator for this LinguisticSort instance that is guaranteed to be
     * case-insensitive. Danish collation, unfortunately, is a little odd, in that "v"
     * and "w" are considered to be the same character. To make up for this, they made
     * "v" and "V" a secondary difference, which makes Enum comparisons in FilterItem
     * a little wonky.  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4174436
     */
    public Collator getGuaranteedCaseInsensitiveCollator() {
        Collator collator = getCollator();
        if ("da".equals(this.locale.getLanguage())) {
            collator.setStrength(Collator.PRIMARY);
        }
        return collator;
    }

    Locale getLocale() {
        return this.locale;
    }

    /**
     * @return a new comparator for strings for this LinguisticSort instance.
     */
    @SuppressWarnings("unchecked")
    // Converting from Comparator<Object> to Comparator<String>
    public Comparator<String> getNonCachingComparator() {
        return (Comparator<String>) this.collator.clone();
    }

    /**
     * @return a new comparator for strings for this LinguisticSort instance.
     * @param size the number of elements to compare (default is 16).
     */
    public Comparator<String> getComparator(int size) {
        return new LinguisticSort.CollatingComparator(getCollator(), size);
    }

    /**
     * A String comparator that uses the current collation
     */
    static class CollatingComparator implements Comparator<String> {
        private final Collator collator;
        private final Map<String, CollationKey> cKeyMap;

        CollatingComparator(Collator collator) {
            this(collator, 16);
        }

        CollatingComparator(Collator collator, int defaultSize) {
            this.collator = collator;
            cKeyMap = new HashMap<>(defaultSize);
        }

        @SuppressWarnings(
                value = "ES_COMPARING_PARAMETER_STRING_WITH_EQ",
                justification = "Reference comparison used for performance improvement.")
        public int compare(String o1, String o2) {
            if (o1 == o2) {
                return 0;
            } else if (o2 == null) {
                return 1;
            } else if (o1 == null) {
                return -1;
            }

            return getCollationKey(o1).compareTo(getCollationKey(o2));
        }

        private CollationKey getCollationKey(String comp) {
            CollationKey key = cKeyMap.get(comp);
            if (key == null) {
                key = collator.getCollationKey(comp);
                cKeyMap.put(comp, key);
            }
            return key;
        }
    }

    /**
     * Returns the number of letters to show in the rolodex.
     */
    public int getAlphabetLength() {
        return this.alphabet.length;
    }

    /**
     * Returns the n-th of letter in the rolodex.  Note, a 'letter'
     * in a language be composed of more than one unicode characters,
     * for example, letter 'ch' in Czech.
     */
    public String getAlphabet(int index) {
        return this.alphabet[index];
    }

    // Used only for test code
    String[] getAlphabet() {
        return this.alphabet;
    }

    /**
     * Return the rolodexIndex for a string.
     *
     * @param searchTerm  Must be a 1-char string
     * @return the rolodexIndex, including Other (i.e. getAlphabetLength) if it doesn't
     *         fall into a bucket. If this language doesn't have a rolodex (e.g. Arabic,
     *         Latvian, etc.) return -1
     * @throws IllegalArgumentException if the string is null or not of length 1
     */
    public int getRolodexIndexForChar(String searchTerm) {
        if (searchTerm == null || searchTerm.length() != 1) {
            throw new IllegalArgumentException("Must be a one-length string");
        }

        if (this.getAlphabetLength() == 0) {
            return -1;
        }

        for (int i = 0; i < this.getAlphabetLength(); i++) {
            int comparison = this.collator.compare(searchTerm, this.getAlphabet(i));

            if (comparison < 0) {
                //If it's less than 'a', return Other
                //Otherwise, it's less than the current index, but it wasn't 0 on the
                // previous comparison, so return the previous rolodex letter.
                return i == 0 ? this.getAlphabetLength() : (i - 1);
            } else if (comparison == 0) {
                return i;
            }
        }
        return this.getAlphabetLength();
    }

    /**
     * Returns the sql expression to convert the given sql expression to upper case.
     */
    public String getUpperCaseSql(String expr, boolean isPostgres) {
        if (isPostgres) {
            return "icu_upper(" + expr + ",'" + this.locale.toString() + "')";
        } else {
            return upper.getSql(expr);
        }
    }

    /**
     * @return true if sql UPPER() is used in getUpperCaseSql().  Note that this is always false
     *         for postgres because postgres always use the icu_upper() function for all languages.
     */
    public boolean usesUpperToGetUpperCase(boolean isPostgres) {
        return !isPostgres && "upper(x)".equals(upper.getSql("x"));
    }

    /**
     * Returns the upper case value of the given value, or what would be the result
     * of applying the sql expression in getUpperCaseSql() to the given value.
     */
    public String getUpperCaseValue(String value, boolean isPostgres) {
        String singleWidth = value;
        if (this.hasDoubleWidth) {
            singleWidth = toSingleWidth(value);
        }
        if (isPostgres) {
            return singleWidth.toUpperCase(this.locale);
        } else {
            return upper.toUpperCase(singleWidth);
        }
    }

    private static final char[][] DOUBLE_TO_SINGLE = new char[256][];
    static {
        DOUBLE_TO_SINGLE[0x20] = new char[256];
        DOUBLE_TO_SINGLE[0x20][0x18] = '`';
        DOUBLE_TO_SINGLE[0x20][0x19] = '\'';
        DOUBLE_TO_SINGLE[0x20][0x1D] = '"';

        DOUBLE_TO_SINGLE[0x22] = new char[256];
        DOUBLE_TO_SINGLE[0x22][0x3C] = '~';

        DOUBLE_TO_SINGLE[0x30] = new char[256];
        DOUBLE_TO_SINGLE[0x30][0x00] = ' ';

        DOUBLE_TO_SINGLE[0xFE] = new char[256];
        DOUBLE_TO_SINGLE[0xFE][0x3F] = '^';

        DOUBLE_TO_SINGLE[0xFF] = new char[256];
        DOUBLE_TO_SINGLE[0xFF][0x01] = '!';
        DOUBLE_TO_SINGLE[0xFF][0x03] = '#';
        DOUBLE_TO_SINGLE[0xFF][0x04] = '$';
        DOUBLE_TO_SINGLE[0xFF][0x05] = '%';
        DOUBLE_TO_SINGLE[0xFF][0x06] = '&';
        DOUBLE_TO_SINGLE[0xFF][0x08] = '(';
        DOUBLE_TO_SINGLE[0xFF][0x09] = ')';
        DOUBLE_TO_SINGLE[0xFF][0x0A] = '*';
        DOUBLE_TO_SINGLE[0xFF][0x0B] = '+';
        DOUBLE_TO_SINGLE[0xFF][0x0C] = ',';
        DOUBLE_TO_SINGLE[0xFF][0x0D] = '-';
        DOUBLE_TO_SINGLE[0xFF][0x0E] = '.';
        DOUBLE_TO_SINGLE[0xFF][0x0F] = '/';
        DOUBLE_TO_SINGLE[0xFF][0x10] = '0';
        DOUBLE_TO_SINGLE[0xFF][0x11] = '1';
        DOUBLE_TO_SINGLE[0xFF][0x12] = '2';
        DOUBLE_TO_SINGLE[0xFF][0x13] = '3';
        DOUBLE_TO_SINGLE[0xFF][0x14] = '4';
        DOUBLE_TO_SINGLE[0xFF][0x15] = '5';
        DOUBLE_TO_SINGLE[0xFF][0x16] = '6';
        DOUBLE_TO_SINGLE[0xFF][0x17] = '7';
        DOUBLE_TO_SINGLE[0xFF][0x18] = '8';
        DOUBLE_TO_SINGLE[0xFF][0x19] = '9';
        DOUBLE_TO_SINGLE[0xFF][0x1A] = ':';
        DOUBLE_TO_SINGLE[0xFF][0x1B] = ';';
        DOUBLE_TO_SINGLE[0xFF][0x1C] = '<';
        DOUBLE_TO_SINGLE[0xFF][0x1D] = '=';
        DOUBLE_TO_SINGLE[0xFF][0x1E] = '>';
        DOUBLE_TO_SINGLE[0xFF][0x1F] = '?';
        DOUBLE_TO_SINGLE[0xFF][0x20] = '@';
        DOUBLE_TO_SINGLE[0xFF][0x21] = 'A';
        DOUBLE_TO_SINGLE[0xFF][0x22] = 'B';
        DOUBLE_TO_SINGLE[0xFF][0x23] = 'C';
        DOUBLE_TO_SINGLE[0xFF][0x24] = 'D';
        DOUBLE_TO_SINGLE[0xFF][0x25] = 'E';
        DOUBLE_TO_SINGLE[0xFF][0x26] = 'F';
        DOUBLE_TO_SINGLE[0xFF][0x27] = 'G';
        DOUBLE_TO_SINGLE[0xFF][0x28] = 'H';
        DOUBLE_TO_SINGLE[0xFF][0x29] = 'I';
        DOUBLE_TO_SINGLE[0xFF][0x2A] = 'J';
        DOUBLE_TO_SINGLE[0xFF][0x2B] = 'K';
        DOUBLE_TO_SINGLE[0xFF][0x2C] = 'L';
        DOUBLE_TO_SINGLE[0xFF][0x2D] = 'M';
        DOUBLE_TO_SINGLE[0xFF][0x2E] = 'N';
        DOUBLE_TO_SINGLE[0xFF][0x2F] = 'O';
        DOUBLE_TO_SINGLE[0xFF][0x30] = 'P';
        DOUBLE_TO_SINGLE[0xFF][0x31] = 'Q';
        DOUBLE_TO_SINGLE[0xFF][0x32] = 'R';
        DOUBLE_TO_SINGLE[0xFF][0x33] = 'S';
        DOUBLE_TO_SINGLE[0xFF][0x34] = 'T';
        DOUBLE_TO_SINGLE[0xFF][0x35] = 'U';
        DOUBLE_TO_SINGLE[0xFF][0x36] = 'V';
        DOUBLE_TO_SINGLE[0xFF][0x37] = 'W';
        DOUBLE_TO_SINGLE[0xFF][0x38] = 'X';
        DOUBLE_TO_SINGLE[0xFF][0x39] = 'Y';
        DOUBLE_TO_SINGLE[0xFF][0x3A] = 'Z';
        DOUBLE_TO_SINGLE[0xFF][0x3B] = '[';
        DOUBLE_TO_SINGLE[0xFF][0x3C] = '\\';
        DOUBLE_TO_SINGLE[0xFF][0x3D] = ']';
        DOUBLE_TO_SINGLE[0xFF][0x3F] = '_';
        DOUBLE_TO_SINGLE[0xFF][0x41] = 'a';
        DOUBLE_TO_SINGLE[0xFF][0x42] = 'b';
        DOUBLE_TO_SINGLE[0xFF][0x43] = 'c';
        DOUBLE_TO_SINGLE[0xFF][0x44] = 'd';
        DOUBLE_TO_SINGLE[0xFF][0x45] = 'e';
        DOUBLE_TO_SINGLE[0xFF][0x46] = 'f';
        DOUBLE_TO_SINGLE[0xFF][0x47] = 'g';
        DOUBLE_TO_SINGLE[0xFF][0x48] = 'h';
        DOUBLE_TO_SINGLE[0xFF][0x49] = 'i';
        DOUBLE_TO_SINGLE[0xFF][0x4A] = 'j';
        DOUBLE_TO_SINGLE[0xFF][0x4B] = 'k';
        DOUBLE_TO_SINGLE[0xFF][0x4C] = 'l';
        DOUBLE_TO_SINGLE[0xFF][0x4D] = 'm';
        DOUBLE_TO_SINGLE[0xFF][0x4E] = 'n';
        DOUBLE_TO_SINGLE[0xFF][0x4F] = 'o';
        DOUBLE_TO_SINGLE[0xFF][0x50] = 'p';
        DOUBLE_TO_SINGLE[0xFF][0x51] = 'q';
        DOUBLE_TO_SINGLE[0xFF][0x52] = 'r';
        DOUBLE_TO_SINGLE[0xFF][0x53] = 's';
        DOUBLE_TO_SINGLE[0xFF][0x54] = 't';
        DOUBLE_TO_SINGLE[0xFF][0x55] = 'u';
        DOUBLE_TO_SINGLE[0xFF][0x56] = 'v';
        DOUBLE_TO_SINGLE[0xFF][0x57] = 'w';
        DOUBLE_TO_SINGLE[0xFF][0x58] = 'x';
        DOUBLE_TO_SINGLE[0xFF][0x59] = 'y';
        DOUBLE_TO_SINGLE[0xFF][0x5A] = 'z';
        DOUBLE_TO_SINGLE[0xFF][0x5B] = '{';
        DOUBLE_TO_SINGLE[0xFF][0x5C] = '|';
        DOUBLE_TO_SINGLE[0xFF][0x5D] = '}';
    }

    public static char toSingleWidth(char c) {
        // Mask off high 2 bytes and index into char[][]
        char[] cBucket = DOUBLE_TO_SINGLE[c >> 8];
        // If no bucket, then no translation so just use original char
        if (cBucket == null) {
            return c;
        }
        // Mask off low 2 bytes and index into char[]
        char cSingle = cBucket[c & 0x00ff];
        // If char at that index is zero, then no translation so just use original char
        if (cSingle == 0) {
            return c;
        }
        return cSingle;
    }

    /**
     * Convert double width ascii characters to single width.
     * This is the equivalent of Oracle's to_single_byte().
     */
    public static String toSingleWidth(String value) {
        int n = value.length();
        DeferredStringBuilder buf = new DeferredStringBuilder(value);

        for (int i = 0; i < n; i++) {
            char c = value.charAt(i);
            buf.append(toSingleWidth(c));
        }
        return buf.toString();
    }

    /**
     * Returns the sql expression to compute the linguistic sort collation key for the
     * given sql expression.  This supports sorting in the database, where sort order
     * of different upper and lower cases are handled linguistically.
     */
    public String getCollationKeySql(String expr, boolean isPostgres) {
        if (isPostgres) {
            return "icu_sortkey(" + expr + ",'" + this.locale.toString() + "')::text";
        } else {
            return MessageFormat.format(this.collationKeySql, new Object[] { expr });
        }
    }

    /**
     * Returns the sql expression to compute the linguistic sort collation key for the
     * upper case of given sql expression.  This supports case-insensitive filtering
     * in the database.
     */
    public String getUpperCollationKeySql(String expr, boolean isPostgres) {
        if (!isPostgres && String.format(upper.getSqlFormatString(), "{0}")
                .equals(this.collationKeySql)) {
            return getCollationKeySql(expr, false);
        }
        return getCollationKeySql(getUpperCaseSql(expr, isPostgres), isPostgres);
    }

    private String formatLetter(String letter, boolean isPostgres) {
        return getCollationKeySql('\'' + letter + '\'', isPostgres);
    }

    //
    // Private Data
    //

    // TODO: Make this an environment variable.
    private static final boolean IS_MULTI_LINGUAL = true; /*(SfdcEnvProvider.getEnv() == null ||
            SfdcEnvProvider.getEnv().getIniFile().getString("Pages", "encoding").length() > 0);*/

    static String[] getAlphabetFromICU(Locale locale) {
        AlphabeticIndex<?> index = new AlphabeticIndex<String>(locale);
        List<String> alphabet = index.getBucketLabels();
        if (alphabet.size() > 6) {
            // Strip off first and last (which are ...)
            List<String> alphabetWithoutEllipses = alphabet.subList(1, alphabet.size() - 1);
            return alphabetWithoutEllipses.toArray(new String[alphabetWithoutEllipses.size()]);
        } else {
            return new String[0];
        }
    }

    /**
     * You can't refer to a static defined inside the same class as an enum, so you need an
     * inner class to have such constants
     * These are the alphabets that cannot be auto-derived from ICU's CLDR information
     */
    static final class Alphabets {
        static final String[] ENGLISH = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
            "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
        static final String[] CATALAN = { "A", "B", "C", "\u00C7", "D", "E", "F", "G", "H", "I",
            "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
        static final String[] BASQUE = { "A", "B", "C", "\u00C7", "D", "E", "F", "G", "H", "I",
            "J", "K", "L", "M", "N", "\u00D1", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X",
            "Y", "Z" };
        static final String[] JAPANESE = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
            "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "\u30A2",
            "\u30AB", "\u30B5", "\u30BF", "\u30CA", "\u30CF", "\u30DE", "\u30E4", "\u30E9",
            "\u30EF" };

        // A, B, C, Cs, D, E, F, G, Gy, H, I, J, K, L, Ly, M, N, Ny, O, Ö, P, Q, R, S, Sz, T,
        // Ty, U, Ü, V, W, X, Y, Z, Zs
        static final String[] HUNGARIAN = { "A", "B", "C", "Cs", "D", "E", "F", "G", "Gy", "H",
            "I", "J", "K", "L", "Ly", "M", "N", "Ny", "O", "\u00d6", "P", "Q", "R", "S", "Sz",
            "T", "Ty", "U", "\u00dc", "V", "W", "X", "Y", "Z", "Zs" };

        static final String[] TURKISH = { "A", "B", "C", "\u00C7", "D", "E", "F", "G", "\u011E",
            "H", "I", "\u0130", "J", "K", "L", "M", "N", "O", "\u00D6", "P", "R", "S", "\u015E",
            "T", "U", "\u00DC", "V", "Y", "Z" };

        // A, B, C, Ç, D, E, Ə, F, G, Ğ, H, X, I, İ, J, K, Q, L, M, N, O, Ö, P, R, S, Ş, T,
        // U, Ü, V, Y, Z
        static final String[] AZERBAIJANI = { "A", "B", "C", "\u00C7", "D", "E", "\u018F", "F",
            "G", "\u011E", "H", "X", "I", "\u0130", "J", "K", "Q", "L", "M", "N", "O", "\u00D6",
            "P", "R", "S", "\u015E", "T", "U", "\u00DC", "V", "Y", "Z" };

        // Russian without  Ё, Ы, Э
        static final String[] BULGARIAN = { "\u0410", "\u0411", "\u0412", "\u0413", "\u0414",
            "\u0415", "\u0416", "\u0417", "\u0418", "\u0419", "\u041a", "\u041b", "\u041c",
            "\u041d", "\u041e", "\u041f", "\u0420", "\u0421", "\u0422", "\u0423", "\u0424",
            "\u0425", "\u0426", "\u0427", "\u0428", "\u0429", "\u042a", "\u042c", "\u042e",
            "\u042f" };

        // A B C Č Ć D Đ Dž E F G H I J K L Lj M N Nj O P R S Š T U V Z Ž
        static final String[] SERBIAN_LATIN = { "A", "B", "C", "\u010c", "\u0106", "D", "\u0110",
            "D\u017e", "E", "F", "G", "H", "I", "J", "K", "L", "Lj", "M", "N", "Nj", "O", "P", "R",
            "S", "\u0160", "T", "U", "V", "Z", "\u017d" };

        // A Á Ä B C Č D Ď DZ DŽ E É F G H CH I Í J K L Ĺ Ľ M N Ň O Ó Ô P Q R Ŕ S Š T Ť U Ú V W
        // X Y Ý Z Ž
        static final String[] SLOVAK = { "A", "\u00c1", "\u00c4", "B", "C", "\u010c", "D",
            "\u010e", "DZ", "D\u017d", "E", "\u00c9", "F", "G", "H", "CH", "I", "\u00cd", "J",
            "K", "L", "\u0139", "\u013d", "M", "N", "\u0147", "O", "\u00d3", "\u00d4", "P", "Q",
            "R", "\u0154", "S", "\u0160", "T", "\u0164", "U", "\u00da", "V", "W", "X", "Y",
            "\u00dd", "Z", "\u017d" };

        // ა ბ გ დ ე ვ ზ თ ი კ ლ მ ნ ო პ ჟ რ ს ტ უ ფ ქ ღ .ყ შ ჩ ც ძ წ ჭ ხ ჯ ჰ
        static final String[] GEORGIAN = { "\u10d0", "\u10d1", "\u10d2", "\u10d3", "\u10d4",
            "\u10d5", "\u10d6", "\u10d7", "\u10d8", "\u10d9", "\u10da", "\u10db", "\u10dc",
            "\u10dd", "\u10de", "\u10df", "\u10e0", "\u10e1", "\u10e2", "\u10e3", "\u10e4",
            "\u10e5", "\u10e6", "\u10e7", "\u10e8", "\u10e9", "\u10ea", "\u10eb", "\u10ec",
            "\u10ed", "\u10ee", "\u10ef", "\u10f0" };

        // A B C D E F G H I J K L M N O P Q R S Š Z Ž T U V W Õ Ä Ö Ü X Y
        static final String[] ESTONIAN = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
            "L", "M", "N", "O", "P", "Q", "R", "S", "\u0160", "Z", "\u017d", "T", "U", "V", "W",
            "\u00d5", "\u00c4", "\u00d6", "\u00dc", "X", "Y" };

        // A Á B D Ð E É F G H I Í J K L M N O Ó P R S T U Ú V X Y Ý Þ Æ Ö
        static final String[] ICELANDIC = { "A", "\u00c1", "B", "D", "\u00d0", "E", "\u00c9", "F",
            "G", "H", "I", "\u00cd", "J", "K", "L", "M", "N", "O", "\u00d3", "P", "R", "S", "T",
            "U", "\u00da", "V", "X", "Y", "\u00dd", "\u00de", "\u00c6", "\u00d6" };

        // A Ā B C Č D E Ē F G Ģ H I Ī J K Ķ L Ļ M N Ņ O P R S Š T U Ū V Z Ž
        static final String[] LATVIAN = { "A", "\u0100", "B", "C", "\u010c", "D", "E", "\u0112",
            "F", "G", "\u0122", "H", "I", "\u012a", "J", "K", "\u0136", "L", "\u013b", "M", "N",
            "\u0145", "O", "P", "R", "S", "\u0160", "T", "U", "\u016a", "V", "Z", "\u017d" };

        // A \u0104 B C \u010c D E \u0118 \u0116 F G H I \u012e Y J K L M N O P R S \u0160 T U
        // \u0172 \u016a V Z \u017d
        static final String[] LUXEMBOURGISH = { "A", "B", "C", "D", "E", "F", "G", "H", "I",
            "J", "K", "L", "M", "N", "O", "P", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
            "Ä", "Ë", "É" };

        // Russian with Ң, Ө, Ү
        static final String[] KYRGYZ =  { "\u0410", "\u0411", "\u0412", "\u0413", "\u0414",
            "\u0415", "\u0401", "\u0416", "\u0417", "\u0418", "\u0419", "\u041a", "\u041b",
            "\u041c", "\u041d", "\u04a2", "\u041e", "\u04e8", "\u041f", "\u0420", "\u0421",
            "\u0422", "\u0423", "\u04ae", "\u0424", "\u0425", "\u0426", "\u0427", "\u0428",
            "\u0429", "\u042a", "\u042b", "\u042c", "\u042d", "\u042e", "\u042f" };

        // Kyrgyz with Ә, Ғ, Ұ, Һ, І (ICU4J doesn't have some of these characters for sorting...)
        static final String[] KAZAKH =  { "\u0410", "\u04d8", "\u0411", "\u0412", "\u0413",
            "\u0492", "\u0414", "\u0415", "\u0401", "\u0416", "\u0417", "\u0418", "\u0419",
            "\u041a", "\u049a", "\u041b", "\u041c", "\u041d", "\u04a2", "\u041e", "\u04e8",
            "\u041f", "\u0420", "\u0421", "\u0422", "\u0423", "\u04b0", "\u04ae", "\u0424",
            "\u0425", "\u04ba", "\u0426", "\u0427", "\u0428", "\u0429", "\u042a", "\u042b",
            "\u0406", "\u042c", "\u042d", "\u042e", "\u042f" };

        // Cyrillic Variant
        static final String[] TAJIK = { "\u0410", "\u0411", "\u0412", "\u0413", "\u0492", "\u0414",
            "\u0415", "\u0401", "\u0416", "\u0417", "\u0418", "\u04e2", "\u0419", "\u041a",
            "\u049a", "\u041b", "\u041c", "\u041d", "\u041e", "\u041f", "\u0420", "\u0421",
            "\u0422", "\u0423", "\u04ee", "\u0424", "\u0425", "\u04b2", "\u0427", "\u04b6",
            "\u0428",  "\u042a", "\u042d", "\u042e", "\u042f" };

        // اآبپتٹثجچحخدڈذرڑزژسشصضطظعغفقکگلمنوەھ۶ىے
        static final String[] URDU = new String[] {"\u0627", "\u0622", "\u0628", "\u067e",
            "\u062a", "\u0679", "\u062b", "\u062c", "\u0686", "\u062d", "\u062e", "\u062f",
            "\u0688", "\u0630", "\u0631", "\u0691", "\u0632", "\u0698", "\u0633", "\u0634",
            "\u0635", "\u0636", "\u0637", "\u0638", "\u0639", "\u063a", "\u0641", "\u0642",
            "\u06a9", "\u06af", "\u0644", "\u0645", "\u0646", "\u0648", "\u06d5", "\u06be",
            "\u06f6", "\u0649", "\u06d2" };

        // W-1308726: removed Ö and Ü; oracle treats them as the same characters as O and U.
        // A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, ß, T, U, V, W, X, Y, Z
        static final String[] GERMAN = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K",
            "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };

        // ক,খ,গ,ঘ,ঙ,চ,ছ,জ,ঝ,ঞ,ট,ঠ,ড,ঢ,ণ,ত,দ,ধ,ন,প,ফ,ব,ভ,ম,য,র,ল,শ,ষ,স,হ,য়,ড়,ঢ,অ,
        // আ,ই,ঈ,উ,ঊ,ঋ,ৠ,এ,ঐ,ও,ঔ
        static final String[] BENGALI = { "\u0995", "\u0996", "\u0997", "\u0998", "\u0999",
            "\u099a", "\u099b", "\u099c", "\u099d", "\u099e", "\u099f", "\u09a0", "\u09a1",
            "\u09a2", "\u09a3", "\u09a4", "\u09a6", "\u09a7", "\u09a8", "\u09aa", "\u09ab",
            "\u09ac", "\u09ad", "\u09ae", "\u09af", "\u09b0", "\u09b2", "\u09b6", "\u09b7",
            "\u09b8", "\u09b9", "\u09af\u09bc", "\u09a1\u09bc", "\u09a2", "\u0985", "\u0986",
            "\u0987", "\u0988", "\u0989", "\u098a", "\u098b", "\u09e0", "\u098f", "\u0990",
            "\u0993", "\u0994" };

        // A, Ą, B, C, Č, D, E, Ę, Ė, F, G, H, I, Į, Y, J, K, L, M, N, O, P, R, S, Š, T, U, Ų,
        // Ū, V, Z, Ž
        static final String[] LITHUANIAN = { "A", "\u0104", "B", "C", "\u010c", "D", "E", "\u0118",
            "\u0116", "F", "G", "H", "I", "\u012e", "Y", "J", "K", "L", "M", "N", "O", "P", "R",
            "S", "\u0160", "T", "U", "\u0172", "\u016a", "V", "Z", "\u017d" };

        // A, B, C, Č, D, E, F, G, H, I, J, K, L, M, N, O, P, R, S, Š, T, U, V, Z, Ž
        static final String[] SLOVENE = { "A", "B", "C", "\u010c", "D", "E", "F", "G", "H", "I",
            "J", "K", "L", "M", "N", "O", "P", "R", "S", "\u0160", "T", "U", "V", "Z", "\u017d" };

        // Contains "TAMIL LETTER"s from http://www.unicode.org/charts/PDF/U0B80.pdf
        //அ, ஆ, இ, ஈ, உ, ஊ, எ, ஏ, ஐ, ஒ, ஓ, ஔ, க, ங, ச, ஜ, ஞ,
        //ட, ண, த, ந, ன, ப, ம, ய, ர, ற, ல, ள, ழ, வ, ஶ, ஷ, ஸ, ஹ
        static final String[] TAMIL = { "\u0B85", "\u0B86", "\u0B87", "\u0B88", "\u0B89", "\u0B8A",
            "\u0B8E", "\u0B8F", "\u0B90", "\u0B92", "\u0B93", "\u0B94", "\u0B95", "\u0B99",
            "\u0B9A", "\u0B9C", "\u0B9E", "\u0B9F", "\u0BA3", "\u0BA4", "\u0BA8", "\u0BA9",
            "\u0BAA", "\u0BAE", "\u0BAF", "\u0BB0", "\u0BB1", "\u0BB2", "\u0BB3", "\u0BB4",
            "\u0BB5", "\u0BB6", "\u0BB7", "\u0BB8", "\u0BB9" };

        static final String STRING = "upper({0})";

        static final String[] JAPANESE_ROLODEX = {
            // Notes: unistr('\xxxx') is the Oracle sql expression to get unicode
            // character by code point.
            // Two backslashes are converted to one backslash by java compiler.
            /* 'A'  */"unistr('\\3041')",
            /* 'Ka' */"unistr('\\30F5')",
            /* 'Sa' */"unistr('\\3055')",
            /* 'Ta' */"unistr('\\305F')",
            /* 'Na' */"unistr('\\306A')",
            /* 'Ha' */"unistr('\\306F')",
            /* 'Ma' */"unistr('\\307E')",
            /* 'Ya' */"unistr('\\3084')",
            /* 'Ra' */"unistr('\\3089')",
            /* 'Wa' */"unistr('\\308E')", "unistr('\\309D')" };

        // Notes: unistr('\xxxx') is the Oracle sql expression to get unicode character
        // by code point. Two backslashes are converted to one backslash by java compiler.
        static final String[] JAPANESE_ROLODEX_JAVA = {
            /* 'A'  */"\u3041",
            /* 'Ka' */"\u30F5",
            /* 'Sa' */"\u3055",
            /* 'Ta' */"\u305F",
            /* 'Na" */"\u306A",
            /* 'Ha' */"\u306F",
            /* 'Ma' */"\u307E",
            /* 'Ya' */"\u3084",
            /* 'Ra' */"\u3089",
            /* 'Wa' */"\u308E",
            "\u3001" // this is the first character after the last valid kana in java
        };
    }

    /**
     * Apex and possibly other things collate based on upper case versions of strings.
     * Always upper casing and then comparing is slow, though, so this method is intended
     * to return a collator that is consistent with uppper-case-then-compare while perhaps
     * doing something more efficient
     */
    public Collator getUpperCaseCollator(final boolean isPostgres) {
        final Collator innerCollator = getCollator();

        // so far, the best I've been able to do that doesn't break sort order is to special
        // case the english locale and scan for non-ascii characters before deciding how to
        // proceed. With some work the same basic idea would work in many other locales but
        // it would be very nice to find a more general and faster approach. The challenge
        // is that upper casing effectively "normalizes" strings in a way that is very hard
        // to replicate - for instance, western ligatures tend to get expanded by upper casing
        // but Hangul ones don't. Even when that's all sorted out there's the issue that the
        // built in collation rules for various locales are fairly narrowly focused. So, for
        // instance, the English locale doesn't have rules for sorting Greek. With a case
        // insensitive compare in the English locale, lower case Greek letters sort
        // differently from upper case Greek letters but the English locale does upper case
        // Greek letters.
        if (!isPostgres && getLocale() == Locale.ENGLISH) {
            innerCollator.setStrength(Collator.SECONDARY);
            return new Collator() {
                @Override
                public int compare(String source, String target) {
                    // upper case only strings where the SECONDARY strength comparison
                    // (case insensitive comparison) is possibly different for upper
                    // cased and non upper cased strings
                    return innerCollator.compare(getUpperCaseIfNeeded(source),
                            getUpperCaseIfNeeded(target));
                }

                /**
                 * Upper cases on any non-ascii character
                 */
                private String getUpperCaseIfNeeded(String string) {
                    for (int i = 0; i < string.length(); i++) {
                        final char ch = string.charAt(i);
                        if (ch > 127) {
                            // non-ascii character, bail and use the upper case version
                            return getUpperCaseValue(string, false);
                        }
                    }
                    // no non-ascii characters found, we don't need to upper case
                    // - sorting with strength SECONDARY is equivalent.
                    return string;
                }

                @Override
                public CollationKey getCollationKey(String source) {
                    return innerCollator.getCollationKey(getUpperCaseIfNeeded(source));
                }

                @Override
                public int hashCode() {
                    return LinguisticSort.this.hashCode();
                }

                @Override
                public boolean equals(Object that) {
                    return super.equals(that);
                }
            };
        } else {
            return new Collator() {
                @Override
                public int compare(String source, String target) {
                    return innerCollator.compare(getUpperCaseValue(source, isPostgres),
                            getUpperCaseValue(target, isPostgres));
                }

                @Override
                public CollationKey getCollationKey(String source) {
                    return innerCollator.getCollationKey(getUpperCaseValue(source, isPostgres));
                }

                @Override
                public int hashCode() {
                    return LinguisticSort.this.hashCode();
                }

                @Override
                public boolean equals(Object that) {
                    return super.equals(that);
                }
            };
        }
    }
}
