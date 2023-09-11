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

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.phoenix.thirdparty.com.google.common.base.Splitter;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;

/**
 * This utility class was partially copied from Salesforce's internationalization utility library
 * (com.salesforce.i18n:i18n-util:1.0.4), which was released under the 3-clause BSD License.
 * The i18n-util library is not maintained anymore, and it was using vulnerable dependencies.
 * For more info, see: https://issues.apache.org/jira/browse/PHOENIX-6818
 *
 * A collection of utilities for dealing with Locales.
 */
public enum LocaleUtils {
    INSTANCE;

    public static LocaleUtils get() {
        return INSTANCE;
    }

    // TODO: The number of locales in the system is rather small,
    //  but we should probably use a ConcurrentLruMap just in case.
    private static final ConcurrentMap<Locale, Locale> UNIQUE_LOCALE_MAP =
            new ConcurrentHashMap<>(64, .75f, 2);

    /**
     * Returns a locale for language-only ("en") or language/country ("en_UK")
     * iso codes
     */
    public Locale getLocaleByIsoCode(String isoCode) {
        if (isoCode == null) {
            return null;
        }
        if (isoCode.length() == 2) {
            return uniqueifyLocale(new Locale(isoCode));
        } else if (isoCode.length() == 5) {
            String countryIsoCode = isoCode.substring(3, 5);
            String langIsoCode = isoCode.substring(0, 2);
            return uniqueifyLocale(new Locale(langIsoCode, countryIsoCode));
        } else {
            List<String> split = Lists.newArrayList(Splitter.on('_').split(isoCode));
            String language = split.get(0);
            String country = split.size() > 1 ? split.get(1) : "";
            String variant = split.size() > 2 ? split.get(2) : "";
            return uniqueifyLocale(new Locale(language, country, variant));
        }
    }

    /**
     * If you're going to cache a locale, it should call this function so that it caches
     * @param value the locale to uniquify
     * @return the unique locale
     */
    static Locale uniqueifyLocale(Locale value) {
        if (value == null) {
            return null;
        }
        Locale oldValue = UNIQUE_LOCALE_MAP.get(value);
        if (oldValue != null) {
            return oldValue;
        }
        UNIQUE_LOCALE_MAP.put(value, value);
        return value;
    }
}
