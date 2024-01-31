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

import org.apache.commons.lang3.StringUtils;

/**
 * This utility class was partially copied from Salesforce's internationalization utility library
 * (com.salesforce.i18n:i18n-util:1.0.4), which was released under the 3-clause BSD License.
 * The i18n-util library is not maintained anymore, and it was using vulnerable dependencies.
 * For more info, see: https://issues.apache.org/jira/browse/PHOENIX-6818
 *
 * OracleUpper is used in combination with OracleUpperTable to generate upper-case output
 * consistent particular chosen Oracle expressions.
 *
 * @see OracleUpperTable
 */
public class OracleUpper {

    private OracleUpper() {
        // HideUtilityClassConstructor
    }

    /**
     * Upper-case {@code value}, using the information in {@code t} to produce a result
     * consistent with the PL/SQL expression used to generate t.
     */
    public static String toUpperCase(OracleUpperTable t, String value) {
        // Oracle's upper or nls_upper are known to disagree with Java on some particulars.
        //  We search for known exceptional characters and if found take measures to adjust
        // Java's String.toUpperCase. In the average case we incur just a single relatively
        // fast scan of the string. In typical bad cases we'll incur two extra String copies
        // (one copy into the buffer, one out -- this on top of whatever's required by
        // toUpperCase). Note that we have to match Oracle even for characters outside the
        // language's alphabet since we still want to return records containing those characters.
        char[] exceptions = t.getUpperCaseExceptions();
        if (exceptions.length > 0) {
            // Prefer to use String.indexOf in the case of a single search char; it's faster by
            // virtue of not requiring two loops and being intrinsic.
            int nextExceptionIndex = (exceptions.length == 1)
                    ? value.indexOf(exceptions[0]) : StringUtils.indexOfAny(value, exceptions);

            if (nextExceptionIndex >= 0) {
                // Annoying case: we have found a character that we know Oracle handles differently
                // than Java and we must adjust appropriately.
                StringBuilder result = new StringBuilder(value.length());
                String rem = value;
                do {
                    char nextException = rem.charAt(nextExceptionIndex);

                    result.append(rem.substring(0, nextExceptionIndex).toUpperCase(t.getLocale()));
                    result.append(t.getUpperCaseExceptionMapping(nextException));

                    rem = rem.substring(nextExceptionIndex + 1);
                    nextExceptionIndex = (exceptions.length == 1)
                            ? rem.indexOf(exceptions[0]) : StringUtils.indexOfAny(rem, exceptions);
                } while (nextExceptionIndex >= 0);
                result.append(rem.toUpperCase(t.getLocale()));

                return result.toString();
            }
        }

        // Nice case: we know of no reason that Oracle and Java wouldn't agree when converting
        // to upper case.
        return value.toUpperCase(t.getLocale());
    }
}
