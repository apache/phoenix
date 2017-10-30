/* 
 * Copyright (c) 2017, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license. 
 * For full license text, see LICENSE.txt file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.force.db.i18n;

import org.apache.commons.lang.StringUtils;

/**
 * Used in combination with OracleUpperTable to generate upper-case output consistent particular chosen Oracle
 * expressions.
 * 
 * @author afield
 * @since 182
 * 
 * @see OracleUpperTable
 */
public class OracleUpper {
    
    /**
     * Upper-case {@code value}, using the information in {@code t} to produce a result consistent with the PL/SQL
     * expression used to generate t.
     */
    public static String toUpperCase(OracleUpperTable t, String value) {
        /*
         * Oracle's upper or nls_upper are known to disagree with Java on some particulars. We search for known
         * exceptional characters and if found take measures to adjust Java's String.toUpperCase. In the average case
         * we incur just a single relatively fast scan of the string. In typical bad cases we'll incur two extra String
         * copies (one copy into the buffer, one out -- this on top of whatever's required by toUpperCase). Note that we
         * have to match Oracle even for characters outside the language's alphabet since we still want to return
         * records containing those characters.
         */
        char[] exceptions = t.getUpperCaseExceptions();
        if (exceptions.length > 0) {
            // Prefer to use String.indexOf in the case of a single search char; it's faster by virtue of not requiring
            // two loops and being intrinsic.
            int nextExceptionIndex = (exceptions.length == 1)?
                    value.indexOf(exceptions[0]) : StringUtils.indexOfAny(value, exceptions);
                    
            if (nextExceptionIndex >= 0) {
                // Annoying case: we have found a character that we know Oracle handles differently than Java and we must
                // adjust appropriately.
                StringBuilder result = new StringBuilder(value.length());
                String rem = value;
                do {
                    char nextException = rem.charAt(nextExceptionIndex);
                    
                    result.append(rem.substring(0, nextExceptionIndex).toUpperCase(t.getLocale()));
                    result.append(t.getUpperCaseExceptionMapping(nextException));
                    
                    rem = rem.substring(nextExceptionIndex + 1);
                    nextExceptionIndex = (exceptions.length == 1)?
                            rem.indexOf(exceptions[0]) : StringUtils.indexOfAny(rem, exceptions);
                } while (nextExceptionIndex >= 0);
                result.append(rem.toUpperCase(t.getLocale()));
                
                return result.toString();
            }
        }
        
        // Nice case: we know of no reason that Oracle and Java wouldn't agree when converting to upper case.
        return value.toUpperCase(t.getLocale());    
    }
}
