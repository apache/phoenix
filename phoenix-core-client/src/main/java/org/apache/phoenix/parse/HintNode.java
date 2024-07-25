/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.parse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;

import org.apache.phoenix.thirdparty.com.google.common.collect.ImmutableMap;


/**
 * Node representing optimizer hints in SQL
 */
public class HintNode {
    public static final HintNode EMPTY_HINT_NODE = new HintNode();
    
    public static final char SEPARATOR = ' ';
    public static final String PREFIX = "(";
    public static final String SUFFIX = ")";
    // Each hint is of the generic syntax hintWord(hintArgs) where hintArgs in parent are optional.
    private static final Pattern HINT_PATTERN = Pattern.compile(
            "(?<hintWord>\\w+)\\s*(?:\\s*\\(\\s*(?<hintArgs>[^)]+)\\s*\\))?");
    private static final Pattern HINT_ARG_PATTERN = Pattern.compile("(?<hintArg>\"[^\"]+\"|\\S+)");

    public enum Hint {
        /**
         * Forces a range scan to be used to process the query.
         */
        RANGE_SCAN,
        /**
         * Forces a skip scan to be used to process the query.
         */
        SKIP_SCAN,
        /**
         * Prevents the usage of child-parent-join optimization.
         */
        NO_CHILD_PARENT_JOIN_OPTIMIZATION,
        /**
         * Prevents the usage of indexes, forcing usage
         * of the data table for a query.
         */
        NO_INDEX,
        /**
         * Hint of the form {@code INDEX(<table_name> <index_name>...) }
         * to suggest usage of the index if possible. The first
         * usable index in the list of indexes will be choosen.
         * Table and index names may be surrounded by double quotes
         * if they are case sensitive.
         */
        INDEX,
        /**
         * All things being equal, use the data table instead of
         * the index table when optimizing.
         */
        USE_DATA_OVER_INDEX_TABLE,
        /**
         * All things being equal, use the index table instead of
         * the data table when optimizing.
         */
        USE_INDEX_OVER_DATA_TABLE,
        /**
         * Avoid caching any HBase blocks loaded by this query.
         */
        NO_CACHE,
        /**
         * Use sort-merge join algorithm instead of broadcast join (hash join) algorithm.
         */
        USE_SORT_MERGE_JOIN,
        /**
         * Persist the RHS results of a hash join.
         */
        USE_PERSISTENT_CACHE,
        /**
         * Avoid using star-join optimization. Used for broadcast join (hash join) only.
         */
        NO_STAR_JOIN,
        /**
         * Avoid using the no seek optimization. When there are many columns which are not selected coming in between 2
         * selected columns and/or versions of columns, this should be used.
         */
        SEEK_TO_COLUMN,
        /**
         * Avoid seeks to select specified columns. When there are very less number of columns which are not selected in
         * between 2 selected columns this will be give better performance.
         */
        NO_SEEK_TO_COLUMN,
        /**
         * Saves an RPC call on the scan. See Scan.setSmall(true) in HBase documentation.
         */
        SMALL,
        /**
         * Enforces a serial scan.
         */
        SERIAL,
        /**
         * Enforces a forward scan.
         */
        FORWARD_SCAN,
        /**
         * Prefer a hash aggregate over a sort plus streaming aggregate.
         * Issue https://issues.apache.org/jira/browse/PHOENIX-4751.
         */
        HASH_AGGREGATE,
        /**
         * Do not use server merge for hinted uncovered indexes
         */
        NO_INDEX_SERVER_MERGE,

        /**
         * Override the default CDC include scopes.
         */
        CDC_INCLUDE,
        ;
    };

    private final Map<Hint,String> hints;

    public static HintNode create(HintNode hintNode, Hint hint) {
        return create(hintNode, hint, "");
    }
    
    public static HintNode create(HintNode hintNode, Hint hint, String value) {
        Map<Hint,String> hints = new HashMap<Hint,String>(hintNode.hints);
        hints.put(hint, value);
        return new HintNode(hints);
    }
    
    public static HintNode combine(HintNode hintNode, HintNode override) {
        Map<Hint,String> hints = new HashMap<Hint,String>(hintNode.hints);
        hints.putAll(override.hints);
        return new HintNode(hints);
    }
    
    public static HintNode subtract(HintNode hintNode, Hint[] remove) {
        Map<Hint,String> hints = new HashMap<Hint,String>(hintNode.hints);
        for (Hint hint : remove) {
            hints.remove(hint);
        }
        return new HintNode(hints);
    }
    
    private HintNode() {
        hints = new HashMap<Hint,String>();
    }

    private HintNode(Map<Hint,String> hints) {
        this.hints = ImmutableMap.copyOf(hints);
    }

    public HintNode(String hint) {
        Map<Hint,String> hints = new HashMap<Hint,String>();
        Matcher hintMatcher = HINT_PATTERN.matcher(hint);
        while (hintMatcher.find()) {
            try {
                Hint hintWord = Hint.valueOf(hintMatcher.group("hintWord").toUpperCase());
                String hintArgsStr = hintMatcher.group("hintArgs");
                List<String> hintArgs = new ArrayList<>();
                if (hintArgsStr != null) {
                    Matcher hintArgMatcher = HINT_ARG_PATTERN.matcher(hintArgsStr);
                    while (hintArgMatcher.find()) {
                        hintArgs.add(SchemaUtil.normalizeIdentifier(hintArgMatcher.group()));
                    }
                }
                hintArgsStr = String.join(" ", hintArgs);
                hintArgsStr = hintArgsStr.equals("") ? "" : "(" + hintArgsStr + ")";
                if (hints.containsKey(hintWord)) {
                    // Concatenate together any old value with the new value
                    hints.put(hintWord, hints.get(hintWord) + hintArgsStr);
                }
                else {
                    hints.put(hintWord, hintArgsStr);
                }
            } catch (IllegalArgumentException e) { // Ignore unknown/invalid hints
            }
        }
        this.hints = ImmutableMap.copyOf(hints);
    }

    public boolean isEmpty() {
        return hints.isEmpty();
    }

    /**
     * Gets the value of the hint or null if the hint is not present.
     * @param hint the hint
     * @return the value specified in parenthesis following the hint or null
     * if the hint is not present.
     * 
     */
    public String getHint(Hint hint) {
        return hints.get(hint);
    }

    /**
     * Tests for the presence of a hint in a query
     * @param hint the hint
     * @return true if the hint is present and false otherwise
     */
    public boolean hasHint(Hint hint) {
        return hints.containsKey(hint);
    }

    public Set<Hint> getHints(){
        return hints.keySet();
    }
    
    @Override
    public String toString() {
        if (hints.isEmpty()) {
            return StringUtil.EMPTY_STRING;
        }
        StringBuilder buf = new StringBuilder("/*+ ");
        for (Map.Entry<Hint, String> entry : hints.entrySet()) {
            buf.append(entry.getKey());
            buf.append(entry.getValue());
            buf.append(' ');
        }
        buf.append("*/ ");
        return buf.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((hints == null) ? 0 : hints.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        HintNode other = (HintNode)obj;
        if (hints == null) {
            if (other.hints != null) return false;
        } else if (!hints.equals(other.hints)) return false;
        return true;
    }
}
