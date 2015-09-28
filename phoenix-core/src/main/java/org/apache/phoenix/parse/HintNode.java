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

import java.util.HashMap;
import java.util.Map;

import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.StringUtil;

import com.google.common.collect.ImmutableMap;


/**
 * Node representing optimizer hints in SQL
 */
public class HintNode {
    public static final HintNode EMPTY_HINT_NODE = new HintNode();
    
    public static final char SEPARATOR = ' ';
    public static final String PREFIX = "(";
    public static final String SUFFIX = ")";
    // Split on whitespace and parenthesis, keeping the parenthesis in the token array
    private static final String SPLIT_REGEXP = "\\s+|((?<=\\" + PREFIX + ")|(?=\\" + PREFIX + "))|((?<=\\" + SUFFIX + ")|(?=\\" + SUFFIX + "))";
    
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
       * Hint of the form INDEX(<table_name> <index_name>...)
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
        // Split on whitespace or parenthesis. We do not need to handle escaped or
        // embedded whitespace/parenthesis, since we are parsing what will be HBase
        // table names which are not allowed to contain whitespace or parenthesis.
        String[] hintWords = hint.split(SPLIT_REGEXP);
        for (int i = 0; i < hintWords.length; i++) {
            String hintWord = hintWords[i];
            if (hintWord.isEmpty()) {
                continue;
            }
            try {
                Hint key = Hint.valueOf(hintWord.toUpperCase());
                String hintValue = "";
                if (i+1 < hintWords.length && PREFIX.equals(hintWords[i+1])) {
                    StringBuffer hintValueBuf = new StringBuffer(hint.length());
                    hintValueBuf.append(PREFIX);
                    i+=2;
                    while (i < hintWords.length && !SUFFIX.equals(hintWords[i])) {
                        hintValueBuf.append(SchemaUtil.normalizeIdentifier(hintWords[i++]));
                        hintValueBuf.append(SEPARATOR);
                    }
                    // Replace trailing separator with suffix
                    hintValueBuf.replace(hintValueBuf.length()-1, hintValueBuf.length(), SUFFIX);
                    hintValue = hintValueBuf.toString();
                }
                String oldValue = hints.put(key, hintValue);
                // Concatenate together any old value with the new value
                if (oldValue != null) {
                    hints.put(key, oldValue + hintValue);
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
