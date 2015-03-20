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
package org.apache.phoenix.expression.util.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.base.Preconditions;

public class JavaRegexWrapper {
    public static class JavaPattern extends AbstractBasePattern {

        private final Pattern pattern;

        public JavaPattern(String patternString) {
            this(patternString, 0);
        }

        public JavaPattern(String patternString, int flags) {
            if (patternString != null) {
                pattern = Pattern.compile(patternString, flags);
            } else {
                pattern = null;
            }
        }

        @Override
        public AbstractBaseMatcher matcher(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            Preconditions.checkNotNull(ptr);
            Preconditions.checkNotNull(sortOrder);
            String matcherSourceStr = Utils.immutableBytesWritableToString(ptr, sortOrder);
            if (matcherSourceStr == null) {
                return null;
            }
            return new JavaMatcher(pattern.matcher(matcherSourceStr));
        }

        @Override
        public String pattern() {
            return pattern.pattern();
        }
    }

    public static class JavaMatcher extends AbstractBaseMatcher {
        private Matcher matcher;

        public JavaMatcher(Matcher matcher) {
            this.matcher = matcher;
        }

        @Override
        public boolean matches() {
            return matcher.matches();
        }

    }
}
