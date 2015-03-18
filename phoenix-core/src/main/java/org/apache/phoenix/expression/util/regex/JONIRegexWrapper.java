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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.SortOrder;
import org.jcodings.Encoding;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;

import com.google.common.base.Preconditions;

public class JONIRegexWrapper {

    private static final Encoding PVARCHAR_ENCODING = UTF8Encoding.INSTANCE;

    static class JONIPattern extends AbstractBasePattern {

        private final Regex pattern;
        private boolean isLastMatcherStringNull;
        private final String patternString;

        JONIPattern(String patternString) {
            this(patternString, 0);
        }

        JONIPattern(String patternString, int flags) {
            this.patternString = patternString;
            if (patternString != null) {
                patternString = replacePatternQuote(patternString);
                byte[] patternBytes = patternString.getBytes();
                pattern = new Regex(patternBytes, 0, patternBytes.length, flags, PVARCHAR_ENCODING);
            } else {
                pattern = null;
            }
            isLastMatcherStringNull = false;
        }

        public enum ReplaceQuoteMachine {
            STOP, ZERO, ONE_BACKSLASH, IN_QUOTE, IN_QUOTE_ONE_BACKSLASH;

            ReplaceQuoteMachine next(StringBuilder sb, char ch) {
                switch (this) {
                case ZERO:
                    switch (ch) {
                    case '\\':
                        return ONE_BACKSLASH;
                    default:
                        sb.append(ch);
                        return ZERO;
                    }
                case ONE_BACKSLASH:
                    switch (ch) {
                    case 'Q':
                        return IN_QUOTE;
                    default:
                        sb.append('\\');
                        return ZERO.next(sb, ch);
                    }
                case IN_QUOTE:
                    // add backslashes for .^$*+?()[{\|
                    switch (ch) {
                    case '.':
                    case '^':
                    case '$':
                    case '*':
                    case '+':
                    case '?':
                    case '(':
                    case ')':
                    case '[':
                    case '{':
                    case '|':
                        sb.append('\\').append(ch);
                        return IN_QUOTE;
                    case '\\':
                        return IN_QUOTE_ONE_BACKSLASH;
                    default:
                        sb.append(ch);
                        return IN_QUOTE;
                    }
                case IN_QUOTE_ONE_BACKSLASH:
                    switch (ch) {
                    case 'E':
                        return ZERO;
                    default:
                        sb.append('\\');
                        return IN_QUOTE.next(sb, ch);
                    }
                case STOP:
                default:
                    throw new IllegalArgumentException();
                }
            }

            ReplaceQuoteMachine EOF(StringBuilder sb) {
                switch (this) {
                case ONE_BACKSLASH:
                case IN_QUOTE_ONE_BACKSLASH:
                    sb.append('\\');
                case ZERO:
                case IN_QUOTE:
                    return STOP;
                case STOP:
                default:
                    throw new IllegalArgumentException();
                }
            }
        }

        private String replacePatternQuote(String patternString) {
            StringBuilder sb = new StringBuilder();
            ReplaceQuoteMachine cur = ReplaceQuoteMachine.ZERO;
            for (int i = 0; i < patternString.length(); ++i) {
                cur = cur.next(sb, patternString.charAt(i));
            }
            cur = cur.EOF(sb);
            return sb.toString();
        }

        @Override
        public AbstractBaseMatcher matcher(ImmutableBytesWritable ptr, SortOrder sortOrder) {
            Preconditions.checkNotNull(ptr);
            Preconditions.checkNotNull(sortOrder);
            byte[] matcherSourceBytes = Utils.immutableBytesWritableToBytes(ptr, sortOrder);
            if (matcherSourceBytes == null) {
                isLastMatcherStringNull = true;
                return null;
            }
            isLastMatcherStringNull = false;
            return new JONIMatcher(pattern.matcher(matcherSourceBytes), matcherSourceBytes.length);
        }

        @Override
        public boolean isPatternStringNull() {
            return pattern == null;
        }

        @Override
        public boolean isMatcherSourceStrNull() {
            return isLastMatcherStringNull;
        }

        @Override
        public String pattern() {
            return patternString;
        }
    }

    static class JONIMatcher extends AbstractBaseMatcher {
        private Matcher matcher;
        private final int matcherSourceBytesLen;

        JONIMatcher(Matcher matcher, int matcherSourceBytesLen) {
            this.matcher = matcher;
            this.matcherSourceBytesLen = matcherSourceBytesLen;
        }

        @Override
        public boolean matches() {
            return matcherSourceBytesLen == matcher.match(0, matcherSourceBytesLen, Option.DEFAULT);
        }
    }
}
