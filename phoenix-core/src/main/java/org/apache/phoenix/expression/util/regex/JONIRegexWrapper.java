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

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.jcodings.Encoding;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Syntax;

import com.google.common.base.Preconditions;

public class JONIRegexWrapper {
    public static class JONIPattern extends AbstractBasePattern {

        private final Regex pattern;
        private final String patternString;

        public JONIPattern(String patternString) {
            this(patternString, 0);
        }

        public JONIPattern(String patternString, int flags) {
            this(patternString, flags, UTF8Encoding.INSTANCE);
        }

        public JONIPattern(String patternString, int flags, Encoding coding) {
            this.patternString = patternString;
            if (patternString != null) {
                byte[] bytes = patternString.getBytes();
                pattern = new Regex(bytes, 0, bytes.length, flags, coding, Syntax.Java);
            } else {
                pattern = null;
            }
        }

        @Override
        public boolean matches(ImmutableBytesWritable ptr) {
            Preconditions.checkNotNull(ptr);
            byte[] matcherSourceBytes = ptr.get();
            return matches(matcherSourceBytes, 0, matcherSourceBytes.length);
        }

        /**
         * @return if bytes[offset, range) match pattern
         */
        public boolean matches(byte[] bytes, int offset, int range) {
            Matcher matcher = pattern.matcher(bytes, offset, range);
            int ret = matcher.match(offset, range, Option.DEFAULT);
            return (range - offset) == ret;
        }

        @Override
        public String pattern() {
            return patternString;
        }

        @Override
        public void replaceAll(ImmutableBytesWritable srcPtr, ImmutableBytesWritable replacePtr,
                ImmutableBytesWritable replacedPtr) {
            Preconditions.checkNotNull(srcPtr);
            Preconditions.checkNotNull(replacePtr);
            Preconditions.checkNotNull(replacedPtr);
            byte[] srcBytes = srcPtr.get(), replaceBytes = replacePtr.get();
            byte[] replacedBytes = replaceAll(srcBytes, replaceBytes);
            replacedPtr.set(replacedBytes);
        }

        public byte[] replaceAll(byte[] srcBytes, int srcOffset, int srcRange, byte[] replaceBytes,
                int replaceOffset, int replaceRange) {
            class PairInt {
                public int begin, end;

                public PairInt(int begin, int end) {
                    this.begin = begin;
                    this.end = end;
                }
            }
            Matcher matcher = pattern.matcher(srcBytes, srcOffset, srcRange);
            int replaceLen = replaceRange - replaceOffset;
            int cur = srcOffset;
            List<PairInt> searchResults = new LinkedList<PairInt>();
            int totalBytesNeeded = 0;
            while (true) {
                int nextCur = matcher.search(cur, srcRange, Option.DEFAULT);
                if (nextCur < 0) {
                    totalBytesNeeded += srcBytes.length - cur;
                    break;
                }
                searchResults.add(new PairInt(matcher.getBegin(), matcher.getEnd()));
                totalBytesNeeded += (nextCur - cur) + replaceLen;
                cur = matcher.getEnd();
            }
            byte[] ret = new byte[totalBytesNeeded];
            int curPosInSrc = srcOffset, curPosInRet = 0;
            for (PairInt pair : searchResults) {
                System.arraycopy(srcBytes, curPosInSrc, ret, curPosInRet, pair.begin - curPosInSrc);
                curPosInRet += pair.begin - curPosInSrc;
                System.arraycopy(replaceBytes, replaceOffset, ret, curPosInRet, replaceLen);
                curPosInRet += replaceLen;
                curPosInSrc = pair.end;
            }
            System.arraycopy(srcBytes, curPosInSrc, ret, curPosInRet, srcRange - curPosInSrc);
            return ret;
        }

        public byte[] replaceAll(byte[] srcBytes, byte[] replaceBytes) {
            return replaceAll(srcBytes, 0, srcBytes.length, replaceBytes, 0, replaceBytes.length);
        }
    }
}
