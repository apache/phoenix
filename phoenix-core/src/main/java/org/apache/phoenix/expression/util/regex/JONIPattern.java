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
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PArrayDataTypeEncoder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.apache.phoenix.util.StringUtil;
import org.jcodings.Encoding;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Syntax;

import com.google.common.base.Preconditions;

public class JONIPattern extends AbstractBasePattern implements AbstractBaseSplitter {

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
    public void matches(ImmutableBytesWritable srcPtr) {
        Preconditions.checkNotNull(srcPtr);
        boolean ret = matches(srcPtr.get(), srcPtr.getOffset(), srcPtr.getLength());
        srcPtr.set(ret ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
    }

    private boolean matches(byte[] bytes, int offset, int len) {
        int range = offset + len;
        Matcher matcher = pattern.matcher(bytes, offset, range);
        int ret = matcher.match(offset, range, Option.DEFAULT);
        return len == ret;
    }

    @Override
    public String pattern() {
        return patternString;
    }

    @Override
    public void replaceAll(ImmutableBytesWritable srcPtr, byte[] rStrBytes, int rStrOffset,
            int rStrLen) {
        Preconditions.checkNotNull(srcPtr);
        Preconditions.checkNotNull(rStrBytes);
        byte[] replacedBytes =
                replaceAll(srcPtr.get(), srcPtr.getOffset(), srcPtr.getLength(), rStrBytes,
                    rStrOffset, rStrLen);
        srcPtr.set(replacedBytes);
    }

    private byte[] replaceAll(byte[] srcBytes, int srcOffset, int srcLen, byte[] replaceBytes,
            int replaceOffset, int replaceLen) {
        class PairInt {
            public int begin, end;

            public PairInt(int begin, int end) {
                this.begin = begin;
                this.end = end;
            }
        }
        int srcRange = srcOffset + srcLen;
        Matcher matcher = pattern.matcher(srcBytes, 0, srcRange);
        int cur = srcOffset;
        List<PairInt> searchResults = new LinkedList<PairInt>();
        int totalBytesNeeded = 0;
        while (true) {
            int nextCur = matcher.search(cur, srcRange, Option.DEFAULT);
            if (nextCur < 0) {
                totalBytesNeeded += srcRange - cur;
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

    @Override
    public void substr(ImmutableBytesWritable ptr, int offsetInStr) {
        Preconditions.checkNotNull(ptr);
        int offsetInBytes = StringUtil.calculateUTF8Offset(ptr.get(), ptr.getOffset(),
                ptr.getLength(), SortOrder.ASC, offsetInStr);
        if (offsetInBytes < 0) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        } else {
            substr(ptr.get(), offsetInBytes, ptr.getOffset() + ptr.getLength(), ptr);
        }
    }

    private boolean substr(byte[] srcBytes, int offset, int range, ImmutableBytesWritable outPtr) {
        Matcher matcher = pattern.matcher(srcBytes, 0, range);
        boolean ret = matcher.search(offset, range, Option.DEFAULT) >= 0;
        if (ret) {
            int len = matcher.getEnd() - matcher.getBegin();
            outPtr.set(srcBytes, matcher.getBegin(), len);
        } else {
            outPtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        }
        return ret;
    }

    @Override
    public boolean split(ImmutableBytesWritable srcPtr) {
        return split(srcPtr.get(), srcPtr.getOffset(), srcPtr.getLength(), srcPtr);
    }

    private boolean
            split(byte[] srcBytes, int srcOffset, int srcLen, ImmutableBytesWritable outPtr) {
        SortOrder sortOrder = SortOrder.ASC;
        PArrayDataTypeEncoder builder =
                new PArrayDataTypeEncoder(PVarchar.INSTANCE, sortOrder);
        int srcRange = srcOffset + srcLen;
        Matcher matcher = pattern.matcher(srcBytes, 0, srcRange);
        int cur = srcOffset;
        boolean append;
        while (true) {
            int nextCur = matcher.search(cur, srcRange, Option.DEFAULT);
            if (nextCur < 0) {
                builder.appendValue(srcBytes, cur, srcRange - cur);
                break;
            }

            // To handle the following case, which adds null at first.
            // REGEXP_SPLIT("12ONE34TWO56THREE78","[0-9]+")={null, "ONE", "TWO", "THREE", null}
            if (cur == matcher.getBegin()) {
                builder.appendValue(srcBytes, cur, 0);
            }

            if (cur < matcher.getBegin()) {
                builder.appendValue(srcBytes, cur, matcher.getBegin() - cur);
            }
            cur = matcher.getEnd();

            // To handle the following case, which adds null at last.
            // REGEXP_SPLIT("12ONE34TWO56THREE78","[0-9]+")={null, "ONE", "TWO", "THREE", null}
            if (cur == srcRange) {
                builder.appendValue(srcBytes, cur, 0);
                break;
            }
        }
        byte[] bytes = builder.encode();
        if (bytes == null) return false;
        outPtr.set(bytes);
        return true;
    }
}
