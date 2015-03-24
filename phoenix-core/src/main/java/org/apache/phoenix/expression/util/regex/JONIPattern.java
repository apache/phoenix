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

import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;
import org.jcodings.Encoding;
import org.jcodings.specific.UTF8Encoding;
import org.joni.Matcher;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Syntax;

import com.google.common.base.Preconditions;

public class JONIPattern extends AbstractBasePattern {

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
    public void matches(ImmutableBytesWritable srcPtr, ImmutableBytesWritable outPtr) {
        Preconditions.checkNotNull(srcPtr);
        Preconditions.checkNotNull(outPtr);
        boolean ret = matches(srcPtr.get(), srcPtr.getOffset(), srcPtr.getLength());
        outPtr.set(ret ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
    }

    public boolean matches(byte[] bytes, int offset, int len) {
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
    public void replaceAll(ImmutableBytesWritable srcPtr, ImmutableBytesWritable replacePtr,
            ImmutableBytesWritable replacedPtr) {
        Preconditions.checkNotNull(srcPtr);
        Preconditions.checkNotNull(replacePtr);
        Preconditions.checkNotNull(replacedPtr);
        byte[] replacedBytes =
                replaceAll(srcPtr.get(), srcPtr.getOffset(), srcPtr.getLength(), replacePtr.get(),
                    replacePtr.getOffset(), replacePtr.getLength());
        replacedPtr.set(replacedBytes);
    }

    private byte[] replaceAll(byte[] srcBytes, int srcOffset, int srcLen, byte[] replaceBytes,
            int replaceOffset, int replaceLen) {
        int srcRange = srcOffset + srcLen;
        Matcher matcher = pattern.matcher(srcBytes, 0, srcRange);
        int cur = srcOffset;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        while (true) {
            int nextCur = matcher.search(cur, srcRange, Option.DEFAULT);
            if (nextCur < 0) {
                out.write(srcBytes, cur, srcRange - cur);
                break;
            }
            out.write(srcBytes, cur, nextCur - cur);
            out.write(replaceBytes, replaceOffset, replaceLen);
            cur = matcher.getEnd();
        }
        byte[] ret = out.toByteArray();
        try {
            out.close();
        } catch (IOException e) {
            // According to ByteArrayOutputStream#close, never goes here
        }
        return ret;
    }

    @Override
    public boolean substr(ImmutableBytesWritable srcPtr, int offsetInStr,
            ImmutableBytesWritable outPtr) {
        Preconditions.checkNotNull(srcPtr);
        Preconditions.checkNotNull(outPtr);
        byte[] srcBytes = srcPtr.get();
        int offsetInBytes;
        if (offsetInStr == 0) {
            offsetInBytes = 0;
        } else {
            String sourceStr = (String) PVarchar.INSTANCE.toObject(srcPtr);
            if (srcPtr.get().length == 0 && sourceStr == null) sourceStr = "";
            int srcStrLen = sourceStr.length();
            if (offsetInStr < 0) offsetInStr += srcStrLen;
            if (offsetInStr < 0 || offsetInStr >= srcStrLen) return false;
            String strBeforeOffset = sourceStr.substring(0, offsetInStr);
            offsetInBytes = PVarchar.INSTANCE.toBytes(strBeforeOffset).length;
        }
        offsetInBytes += srcPtr.getOffset();
        substr(srcBytes, offsetInBytes, srcPtr.getOffset() + srcPtr.getLength(), outPtr);
        return true;
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
}
