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
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.base.Preconditions;

public class JavaPattern extends AbstractBasePattern {

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
    public void matches(ImmutableBytesWritable srcPtr) {
        Preconditions.checkNotNull(srcPtr);
        String matcherSourceStr = (String) PVarchar.INSTANCE.toObject(srcPtr);
        if (srcPtr.getLength() == 0 && matcherSourceStr == null) matcherSourceStr = "";
        boolean ret = pattern.matcher(matcherSourceStr).matches();
        srcPtr.set(ret ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
    }

    @Override
    public String pattern() {
        return pattern.pattern();
    }

    @Override
    public void replaceAll(ImmutableBytesWritable srcPtr, byte[] rStrBytes, int rStrOffset,
            int rStrLen) {
        Preconditions.checkNotNull(srcPtr);
        Preconditions.checkNotNull(rStrBytes);
        String sourceStr = (String) PVarchar.INSTANCE.toObject(srcPtr);
        String replaceStr = (String) PVarchar.INSTANCE.toObject(rStrBytes, rStrOffset, rStrLen);
        if (srcPtr.getLength() == 0 && sourceStr == null) sourceStr = "";
        if (rStrLen == 0 && replaceStr == null) replaceStr = "";
        String replacedStr = pattern.matcher(sourceStr).replaceAll(replaceStr);
        srcPtr.set(PVarchar.INSTANCE.toBytes(replacedStr));
    }

    @Override
    public void substr(ImmutableBytesWritable ptr, int offsetInStr) {
        Preconditions.checkNotNull(ptr);
        String sourceStr = (String) PVarchar.INSTANCE.toObject(ptr);
        if (sourceStr == null) {
            ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        } else {
            if (offsetInStr < 0) offsetInStr += sourceStr.length();
            if (offsetInStr < 0 || offsetInStr >= sourceStr.length()) {
                ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
            } else {
                Matcher matcher = pattern.matcher(sourceStr);
                boolean ret = matcher.find(offsetInStr);
                if (ret) {
                    ptr.set(PVarchar.INSTANCE.toBytes(matcher.group()));
                } else {
                    ptr.set(ByteUtil.EMPTY_BYTE_ARRAY);
                }
            }
        }
    }
}
