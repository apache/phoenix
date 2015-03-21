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
    public void matches(ImmutableBytesWritable srcPtr, ImmutableBytesWritable outPtr) {
        Preconditions.checkNotNull(srcPtr);
        Preconditions.checkNotNull(outPtr);
        String matcherSourceStr = (String) PVarchar.INSTANCE.toObject(srcPtr);
        if (srcPtr.get().length == 0 && matcherSourceStr == null) matcherSourceStr = "";
        boolean ret = pattern.matcher(matcherSourceStr).matches();
        outPtr.set(ret ? PDataType.TRUE_BYTES : PDataType.FALSE_BYTES);
    }

    @Override
    public String pattern() {
        return pattern.pattern();
    }

    @Override
    public void replaceAll(ImmutableBytesWritable srcPtr, ImmutableBytesWritable replacePtr,
            ImmutableBytesWritable replacedPtr) {
        Preconditions.checkNotNull(srcPtr);
        Preconditions.checkNotNull(replacePtr);
        Preconditions.checkNotNull(replacedPtr);
        String sourceStr = (String) PVarchar.INSTANCE.toObject(srcPtr);
        String replaceStr = (String) PVarchar.INSTANCE.toObject(replacePtr);
        if (srcPtr.get().length == 0 && sourceStr == null) sourceStr = "";
        if (replacePtr.get().length == 0 && replaceStr == null) replaceStr = "";
        String replacedStr = pattern.matcher(sourceStr).replaceAll(replaceStr);
        replacedPtr.set(PVarchar.INSTANCE.toBytes(replacedStr));
    }

    @Override
    public boolean substr(ImmutableBytesWritable srcPtr, int offsetInStr,
            ImmutableBytesWritable outPtr) {
        Preconditions.checkNotNull(srcPtr);
        Preconditions.checkNotNull(outPtr);
        String sourceStr = (String) PVarchar.INSTANCE.toObject(srcPtr);
        if (srcPtr.get().length == 0 && sourceStr == null) sourceStr = "";
        if (offsetInStr < 0) offsetInStr += sourceStr.length();
        if (offsetInStr < 0 || offsetInStr >= sourceStr.length()) return false;
        Matcher matcher = pattern.matcher(sourceStr);
        boolean ret = matcher.find(offsetInStr);
        if (ret) {
            outPtr.set(PVarchar.INSTANCE.toBytes(matcher.group()));
        } else {
            outPtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        }
        return true;
    }
}
