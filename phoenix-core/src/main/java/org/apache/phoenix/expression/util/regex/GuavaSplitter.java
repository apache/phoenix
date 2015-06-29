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

import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.schema.types.PVarcharArray;
import org.apache.phoenix.schema.types.PhoenixArray;
import org.apache.phoenix.util.ByteUtil;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class GuavaSplitter implements AbstractBaseSplitter {
    private final Splitter splitter;

    public GuavaSplitter(String patternString) {
        if (patternString != null) {
            splitter = Splitter.onPattern(patternString);
        } else {
            splitter = null;
        }
    }

    @Override
    public boolean split(ImmutableBytesWritable srcPtr) {
        String sourceStr = (String) PVarchar.INSTANCE.toObject(srcPtr);
        if (sourceStr == null) { // sourceStr evaluated to null
            srcPtr.set(ByteUtil.EMPTY_BYTE_ARRAY);
        } else {
            List<String> splitStrings = Lists.newArrayList(splitter.split(sourceStr));
            PhoenixArray splitArray = new PhoenixArray(PVarchar.INSTANCE, splitStrings.toArray());
            srcPtr.set(PVarcharArray.INSTANCE.toBytes(splitArray));
        }
        return true;
    }
}
