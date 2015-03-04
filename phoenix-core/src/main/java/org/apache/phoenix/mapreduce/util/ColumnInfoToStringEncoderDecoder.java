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
package org.apache.phoenix.mapreduce.util;

import java.util.List;

import org.apache.phoenix.util.ColumnInfo;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * A codec to transform a {@link ColumnInfo} to a {@link String} and decode back.
 */
public class ColumnInfoToStringEncoderDecoder {

    private static final String COLUMN_INFO_DELIMITER = "|";
    
    private ColumnInfoToStringEncoderDecoder() {
        
    }
    
    public static String encode(List<ColumnInfo> columnInfos) {
        Preconditions.checkNotNull(columnInfos);
        return Joiner.on(COLUMN_INFO_DELIMITER)
                     .skipNulls()
                     .join(columnInfos);
    }
    
    public static List<ColumnInfo> decode(final String columnInfoStr) {
        Preconditions.checkNotNull(columnInfoStr);
        List<ColumnInfo> columnInfos = Lists.newArrayList(
                                Iterables.transform(
                                        Splitter.on(COLUMN_INFO_DELIMITER).omitEmptyStrings().split(columnInfoStr),
                                        new Function<String, ColumnInfo>() {
                                            @Override
                                            public ColumnInfo apply(String colInfo) {
                                               return ColumnInfo.fromString(colInfo);
                                            }
                                        }));
        return columnInfos;
        
    }

    
}
