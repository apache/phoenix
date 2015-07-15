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

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.util.ColumnInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A codec to transform a {@link ColumnInfo} to a {@link String} and decode back.
 */
public class ColumnInfoToStringEncoderDecoder {

    static final String CONFIGURATION_VALUE_PREFIX = "phoenix.colinfo.encoder.decoeder.value";
    static final String CONFIGURATION_COUNT = "phoenix.colinfo.encoder.decoder.count";
    
    private ColumnInfoToStringEncoderDecoder() {
        
    }
    
    public static void encode(Configuration configuration, List<ColumnInfo> columnInfos) {
    	Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(columnInfos);
        int count=0;
        for (int i=0; i<columnInfos.size(); ++i) {
        	if (columnInfos.get(i)!=null) {
        		configuration.set(String.format("%s_%d", CONFIGURATION_VALUE_PREFIX, i), columnInfos.get(i).toString());
        		++count;
        	}
        }
        configuration.setInt(CONFIGURATION_COUNT, count);
    }
    
    public static List<ColumnInfo> decode(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        int numCols = configuration.getInt(CONFIGURATION_COUNT, 0);
        List<ColumnInfo> columnInfos = Lists.newArrayListWithExpectedSize(numCols);
        for (int i=0; i<numCols; ++i) {
        	columnInfos.add(ColumnInfo.fromString(configuration.get(String.format("%s_%d", CONFIGURATION_VALUE_PREFIX, i))));
        }
        return columnInfos;
    }

    
}
