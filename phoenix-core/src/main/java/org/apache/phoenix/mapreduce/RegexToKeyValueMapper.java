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
package org.apache.phoenix.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.ColumnInfo;
import org.apache.phoenix.util.UpsertExecutor;
import org.apache.phoenix.util.regex.RegexUpsertExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * MapReduce mapper that converts REGEX input lines into KeyValues that can be written to HFiles.
 * <p/>
 * KeyValues are produced by executing UPSERT statements on a Phoenix connection and then
 * extracting the created KeyValues and rolling back the statement execution before it is
 * committed to HBase.
 */
public class RegexToKeyValueMapper extends FormatToBytesWritableMapper<Map<?, ?>> {

    protected static final Logger LOG = LoggerFactory.getLogger(RegexToKeyValueMapper.class);

    /** Configuration key for the field delimiter for input csv records */
    public static final String REGEX_CONFKEY = "phoenix.mapreduce.import.regex";

    /** Configuration key for the array element delimiter for input arrays */
    public static final String ARRAY_DELIMITER_CONFKEY = "phoenix.mapreduce.import.arraydelimiter";
    
    /** Configuration key for the array element delimiter for input arrays */
    public static final String ARRAY_DELIMITER_DEFAULT = ",";
    
    private LineParser<Map<?, ?>> lineParser;
    
    @Override
    protected  LineParser<Map<?, ?>> getLineParser() {
        return lineParser;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @VisibleForTesting
    @Override
    protected UpsertExecutor<Map<?, ?>, ?> buildUpsertExecutor(Configuration conf) {
        String tableName = conf.get(TABLE_NAME_CONFKEY);
        Preconditions.checkNotNull(tableName, "table name is not configured");
        
        String regex = conf.get(REGEX_CONFKEY);
        Preconditions.checkNotNull(regex, "regex is not configured");
        
        List<ColumnInfo> columnInfoList = buildColumnInfoList(conf);
        
        String arraySeparator = conf.get(ARRAY_DELIMITER_CONFKEY, ARRAY_DELIMITER_DEFAULT);
        
        lineParser = new RegexLineParser(regex, columnInfoList, arraySeparator);

        return new RegexUpsertExecutor(conn, tableName, columnInfoList, upsertListener);
    }

    @VisibleForTesting
    static class RegexLineParser implements LineParser<Map<?, ?>> {
        private Pattern inputPattern;
        private List<ColumnInfo> columnInfoList;
        private String arraySeparator;
        
        public RegexLineParser(String regex, List<ColumnInfo> columnInfo, String arraySep) {
        	inputPattern = Pattern.compile(regex);
        	columnInfoList = columnInfo;
        	arraySeparator = arraySep;
		}

		@Override
        public Map<?, ?> parse(String input) throws IOException {
			Matcher m = inputPattern.matcher(input);
			if (m.groupCount() != columnInfoList.size()) {
				LOG.debug("input {} size doesn't match the pattern {} ", m.groupCount(), columnInfoList.size());
			}
			
			Map<String, Object> data = new HashMap<>();
			
			if (m.find( )) {
				for (int i = 0; i < columnInfoList.size(); i++) {
					ColumnInfo columnInfo = columnInfoList.get(i);
					String colName = columnInfo.getColumnName();
					int sqlType = columnInfo.getSqlType();
					String val = m.group(i + 1);
					Object upsertValue = PDataType.fromTypeId(sqlType).toObject(val);
					data.put(colName, upsertValue);
				}
			}
			return data;
        }
    }
}
