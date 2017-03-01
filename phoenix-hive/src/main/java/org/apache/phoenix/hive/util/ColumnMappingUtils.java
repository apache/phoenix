/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.hive.util;

import com.google.common.base.Splitter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.hive.constants.PhoenixStorageHandlerConstants;

import java.util.*;


/**
 * Util class for mapping between Hive and Phoenix column names
 */
public class ColumnMappingUtils {

    private static final Log LOG = LogFactory.getLog(ColumnMappingUtils.class);

    public static Map<String, String> getColumnMappingMap(String columnMappings) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Column mappings : " + columnMappings);
        }

        if (columnMappings == null || columnMappings.length() == 0) {
            if (LOG.isInfoEnabled()) {
                LOG.info("phoenix.column.mapping not set. using field definition");
            }

            return Collections.emptyMap();
        }

        Map<String, String> columnMappingMap = Splitter.on(PhoenixStorageHandlerConstants.COMMA)
                .trimResults().withKeyValueSeparator(PhoenixStorageHandlerConstants.COLON).split
                        (columnMappings);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Column mapping map : " + columnMappingMap);
        }

        return columnMappingMap;
    }

    public static Map<String, String> getReverseColumnMapping(String columnMapping) {
        Map<String, String> myNewHashMap = new LinkedHashMap<>();
        Map<String, String> forward = getColumnMappingMap(columnMapping);
        for(Map.Entry<String, String> entry : forward.entrySet()){
            myNewHashMap.put(entry.getValue(), entry.getKey());
        }
        return myNewHashMap;
    }

    public static List<String> quoteColumns(List<String> readColumnList) {
        List<String> newList = new LinkedList<>();
        for(String column : readColumnList) {
            newList.add("\""+ column + "\"");
        }
        return newList;
    }
}
