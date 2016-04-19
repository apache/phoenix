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
package org.apache.phoenix.hive;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.serde2.StructObject;

import java.util.List;
import java.util.Map;

/**
 * Implementation for Hive SerDe StructObject
 */
public class PhoenixRow implements StructObject {

    private List<String> columnList;
    private Map<String, Object> resultRowMap;

    public PhoenixRow(List<String> columnList) {
        this.columnList = columnList;
    }

    public PhoenixRow setResultRowMap(Map<String, Object> resultRowMap) {
        this.resultRowMap = resultRowMap;
        return this;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.serde2.StructObject#getField(int)
     */
    @Override
    public Object getField(int fieldID) {
        return resultRowMap.get(columnList.get(fieldID));
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hive.serde2.StructObject#getFieldsAsList()
     */
    @Override
    public List<Object> getFieldsAsList() {
        return Lists.newArrayList(resultRowMap.values());
    }


    @Override
    public String toString() {
        return resultRowMap.toString();
    }
}
