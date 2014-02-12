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
package org.apache.phoenix.compile;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.exception.SQLExceptionInfo;
import org.apache.phoenix.jdbc.PhoenixParameterMetaData;
import org.apache.phoenix.parse.BindParseNode;
import org.apache.phoenix.schema.PDatum;


/**
 * 
 * Class that manages binding parameters and checking type matching. There are
 * two main usages:
 * 
 * 1) the standard query case where we have the values for the binds.
 * 2) the retrieve param metadata case where we don't have the bind values.
 * 
 * In both cases, during query compilation we figure out what type the bind variable
 * "should" be, based on how it's used in the query. For example foo < ? would expect
 * that the bind variable type matches or can be coerced to the type of foo. For (1),
 * we check that the bind value has the correct type and for (2) we set the param
 * metadata type.
 *
 * 
 * @since 0.1
 */
public class BindManager {
    public static final Object UNBOUND_PARAMETER = new Object();

    private final List<Object> binds;
    private final PhoenixParameterMetaData bindMetaData;

    public BindManager(List<Object> binds) {
        this.binds = binds;
        this.bindMetaData = new PhoenixParameterMetaData(binds.size());
    }

    public ParameterMetaData getParameterMetaData() {
        return bindMetaData;
    }
    
    public Object getBindValue(BindParseNode node) throws SQLException {
        int index = node.getIndex();
        if (index < 0 || index >= binds.size()) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_INDEX_OUT_OF_BOUND)
                .setMessage("binds size: " + binds.size() + "; index: " + index).build().buildException();
        }
        Object value = binds.get(index);
        if (value == UNBOUND_PARAMETER) {
            throw new SQLExceptionInfo.Builder(SQLExceptionCode.PARAM_VALUE_UNBOUND)
            .setMessage(node.toString()).build().buildException();
        }
        return value;
    }

    public void addParamMetaData(BindParseNode bind, PDatum column) throws SQLException {
        bindMetaData.addParam(bind,column);
    }
}
