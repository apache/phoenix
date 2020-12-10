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
package org.apache.phoenix.util.json;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.phoenix.util.AbstractUpsertExecutorTest;
import org.apache.phoenix.util.UpsertExecutor;
import org.junit.Before;

public class JsonUpsertExecutorTest extends AbstractUpsertExecutorTest<Map<?, ?>, Object> {

    private UpsertExecutor<Map<?, ?>, Object> upsertExecutor;

    @Override
    protected UpsertExecutor<Map<?, ?>, Object> getUpsertExecutor() {
        return upsertExecutor;
    }

    @Override
    protected Map<?, ?> createRecord(Object... columnValues) throws IOException {
        Map ret = new HashMap(columnValues.length);
        int min = Math.min(columnInfoList.size(), columnValues.length);
        for (int i = 0; i < min; i++) {
            ret.put(columnInfoList.get(i).getColumnName().replace("\"", "").toLowerCase(), columnValues[i]);
        }
        return ret;
    }

    @Before
    public void setUp() throws SQLException {
        super.setUp();
        upsertExecutor = new JsonUpsertExecutor(conn, columnInfoList, preparedStatement, upsertListener);
    }

    @Override
    protected UpsertExecutor<Map<?, ?>, Object> getUpsertExecutor(Connection conn) {
        return new JsonUpsertExecutor(conn, columnInfoList, preparedStatement, upsertListener);
    }
}
