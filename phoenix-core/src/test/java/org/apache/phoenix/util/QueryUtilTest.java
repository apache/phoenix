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
package org.apache.phoenix.util;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class QueryUtilTest {

    private static final ColumnInfo ID_COLUMN = new ColumnInfo("ID", Types.BIGINT);
    private static final ColumnInfo NAME_COLUMN = new ColumnInfo("NAME", Types.VARCHAR);

    @Test
    public void testConstructUpsertStatement_ColumnInfos() {
        assertEquals(
                "UPSERT INTO MYTAB (ID, NAME) VALUES (?, ?)",
                QueryUtil.constructUpsertStatement("MYTAB", ImmutableList.of(ID_COLUMN, NAME_COLUMN)));

    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructUpsertStatement_ColumnInfos_NoColumns() {
        QueryUtil.constructUpsertStatement("MYTAB", ImmutableList.<ColumnInfo>of());
    }

    @Test
    public void testConstructGenericUpsertStatement() {
        assertEquals(
                "UPSERT INTO MYTAB VALUES (?, ?)",
                QueryUtil.constructGenericUpsertStatement("MYTAB", 2));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testConstructGenericUpsertStatement_NoColumns() {
        QueryUtil.constructGenericUpsertStatement("MYTAB", 0);
    }
}
