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

package org.apache.phoenix.parse;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PLong;
import org.junit.Test;

import static org.junit.Assert.*;

public class CastParseNodeTest {

    @Test
    public void testToSQL() {
        ColumnParseNode columnParseNode = new ColumnParseNode(TableName.create("SCHEMA1", "TABLE1"), "V");
        CastParseNode castParseNode = new CastParseNode(columnParseNode, PLong.INSTANCE, null, null, false);
        StringBuilder stringBuilder = new StringBuilder();
        castParseNode.toSQL(null, stringBuilder);
        assertEquals(" CAST(TABLE1.V AS BIGINT)", stringBuilder.toString());
    }

    @Test
    public void testToSQL_WithLengthAndScale() {
        ColumnParseNode columnParseNode = new ColumnParseNode(TableName.create("SCHEMA1", "TABLE1"), "V");
        CastParseNode castParseNode = new CastParseNode(columnParseNode, PDecimal.INSTANCE, 5, 3, false);
        StringBuilder stringBuilder = new StringBuilder();
        castParseNode.toSQL(null, stringBuilder);
        assertEquals(" CAST(TABLE1.V AS DECIMAL(5,3))", stringBuilder.toString());
    }

    @Test
    public void testToSQL_ArrayType() {
        ColumnParseNode columnParseNode = new ColumnParseNode(TableName.create("SCHEMA1", "TABLE1"), "V");
        CastParseNode castParseNode = new CastParseNode(columnParseNode, PLong.INSTANCE, null, null, true);
        StringBuilder stringBuilder = new StringBuilder();
        castParseNode.toSQL(null, stringBuilder);
        assertEquals(" CAST(TABLE1.V AS BIGINT ARRAY)", stringBuilder.toString());
    }
}