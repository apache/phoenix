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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.SchemaUtil;
import org.junit.Test;

public class SchemaUtilTest {

    @Test
    public void testExceptionCode() throws Exception {
        SQLExceptionCode code = SQLExceptionCode.fromErrorCode(SQLExceptionCode.AGGREGATE_IN_GROUP_BY.getErrorCode());
        assertEquals(SQLExceptionCode.AGGREGATE_IN_GROUP_BY, code);
    }
    
    @Test
    public void testGetTableName() {
        String tableDisplayName = SchemaUtil.getTableName("schemaName", "tableName");
        assertEquals(tableDisplayName, "schemaName.tableName");
        tableDisplayName = SchemaUtil.getTableName(null, "tableName");
        assertEquals(tableDisplayName, "tableName");
    }

    @Test
    public void testGetColumnName() {
        String columnDisplayName;
        columnDisplayName = SchemaUtil.getMetaDataEntityName("schemaName", "tableName", "familyName", "columnName");
        assertEquals(columnDisplayName, "schemaName.tableName.familyName.columnName");
        columnDisplayName = SchemaUtil.getMetaDataEntityName(null, "tableName", "familyName", "columnName");
        assertEquals(columnDisplayName, "tableName.familyName.columnName");
        columnDisplayName = SchemaUtil.getMetaDataEntityName("schemaName", "tableName", null, "columnName");
        assertEquals(columnDisplayName, "schemaName.tableName.columnName");
        columnDisplayName = SchemaUtil.getMetaDataEntityName(null, null, "familyName", "columnName");
        assertEquals(columnDisplayName, "familyName.columnName");
        columnDisplayName = SchemaUtil.getMetaDataEntityName(null, null, null, "columnName");
        assertEquals(columnDisplayName, "columnName");
    }
    
    @Test
    public void testEscapingColumnName() {
        assertEquals("\"ID\"", SchemaUtil.getEscapedFullColumnName("ID"));
        assertEquals("\"0\".\"NAME\"", SchemaUtil.getEscapedFullColumnName("0.NAME"));
        assertEquals("\"CF1\".\"LOCATION\"", SchemaUtil.getEscapedFullColumnName("CF1.LOCATION"));
    }

    @Test
    public void testGetTableNameFromFullNameByte() {
        String tableDisplayName = SchemaUtil.getTableNameFromFullName(Bytes.toBytes("schemaName.tableName"));
        assertEquals(tableDisplayName, "tableName");
    }

    @Test
    public void testGetTableNameFromFullName() {
        String tableDisplayName = SchemaUtil.getTableNameFromFullName("schemaName.tableName");
        assertEquals(tableDisplayName, "tableName");
    }
}
