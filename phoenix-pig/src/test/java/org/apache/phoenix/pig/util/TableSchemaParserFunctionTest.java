/*
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 *distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you maynot use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicablelaw or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.pig.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.pig.util.TableSchemaParserFunction;
import org.junit.Test;

import com.google.common.base.Joiner;

public class TableSchemaParserFunctionTest {

    final TableSchemaParserFunction function = new TableSchemaParserFunction();
    
    @Test
    public void testTableSchema() {
        final String loadTableSchema = "EMPLOYEE/col1,col2";
        final Pair<String,String> pair = function.apply(loadTableSchema);
        assertEquals("EMPLOYEE", pair.getFirst());
        assertEquals(pair.getSecond(),Joiner.on(',').join("col1","col2"));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testEmptyTableSchema() {
        final String loadTableSchema = "";
        function.apply(loadTableSchema);
    }
    
    @Test
    public void testTableOnlySchema() {
        final String loadTableSchema = "EMPLOYEE";
        final Pair<String,String> pair = function.apply(loadTableSchema);
        assertEquals("EMPLOYEE", pair.getFirst());
        assertNull(pair.getSecond());
    }
}
