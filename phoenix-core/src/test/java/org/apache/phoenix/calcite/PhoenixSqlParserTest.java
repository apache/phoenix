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
package org.apache.phoenix.calcite;

import java.io.IOException;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.phoenix.calcite.parser.PhoenixParserImpl;
import org.junit.Test;

public class PhoenixSqlParserTest extends SqlParserTest {
    
    @Test
    public void testDDL(){
        getTester().checkNode("create table \"t0\"(\"a\" varchar(20) not null primary key, b integer)", isDdl());
        getTester().checkNode("drop table \"t0\"", isDdl());
        getTester().checkNode("create view v1 as select * from \"t0\"", isDdl());
        getTester().checkNode("drop view v1", isDdl());
        getTester().checkNode("create index idx1 on itest(b desc, c) include (d)", isDdl());
        getTester().checkNode("create local index idx1 on itest(b desc, c) include (d)", isDdl());
        getTester().checkNode("drop index idx1 on itest", isDdl());
        getTester().checkNode("create sequence if not exists s0 start with 2 increment 3 minvalue 2 maxvalue 90 cycle cache 3", isDdl());
        getTester().checkNode("drop sequence if exists s0", isDdl());
        getTester().checkNode("update statistics stest columns set dummy=2", isDdl());
        getTester().checkNode("create or replace function myfunction(INTEGER, INTEGER CONSTANT defaultValue=10 minvalue=1 maxvalue=15) returns INTEGER as 'org.apache.phoenix.end2end.MyReverse' using jar 'hdfs://localhost:51573/hbase/tmpjars/myjar1.jar'", isDdl());
        getTester().checkNode("drop function if exists myfunction", isDdl());
        getTester().checkNode("upload jars './myjar.jar'", isDdl());
        getTester().checkNode("delete jar '/myjar.jar'", isDdl());
    }

    private SqlParser getSqlParser(String sql) {
        return SqlParser.create(sql,
            SqlParser.configBuilder().setParserFactory(PhoenixParserImpl.FACTORY)
                .build());
    }

    @Override
    public void testBackTickQuery() {
        // Noop
    }

    @Override
    public void testBracketIdentifier() {
        // Noop
    }

    @Override
    public void testGenerateKeyWords() throws IOException {
        // Noop
    }
}
