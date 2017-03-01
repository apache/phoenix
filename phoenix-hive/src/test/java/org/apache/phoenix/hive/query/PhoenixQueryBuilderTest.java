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
package org.apache.phoenix.hive.query;

import com.google.common.collect.Lists;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.phoenix.hive.ql.index.IndexSearchCondition;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

public class PhoenixQueryBuilderTest {
    private static final PhoenixQueryBuilder BUILDER = PhoenixQueryBuilder.getInstance();
    private static final String TABLE_NAME = "TEST_TABLE";

    private IndexSearchCondition mockedIndexSearchCondition(String comparisionOp,
                                                            Object constantValue,
                                                            Object[] constantValues,
                                                            String columnName,
                                                            String typeString,
                                                            boolean isNot) {
        IndexSearchCondition condition = mock(IndexSearchCondition.class);
        when(condition.getComparisonOp()).thenReturn(comparisionOp);

        if (constantValue != null) {
            ExprNodeConstantDesc constantDesc = mock(ExprNodeConstantDesc.class);
            when(constantDesc.getValue()).thenReturn(constantValue);
            when(condition.getConstantDesc()).thenReturn(constantDesc);
        }

        ExprNodeColumnDesc columnDesc = mock(ExprNodeColumnDesc.class);
        when(columnDesc.getColumn()).thenReturn(columnName);
        when(columnDesc.getTypeString()).thenReturn(typeString);
        when(condition.getColumnDesc()).thenReturn(columnDesc);


        if (ArrayUtils.isNotEmpty(constantValues)) {
            ExprNodeConstantDesc[] constantDescs = new ExprNodeConstantDesc[constantValues.length];
            for (int i = 0; i < constantDescs.length; i++) {
                constantDescs[i] = mock(ExprNodeConstantDesc.class);
                when(condition.getConstantDesc(i)).thenReturn(constantDescs[i]);
                when(constantDescs[i].getValue()).thenReturn(constantValues[i]);
            }
            when(condition.getConstantDescs()).thenReturn(constantDescs);
        }

        when(condition.isNot()).thenReturn(isNot);

        return condition;
    }

    @Test
    public void testBuildQueryWithCharColumns() throws IOException {
        final String COLUMN_CHAR = "Column_Char";
        final String COLUMN_VARCHAR = "Column_VChar";
        final String expectedQueryPrefix = "select /*+ NO_CACHE  */ \"" + COLUMN_CHAR + "\",\"" + COLUMN_VARCHAR +
                "\" from TEST_TABLE where ";

        JobConf jobConf = new JobConf();
        List<String> readColumnList = Lists.newArrayList(COLUMN_CHAR, COLUMN_VARCHAR);
        List<IndexSearchCondition> searchConditions = Lists.newArrayList(
                mockedIndexSearchCondition("GenericUDFOPEqual", "CHAR_VALUE", null, COLUMN_CHAR, "char(10)", false),
                mockedIndexSearchCondition("GenericUDFOPEqual", "CHAR_VALUE2", null, COLUMN_VARCHAR, "varchar(10)", false)
        );

        assertEquals(expectedQueryPrefix + "\"Column_Char\" = 'CHAR_VALUE' and \"Column_VChar\" = 'CHAR_VALUE2'",
                BUILDER.buildQuery(jobConf, TABLE_NAME, readColumnList, searchConditions));

        searchConditions = Lists.newArrayList(
                mockedIndexSearchCondition("GenericUDFIn", null,
                        new Object[]{"CHAR1", "CHAR2", "CHAR3"}, COLUMN_CHAR, "char(10)", false)
        );

        assertEquals(expectedQueryPrefix + "\"Column_Char\" in ('CHAR1', 'CHAR2', 'CHAR3')",
                BUILDER.buildQuery(jobConf, TABLE_NAME, readColumnList, searchConditions));

        searchConditions = Lists.newArrayList(
                mockedIndexSearchCondition("GenericUDFIn", null,
                        new Object[]{"CHAR1", "CHAR2", "CHAR3"}, COLUMN_CHAR, "char(10)", true)
        );

        assertEquals(expectedQueryPrefix + "\"Column_Char\" not in ('CHAR1', 'CHAR2', 'CHAR3')",
                BUILDER.buildQuery(jobConf, TABLE_NAME, readColumnList, searchConditions));

        searchConditions = Lists.newArrayList(
                mockedIndexSearchCondition("GenericUDFBetween", null,
                        new Object[]{"CHAR1", "CHAR2"}, COLUMN_CHAR, "char(10)", false)
        );

        assertEquals(expectedQueryPrefix + "\"Column_Char\" between 'CHAR1' and 'CHAR2'",
                BUILDER.buildQuery(jobConf, TABLE_NAME, readColumnList, searchConditions));

        searchConditions = Lists.newArrayList(
                mockedIndexSearchCondition("GenericUDFBetween", null,
                        new Object[]{"CHAR1", "CHAR2"}, COLUMN_CHAR, "char(10)", true)
        );

        assertEquals(expectedQueryPrefix + "\"Column_Char\" not between 'CHAR1' and 'CHAR2'",
                BUILDER.buildQuery(jobConf, TABLE_NAME, readColumnList, searchConditions));
    }

    @Test
    public void testBuildBetweenQueryWithDateColumns() throws IOException {
        final String COLUMN_DATE = "Column_Date";
        final String tableName = "TEST_TABLE";
        final String expectedQueryPrefix = "select /*+ NO_CACHE  */ \"" + COLUMN_DATE +
                "\" from " + tableName + " where ";

        JobConf jobConf = new JobConf();
        List<String> readColumnList = Lists.newArrayList(COLUMN_DATE);

        List<IndexSearchCondition> searchConditions = Lists.newArrayList(
                mockedIndexSearchCondition("GenericUDFBetween", null,
                        new Object[]{"1992-01-02", "1992-02-02"}, COLUMN_DATE, "date", false)
        );

        assertEquals(expectedQueryPrefix +
                        "\"" + COLUMN_DATE + "\" between to_date('1992-01-02') and to_date('1992-02-02')",
                BUILDER.buildQuery(jobConf, TABLE_NAME, readColumnList, searchConditions));

        searchConditions = Lists.newArrayList(
                mockedIndexSearchCondition("GenericUDFBetween", null,
                        new Object[]{"1992-01-02", "1992-02-02"}, COLUMN_DATE, "date", true)
        );

        assertEquals(expectedQueryPrefix +
                        "\"" + COLUMN_DATE + "\" not between to_date('1992-01-02') and to_date('1992-02-02')",
                BUILDER.buildQuery(jobConf, TABLE_NAME, readColumnList, searchConditions));
    }

    @Test
    public void testBuildQueryWithNotNull() throws IOException {
        final String COLUMN_DATE = "Column_Date";
        final String tableName = "TEST_TABLE";
        final String expectedQueryPrefix = "select /*+ NO_CACHE  */ \"" + COLUMN_DATE +
                "\" from " + tableName + " where ";

        JobConf jobConf = new JobConf();
        List<String> readColumnList = Lists.newArrayList(COLUMN_DATE);

        List<IndexSearchCondition> searchConditions = Lists.newArrayList(
                mockedIndexSearchCondition("GenericUDFOPNotNull", null,
                        null, COLUMN_DATE, "date", true)
        );

        assertEquals(expectedQueryPrefix +
                        "\"" + COLUMN_DATE + "\" is not null ",
                BUILDER.buildQuery(jobConf, TABLE_NAME, readColumnList, searchConditions));
    }
}
