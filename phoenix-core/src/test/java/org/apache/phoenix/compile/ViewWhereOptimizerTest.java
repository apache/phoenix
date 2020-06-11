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

import org.apache.phoenix.parse.EqualParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.ColumnParseNode;
import org.apache.phoenix.parse.InListParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNodeFactory;
import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.parse.RowValueConstructorParseNode;
import org.apache.phoenix.schema.AmbiguousColumnException;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.schema.TableRef;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

public class ViewWhereOptimizerTest extends BaseConnectionlessQueryTest {
    protected static final ParseNodeFactory NODE_FACTORY = new ParseNodeFactory();

    private String schemaName = "A";
    private PName schemaPName = PNameFactory.newName(schemaName);

    private String tableName = "TEST_TABLE";
    private TableName tableNameT = TableName.create(schemaName, tableName);
    private PTable table = mock(PTable.class);
    private TableRef tableRef = mock(TableRef.class);

    private String viewName = "TEST_VIEW";
    private TableName viewNameT = TableName.create(schemaName, viewName);

    private String pk1Name = "KEY1";
    private String pk2Name = "KEY2";
    private String pk3Name = "KEY3";
    private PName pk1PName = PNameFactory.newName(pk1Name);
    private PName pk2PName = PNameFactory.newName(pk2Name);
    private PName pk3PName = PNameFactory.newName(pk3Name);

    private PTable view = mock(PTable.class);
    private TableRef viewRef = mock(TableRef.class);

    private StatementContext context = mock(StatementContext.class);
    private PColumn pk1 = mock(PColumn.class);
    private PColumn pk2 = mock(PColumn.class);
    private PColumn pk3 = mock(PColumn.class);

    private ParseNodeFactory parseNodeFactory = new ParseNodeFactory();

    private ColumnParseNode key1 = new ColumnParseNode(viewNameT, pk1Name);
    private ColumnParseNode key2 = new ColumnParseNode(viewNameT, pk2Name);
    private ColumnParseNode key3 = new ColumnParseNode(viewNameT, pk3Name);
    private RowValueConstructorParseNode allPKsForInClause = new RowValueConstructorParseNode(Arrays.asList(key1, key2, key3));

    @Before
    public void setUp() throws AmbiguousColumnException, ColumnNotFoundException {
        when(context.getCurrentTable()).thenReturn(viewRef);
        when(viewRef.getTable()).thenReturn(view);
        when(view.getPKColumns()).thenReturn(Arrays.asList(pk1, pk2, pk3));
        when(view.getColumnForColumnName(viewNameT.toString() + "." + pk1Name)).thenReturn(pk1);
        when(view.getColumnForColumnName(viewNameT.toString() + "." + pk2Name)).thenReturn(pk2);
        when(view.getColumnForColumnName(viewNameT.toString() + "." + pk3Name)).thenReturn(pk3);
        when(view.getColumnForColumnName(pk1Name)).thenReturn(pk1);
        when(view.getColumnForColumnName(pk2Name)).thenReturn(pk2);
        when(view.getColumnForColumnName(pk3Name)).thenReturn(pk3);
        when(view.getSchemaName()).thenReturn(schemaPName);
        when(view.getTableName()).thenReturn(PNameFactory.newName(viewName));
        when(pk1.getName()).thenReturn(pk1PName);
        when(pk2.getName()).thenReturn(pk2PName);
        when(pk3.getName()).thenReturn(pk3PName);
    }

    /**
     * Table with a composite primary key:
     * CREATE TABLE MY_TABLE (KEY1 VARCHAR, KEY2 VARCHAR, KEY3 VARCHAR CONSTRAINT MY_TABLE_PK PRIMARY KEY (KEY1,KEY2,KEY3))
     * <p>
     * and a view with pk column as view condition:
     * CRATE VIEW MY_VIEW AS  FROM SELECT * FROM MY_TABLE WHERE KEY2 = 'A'
     * <p>
     * query is an in clause in tuple by supplying pk columns partially.
     * <p>
     * SELECT * FROM MY_VIEW WHERE (KEY1, KEY3) IN ((:1,:2), (:3,:4) , (:5,:6) )
     * <p>
     * which should be converted to
     * SELECT * FROM MY_TABLE WHERE (KEY1, KEY2, KEY3) IN ((:1,'A',:2), (:3,'A',:4) , (:5,'A',:6) )
     */
    @Test
    public void testViewConditionsGetsConvertedToInCaluse() throws Exception {
        List<ParseNode> pkColumnNodes = new ArrayList<>();

        pkColumnNodes.add(key1);
        pkColumnNodes.add(key3);
        RowValueConstructorParseNode columnNamesInTheInClause = new RowValueConstructorParseNode(pkColumnNodes);
        List<ParseNode> tuplesWithValues = new ArrayList<>();
        tuplesWithValues.add(columnNamesInTheInClause);

        int paramCounter = 0;
        for (int i = 0; i < 3; i++) {
            List<ParseNode> tupleValues = new ArrayList<>();
            tupleValues.add(parseNodeFactory.bind(String.valueOf(++paramCounter)));
            tupleValues.add(parseNodeFactory.bind(String.valueOf(++paramCounter)));
            tuplesWithValues.add(new RowValueConstructorParseNode(tupleValues));
        }
        InListParseNode whereClause = NODE_FACTORY.inList(tuplesWithValues, false);
        EqualParseNode viewWhere = parseNodeFactory.equal(new ColumnParseNode(null, pk2Name), new LiteralParseNode("A"));
        ParseNode optimizedInClause = ViewWhereOptimizer.optimizedViewWhereClause(context, whereClause, viewWhere);
        List<ParseNode> children = optimizedInClause.getChildren();
        assertEquals(children.size(), 4);
        assertEquals(children.get(0), allPKsForInClause);
        assertEquals(children.get(1), new RowValueConstructorParseNode(Arrays.asList(parseNodeFactory.bind(String.valueOf(1)), new LiteralParseNode("A"), parseNodeFactory.bind(String.valueOf(2)))));
        assertEquals(children.get(2), new RowValueConstructorParseNode(Arrays.asList(parseNodeFactory.bind(String.valueOf(3)), new LiteralParseNode("A"), parseNodeFactory.bind(String.valueOf(4)))));
        assertEquals(children.get(3), new RowValueConstructorParseNode(Arrays.asList(parseNodeFactory.bind(String.valueOf(5)), new LiteralParseNode("A"), parseNodeFactory.bind(String.valueOf(6)))));
    }

    /**
     * Table with a composite primary key:
     * CREATE TABLE MY_TABLE (KEY1 VARCHAR, KEY2 VARCHAR, KEY3 VARCHAR CONSTRAINT MY_TABLE_PK PRIMARY KEY (KEY1,KEY2,KEY3))
     * <p>
     * and a view with pk column as view condition:
     * CRATE VIEW MY_VIEW AS  FROM SELECT * FROM MY_TABLE WHERE KEY1 = 'A' AND KEY3 = 'C'
     * <p>
     * query is an in clause in tuple by supplying pk columns partially.
     * <p>
     * SELECT * FROM MY_VIEW WHERE (KEY2) IN ((:1), (:2) , (:3) )
     * <p>
     * which should be converted to
     * SELECT * FROM MY_TABLE WHERE (KEY1, KEY2, KEY3) IN (('A':1,'C'), ('A',:2,'C') , ('A',:3,'C') )
     */
    @Test
    public void testMultiViewPKConditionsGetsConvertedToInClause() throws Exception {
        List<ParseNode> pkColumnNodes = new ArrayList<>();

        pkColumnNodes.add(key2);
        RowValueConstructorParseNode columnNamesInTheInClause = new RowValueConstructorParseNode(pkColumnNodes);
        List<ParseNode> tuplesWithValues = new ArrayList<>();
        tuplesWithValues.add(columnNamesInTheInClause);

        int paramCounter = 0;
        for (int i = 0; i < 3; i++) {
            List<ParseNode> tupleValues = new ArrayList<>();
            tupleValues.add(parseNodeFactory.bind(String.valueOf(++paramCounter)));
            tuplesWithValues.add(new RowValueConstructorParseNode(tupleValues));
        }
        InListParseNode whereClause = NODE_FACTORY.inList(tuplesWithValues, false);
        EqualParseNode equal1 = parseNodeFactory.equal(new ColumnParseNode(null, pk1Name), new LiteralParseNode("A"));
        EqualParseNode equal2 = parseNodeFactory.equal(new ColumnParseNode(null, pk3Name), new LiteralParseNode("C"));
        ParseNode viewWhere = parseNodeFactory.and(Arrays.asList(equal1, equal2));
        ParseNode optimizedInClause = ViewWhereOptimizer.optimizedViewWhereClause(context, whereClause, viewWhere);
        List<ParseNode> children = optimizedInClause.getChildren();
        assertEquals(children.size(), 4);
        assertEquals(children.get(0), allPKsForInClause);
        assertEquals(children.get(1), new RowValueConstructorParseNode(Arrays.asList(new LiteralParseNode("A"), parseNodeFactory.bind(String.valueOf(1)), new LiteralParseNode("C"))));
        assertEquals(children.get(2), new RowValueConstructorParseNode(Arrays.asList(new LiteralParseNode("A"), parseNodeFactory.bind(String.valueOf(2)), new LiteralParseNode("C"))));
        assertEquals(children.get(3), new RowValueConstructorParseNode(Arrays.asList(new LiteralParseNode("A"), parseNodeFactory.bind(String.valueOf(3)), new LiteralParseNode("C"))));
    }

    /**
     * Table with a composite primary key:
     * CREATE TABLE MY_TABLE (KEY1 VARCHAR, KEY2 VARCHAR, KEY3 VARCHAR CONSTRAINT MY_TABLE_PK PRIMARY KEY (KEY1,KEY2,KEY3))
     * <p>
     * and a view with pk column as view condition:
     * CRATE VIEW MY_VIEW AS  FROM SELECT * FROM MY_TABLE WHERE KEY2 = 'A'
     * <p>
     * query is an in clause in tuple by supplying all pk columns.
     * <p>
     * SELECT * FROM MY_VIEW WHERE (KEY1) IN (:1,:2,:3)
     * <p>
     * which should be converted to
     * SELECT * FROM MY_TABLE WHERE (KEY1, KEY2) IN ((:1,'A'), (:2,'A') , (:3,'A'))
     */
    @Test
    public void testOptimizesAPartialInClauseWithoutAllColumns() throws Exception {
        List<ParseNode> pkColumnNodes = new ArrayList<>();
        pkColumnNodes.add(key1);
        RowValueConstructorParseNode columnNamesInTheInClause = new RowValueConstructorParseNode(pkColumnNodes);
        List<ParseNode> tuplesWithValues = new ArrayList<>();
        tuplesWithValues.add(columnNamesInTheInClause);

        int paramCounter = 0;
        for (int i = 0; i < 3; i++) {
            List<ParseNode> tupleValues = new ArrayList<>();
            tupleValues.add(parseNodeFactory.bind(String.valueOf(++paramCounter)));
            tuplesWithValues.add(new RowValueConstructorParseNode(tupleValues));
        }
        InListParseNode whereClause = NODE_FACTORY.inList(tuplesWithValues, false);
        EqualParseNode viewWhere = parseNodeFactory.equal(new ColumnParseNode(null, pk2Name), new LiteralParseNode("A"));
        ParseNode optimizedInClause = ViewWhereOptimizer.optimizedViewWhereClause(context, whereClause, viewWhere);
        List<ParseNode> children = optimizedInClause.getChildren();
        assertEquals(children.size(), 4);
        assertEquals(children.get(0), new RowValueConstructorParseNode(Arrays.asList(key1, key2)));
        assertEquals(children.get(1), new RowValueConstructorParseNode(Arrays.asList(parseNodeFactory.bind(String.valueOf(1)), new LiteralParseNode("A"))));
        assertEquals(children.get(2), new RowValueConstructorParseNode(Arrays.asList(parseNodeFactory.bind(String.valueOf(2)), new LiteralParseNode("A"))));
        assertEquals(children.get(3), new RowValueConstructorParseNode(Arrays.asList(parseNodeFactory.bind(String.valueOf(3)), new LiteralParseNode("A"))));
    }

    /**
     * Table with a composite primary key:
     * CREATE TABLE MY_TABLE (KEY1 VARCHAR, KEY2 VARCHAR, KEY3 VARCHAR CONSTRAINT MY_TABLE_PK PRIMARY KEY (KEY1,KEY2,KEY3))
     * <p>
     * and a view with pk column as view condition:
     * CRATE VIEW MY_VIEW AS  FROM SELECT * FROM MY_TABLE WHERE KEY2 = 'A'
     * <p>
     * query is an in clause in tuple by supplying all pk columns.
     * <p>
     * SELECT * FROM MY_VIEW WHERE (KEY1,KEY2, KEY3) IN ((:1,:2,:3), (:4,:5,:6), (:7,:8,:9))
     * <p>
     * which should do no conversion
     */
    @Test
    public void testWontOptimizeIfQueryContainsAllPkColumns() throws Exception {
        List<ParseNode> pkColumnNodes = new ArrayList<>();

        pkColumnNodes.add(key1);
        pkColumnNodes.add(key2);
        pkColumnNodes.add(key3);
        RowValueConstructorParseNode columnNamesInTheInClause = new RowValueConstructorParseNode(pkColumnNodes);
        List<ParseNode> tuplesWithValues = new ArrayList<>();
        tuplesWithValues.add(columnNamesInTheInClause);

        int paramCounter = 0;
        for (int i = 0; i < 3; i++) {
            List<ParseNode> tupleValues = new ArrayList<>();
            tupleValues.add(parseNodeFactory.bind(String.valueOf(++paramCounter)));
            tupleValues.add(parseNodeFactory.bind(String.valueOf(++paramCounter)));
            tupleValues.add(parseNodeFactory.bind(String.valueOf(++paramCounter)));
            tuplesWithValues.add(new RowValueConstructorParseNode(tupleValues));
        }
        InListParseNode whereClause = NODE_FACTORY.inList(tuplesWithValues, false);
        EqualParseNode viewWhere = parseNodeFactory.equal(new ColumnParseNode(null, pk2Name), new LiteralParseNode("A"));
        ParseNode optimizedInClause = ViewWhereOptimizer.optimizedViewWhereClause(context, whereClause, viewWhere);
        assertEquals(optimizedInClause, null);
    }

    /**
     * Table with a composite primary key:
     * CREATE TABLE MY_TABLE (KEY1 VARCHAR, KEY2 VARCHAR, KEY3 VARCHAR CONSTRAINT MY_TABLE_PK PRIMARY KEY (KEY1,KEY2,KEY3))
     * <p>
     * and a view with pk column as view condition:
     * CRATE VIEW MY_VIEW AS  FROM SELECT * FROM MY_TABLE WHERE KEY2 = 'A'
     * <p>
     * query is an in clause in tuple by supplying all pk columns.
     * <p>
     * SELECT * FROM MY_VIEW WHERE (KEY2) IN (:1,:2,:3)
     * <p>
     * which should do no conversion
     */
    @Test
    public void testWontOptimizeIfConditionIncludesViewCondition() throws Exception {
        List<ParseNode> pkColumnNodes = new ArrayList<>();
        pkColumnNodes.add(key2);
        RowValueConstructorParseNode columnNamesInTheInClause = new RowValueConstructorParseNode(pkColumnNodes);
        List<ParseNode> tuplesWithValues = new ArrayList<>();
        tuplesWithValues.add(columnNamesInTheInClause);

        int paramCounter = 0;
        for (int i = 0; i < 3; i++) {
            List<ParseNode> tupleValues = new ArrayList<>();
            tupleValues.add(parseNodeFactory.bind(String.valueOf(++paramCounter)));
            tuplesWithValues.add(new RowValueConstructorParseNode(tupleValues));
        }
        InListParseNode whereClause = NODE_FACTORY.inList(tuplesWithValues, false);
        EqualParseNode viewWhere = parseNodeFactory.equal(new ColumnParseNode(null, pk2Name), new LiteralParseNode("A"));
        ParseNode optimizedInClause = ViewWhereOptimizer.optimizedViewWhereClause(context, whereClause, viewWhere);
        assertEquals(optimizedInClause, null);
    }
}
