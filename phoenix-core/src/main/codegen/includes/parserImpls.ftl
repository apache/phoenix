<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

<#--
  Add implementations of additional parser statements here.
  Each implementation should return an object of SqlNode type.
-->

/**
 * Parses statement
 *   COMMIT
 */
SqlNode SqlCommit() :
{
    SqlParserPos pos;
}
{
    <COMMIT> { pos = getPos(); }
    {
        return new SqlCommit(pos);
    }
}

/**
 * Parses an EXPLAIN statement.
 * Phoenix equivalent for EXPLAIN PLAN statement.
 */
SqlNode SqlPhoenixExplain() :
{
    SqlNode stmt;
    SqlExplainLevel detailLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
    SqlExplain.Depth depth = SqlExplain.Depth.PHYSICAL;
    SqlParserPos pos;
    boolean asXml = false;
}
{
    <EXPLAIN>
    stmt = SqlQueryOrDml() {
        pos = getPos();
        return new SqlExplain(pos,
            stmt,
            detailLevel.symbol(SqlParserPos.ZERO),
            depth.symbol(SqlParserPos.ZERO),
            SqlLiteral.createBoolean(asXml, SqlParserPos.ZERO),
            nDynamicParams);
    }
}

/**
 * Parses statement
 *   CREATE VIEW
 */
SqlNode SqlCreateView() :
{
    SqlParserPos pos;
    SqlIdentifier tableName;
    boolean ifNotExists;
    SqlNodeList columnDefs;
    SqlIdentifier baseTableName;
    SqlNode where;
    SqlNodeList tableOptions;
}
{
    <CREATE> { pos = getPos(); } <VIEW>
    (
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
        |
        {
            ifNotExists = false;
        }
    )
    tableName = DualIdentifier()
    (
        <LPAREN>
        columnDefs = ColumnDefList()
        <RPAREN>
        |
        {
            columnDefs = SqlNodeList.EMPTY;
        }
    )
    (
        <AS> <SELECT> <STAR> <FROM> baseTableName = DualIdentifier()
        where = WhereOpt()
        |
        {
            baseTableName = null;
            where = null;
        }
    )
    (
        tableOptions = FamilyOptionList()
        |
        {
            tableOptions = SqlNodeList.EMPTY;
        }
    )
    {
        return new SqlCreateTable(pos.plus(getPos()), tableName,
            SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
            columnDefs, baseTableName, where, tableOptions);
    }
}

/**
 * Parses statement
 *   CREATE TABLE
 */
SqlNode SqlCreateTable() :
{
    SqlParserPos pos;
    SqlIdentifier tableName;
    boolean ifNotExists;
    SqlNodeList columnDefs;
    SqlIdentifier pkConstraint;
    SqlNodeList pkConstraintColumnDefs;
    SqlNodeList tableOptions;
    SqlNodeList splitKeys;
}
{
    <CREATE> { pos = getPos(); } <TABLE>
    (
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
        |
        {
            ifNotExists = false;
        }
    )
    tableName = DualIdentifier()
    <LPAREN>
    columnDefs = ColumnDefList()
    (
    	(
    		<COMMA>
    	)?
        <CONSTRAINT> pkConstraint = SimpleIdentifier() <PRIMARY> <KEY>
        <LPAREN> pkConstraintColumnDefs = PkConstraintColumnDefList() <RPAREN>
        |
        {
            pkConstraint = null;
            pkConstraintColumnDefs = SqlNodeList.EMPTY;
        }
    )
    <RPAREN>
    (
        tableOptions = FamilyOptionList()
        |
        {
            tableOptions = SqlNodeList.EMPTY;
        }
    )
    (
        <SPLIT> <ON>
        <LPAREN> splitKeys = SplitKeyList() <RPAREN>
        |
        {
            splitKeys = SqlNodeList.EMPTY;
        }
    )
    {
        return new SqlCreateTable(pos.plus(getPos()), tableName,
            SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
            columnDefs, pkConstraint, pkConstraintColumnDefs,
            tableOptions, splitKeys);
    }
}

/**
 * Parses statement
 *   CREATE INDEX
 */
SqlNode SqlCreateIndex() :
{
    SqlParserPos pos;
    SqlIdentifier indexName;
    boolean isLocal = false;
    boolean ifNotExists = false;
    SqlIdentifier dataTableName;
    SqlNodeList expressions;
    SqlNodeList includeColumns = SqlNodeList.EMPTY;
    boolean async = false;
    SqlNodeList indexOptions = SqlNodeList.EMPTY;
    SqlNodeList splitKeys = SqlNodeList.EMPTY;
}
{
    <CREATE> { pos = getPos(); }
    [
        <LOCAL> { isLocal = true; }
    ]
    <INDEX>
    [
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
    indexName = SimpleIdentifier()
    <ON> dataTableName = DualIdentifier()
    <LPAREN>
    expressions = IndexExpressionList()
    <RPAREN>
    [
        <INCLUDE> <LPAREN>
        includeColumns = IndexIncludeList()
        <RPAREN>
    ]
    [
        <ASYNC> { async = true; }
    ]
    [
        indexOptions = FamilyOptionList()
    ]
    [
        <SPLIT> <ON> <LPAREN>
        splitKeys = SplitKeyList()
        <RPAREN>
    ]
    {
        return new SqlCreateIndex(pos.plus(getPos()), indexName,
            SqlLiteral.createBoolean(isLocal, SqlParserPos.ZERO),
            SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
            dataTableName, expressions, includeColumns,
            SqlLiteral.createBoolean(async, SqlParserPos.ZERO),
            indexOptions, splitKeys);
    }
}

/**
 * Parses statement
 *   CREATE SEQUENCE
 */
SqlNode SqlCreateSequence() :
{
    SqlParserPos pos;
    SqlIdentifier sequenceName;
    boolean ifNotExists = false;
    SqlLiteral startWith = null;
    SqlLiteral incrementBy = null;
    SqlLiteral minValue = null;
    SqlLiteral maxValue = null;
    boolean cycle = false;
    SqlLiteral cache = null;
    Integer v;
}
{
    <CREATE> { pos = getPos(); } <SEQUENCE>
    [
        <IF> <NOT> <EXISTS> { ifNotExists = true; }
    ]
    sequenceName = DualIdentifier()
    [
        <START> [ <WITH> ]
        v = UnsignedIntLiteral() { startWith = SqlLiteral.createExactNumeric(v.toString(), getPos()); }
    ]
    [
        <INCREMENT> [ <BY> ]
        v = UnsignedIntLiteral() { incrementBy = SqlLiteral.createExactNumeric(v.toString(), getPos()); }
    ]
    [
        <MINVALUE>
        v = UnsignedIntLiteral() { minValue = SqlLiteral.createExactNumeric(v.toString(), getPos()); }
    ]
    [
        <MAXVALUE>
        v = UnsignedIntLiteral() { maxValue = SqlLiteral.createExactNumeric(v.toString(), getPos()); }
    ]
    [
        <CYCLE> { cycle = true; }
    ]
    [
        <CACHE>
        v = UnsignedIntLiteral() { cache = SqlLiteral.createExactNumeric(v.toString(), getPos()); }
    ]
    {
        return new SqlCreateSequence(pos.plus(getPos()), sequenceName,
            SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
            startWith, incrementBy, minValue, maxValue,
            SqlLiteral.createBoolean(cycle, SqlParserPos.ZERO), cache);
    }
}

/**
 * Parses statement
 *   DROP TABLE
 */
SqlNode SqlDropTableOrDropView() :
{
    SqlParserPos pos;
    boolean isDropTable;
    SqlIdentifier tableName;
    boolean ifExists;
    boolean cascade;
}
{
    <DROP> { pos = getPos(); }
    (
        <TABLE> { isDropTable = true; }
        |
        <VIEW> { isDropTable = false; }
    )
    (
        <IF> <EXISTS> { ifExists = true; }
        |
        {
            ifExists = false;
        }
    )
    tableName = DualIdentifier()
    (
        <CASCADE> { cascade = true; }
        |
        {
            cascade = false;
        }
    )
    {
        return new SqlDropTable(pos.plus(getPos()), isDropTable, tableName,
            SqlLiteral.createBoolean(ifExists, SqlParserPos.ZERO),
            SqlLiteral.createBoolean(cascade, SqlParserPos.ZERO));
    }
}

/**
 * Parses statement
 *   DROP INDEX
 */
SqlNode SqlDropIndex() :
{
    SqlParserPos pos;
    SqlIdentifier indexName;
    boolean ifExists;
    SqlIdentifier dataTableName;
}
{
    <DROP> { pos = getPos(); } <INDEX>
    (
        <IF> <EXISTS> { ifExists = true; }
        |
        {
            ifExists = false;
        }
    )
    indexName = SimpleIdentifier()
    <ON>
    dataTableName = DualIdentifier()
    {
        return new SqlDropIndex(pos.plus(getPos()), indexName,
            SqlLiteral.createBoolean(ifExists, SqlParserPos.ZERO), dataTableName);
    }
}

/**
 * Parses statement
 *   DROP SEQUENCE
 */
SqlNode SqlDropSequence() :
{
    SqlParserPos pos;
    SqlIdentifier sequenceName;
    boolean ifExists;
}
{
    <DROP> { pos = getPos(); } <SEQUENCE>
    (
        <IF> <EXISTS> { ifExists = true; }
        |
        {
            ifExists = false;
        }
    )
    sequenceName = DualIdentifier()
    {
        return new SqlDropSequence(pos.plus(getPos()), sequenceName,
            SqlLiteral.createBoolean(ifExists, SqlParserPos.ZERO));
    }
}

/**
 * Parses statement
 *   UPDATE STATISTICS
 */
SqlNode SqlUpdateStatistics() :
{
    SqlParserPos pos;
    SqlIdentifier tableName;
    StatisticsCollectionScope scope;
    SqlNodeList statsOptions = SqlNodeList.EMPTY;
}
{
    <UPDATE> { pos = getPos(); } <STATISTICS>
    tableName = DualIdentifier()
    (
        <ALL> { scope = StatisticsCollectionScope.ALL; }
        |
        <INDEX> { scope = StatisticsCollectionScope.INDEX; }
        |
        <COLUMNS> { scope = StatisticsCollectionScope.COLUMNS; }
        |
        {
            scope = StatisticsCollectionScope.getDefault();
        }
    )
    [
        <SET>
        statsOptions = GeneralOptionList()
    ]
    {
        return new SqlUpdateStatistics(pos.plus(getPos()), tableName, scope, statsOptions);
    }
}

SqlNodeList ColumnDefList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> columnDefList;
}
{
    { pos = getPos(); }
    e = ColumnDef() { columnDefList = startList(e); }
    (
        <COMMA> e = ColumnDef() { columnDefList.add(e); }
    ) *
    {
        return new SqlNodeList(columnDefList, pos.plus(getPos()));
    }
}

SqlNodeList PkConstraintColumnDefList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> pkConstraintColumnDefList;
}
{
    { pos = getPos(); }
    e = ColumnDefInPkConstraint() { pkConstraintColumnDefList = startList(e); }
    (
        <COMMA> e = ColumnDefInPkConstraint() { pkConstraintColumnDefList.add(e); }
    ) *
    {
        return new SqlNodeList(pkConstraintColumnDefList, pos.plus(getPos()));
    }
}

SqlNodeList IndexExpressionList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> indexExpressionList;
}
{
    { pos = getPos(); }
    e = IndexExpression() { indexExpressionList = startList(e); }
    (
        <COMMA> e = IndexExpression() { indexExpressionList.add(e); }
    ) *
    {
        return new SqlNodeList(indexExpressionList, pos.plus(getPos()));
    }
}

SqlNodeList IndexIncludeList() :
{
    SqlParserPos pos;
    SqlIdentifier e;
    List<SqlNode> indexIncludeList;
}
{
    { pos = getPos(); }
    e = DualIdentifier() { indexIncludeList = startList(e); }
    (
        <COMMA> e = DualIdentifier() { indexIncludeList.add(e); }
    ) *
    {
        return new SqlNodeList(indexIncludeList, pos.plus(getPos()));
    }
}

SqlNodeList FamilyOptionList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> familyOptionList;
}
{
    { pos = getPos(); }
    e = FamilyOption() { familyOptionList = startList(e); }
    (
        <COMMA> e = FamilyOption() { familyOptionList.add(e); }
    ) *
    {
        return new SqlNodeList(familyOptionList, pos.plus(getPos()));
    }
}

SqlNodeList GeneralOptionList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> generalOptionList;
}
{
    { pos = getPos(); }
    e = GeneralOption() { generalOptionList = startList(e); }
    (
        <COMMA> e = GeneralOption() { generalOptionList.add(e); }
    ) *
    {
        return new SqlNodeList(generalOptionList, pos.plus(getPos()));
    }
}

SqlNodeList SplitKeyList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> splitKeyList;
}
{
    { pos = getPos(); }
    e = StringLiteral() { splitKeyList = startList(e); }
    (
        <COMMA> e = StringLiteral() { splitKeyList.add(e); }
    ) *
    {
        return new SqlNodeList(splitKeyList, pos.plus(getPos()));
    }
}

SqlColumnDefNode ColumnDef() :
{
    SqlIdentifier columnName;
    SqlDataTypeNode dataType;
    Boolean isNull = null;
    boolean isPk = false;
    SortOrder sortOrder = SortOrder.getDefault();
    boolean isRowTimestamp = false;
    SqlParserPos pos;
}
{
    columnName = DualIdentifier()
    dataType = PhoenixDataType()
    [
        <NOT> <NULL>
        {isNull = false;}
        |
        <NULL>
        {isNull = true;}
    ]
    [
        <PRIMARY> <KEY>
        {isPk = true;}
    ]
    [
        <ASC>
        {sortOrder = SortOrder.ASC;}
        |
        <DESC>
        {sortOrder = SortOrder.DESC;}
    ]
    [
        <ROW_TIMESTAMP>
        {isRowTimestamp = true;}
    ]
    {
        pos = columnName.getParserPosition().plus(getPos());
        return new SqlColumnDefNode(pos, columnName, dataType, isNull, isPk, sortOrder, null, isRowTimestamp);
    }
}

SqlDataTypeNode PhoenixDataType() :
{
    SqlIdentifier typeName;
    Integer maxLength = null;
    Integer scale = null;
    boolean isArray = false;
    Integer arrSize = null;
    SqlParserPos pos;
}
{
    typeName = TypeName()
    [
        <LPAREN>
        maxLength = UnsignedIntLiteral()
        [
            <COMMA>
            scale = UnsignedIntLiteral()
        ]
        <RPAREN>
    ]
    [
        <ARRAY> { isArray = true; }
    ]
    [
        <LBRACKET> { isArray = true; }
        [
            arrSize = UnsignedIntLiteral()
        ]	
        <RBRACKET>
    ]
    {
        pos = typeName.getParserPosition().plus(getPos());
        return new SqlDataTypeNode(pos, typeName, maxLength, scale, isArray, arrSize);
    }
}

SqlColumnDefInPkConstraintNode ColumnDefInPkConstraint() :
{
    SqlIdentifier columnName;
    SortOrder sortOrder = SortOrder.getDefault();
    boolean isRowTimestamp = false;
    SqlParserPos pos;
}
{
    columnName = DualIdentifier()
    [
        <ASC>
        {sortOrder = SortOrder.ASC;}
        |
        <DESC>
        {sortOrder = SortOrder.DESC;}
    ]
    [
        <ROW_TIMESTAMP>
        {isRowTimestamp = true;}
    ]
    {
        pos = columnName.getParserPosition().plus(getPos());
        return new SqlColumnDefInPkConstraintNode(pos, columnName, sortOrder, isRowTimestamp);
    }
}

SqlIndexExpressionNode IndexExpression() :
{
    SqlNode expression;
    SortOrder sortOrder = SortOrder.getDefault();
    SqlParserPos pos;
}
{
    expression = Expression(ExprContext.ACCEPT_NONQUERY)
    [
        <ASC>
        {sortOrder = SortOrder.ASC;}
        |
        <DESC>
        {sortOrder = SortOrder.DESC;}
    ]
    {
        pos = expression.getParserPosition().plus(getPos());
        return new SqlIndexExpressionNode(pos, expression, sortOrder);
    }
}

SqlOptionNode FamilyOption() :
{
    SqlIdentifier key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = DualIdentifier()
    <EQ>
    value = Literal()
    {
        pos = key.getParserPosition().plus(getPos());
        return new SqlOptionNode(pos, key, (SqlLiteral) value);
    }
}

SqlOptionNode GeneralOption() :
{
    SqlIdentifier key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = SimpleIdentifier()
    <EQ>
    value = Literal()
    {
        pos = key.getParserPosition().plus(getPos());
        return new SqlOptionNode(pos, key, (SqlLiteral) value);
    }
}

SqlIdentifier DualIdentifier() :
{
    List<String> list = new ArrayList<String>();
    List<SqlParserPos> posList = new ArrayList<SqlParserPos>();
    String p;
}
{
    p = Identifier()
    {
        posList.add(getPos());
        list.add(p);
    }
    [
        <DOT>
            p = Identifier() {
                list.add(p);
                posList.add(getPos());
            }
    ]
    {
        SqlParserPos pos = SqlParserPos.sum(posList);
        return new SqlIdentifier(list, null, pos, posList);
    }
}
