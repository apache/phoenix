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

// Remove when
//  [CALCITE-851] Add original SQL string as a field in the parser
// is fixed.
JAVACODE
public String originalSql() {
   return org.apache.phoenix.calcite.jdbc.PhoenixPrepareImpl.THREAD_SQL_STRING.get();
}

SqlNode SqlCreateView() :
{
    SqlParserPos pos;
    SqlIdentifier name;
    SqlNode query;
}
{
    <CREATE> { pos = getPos(); } <VIEW> name = CompoundIdentifier()
    <AS>
    query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY)
    {
        String sql = originalSql();
        SqlParserPos pos2 = query.getParserPosition();
        SqlParserPos pos3 = getPos();
        int start = SqlParserUtil.lineColToIndex(sql, pos2.getLineNum(), pos2.getColumnNum());
        int end = SqlParserUtil.lineColToIndex(sql, pos3.getEndLineNum(), pos3.getEndColumnNum());
        String queryString = sql.substring(start, end + 1);
        System.out.println("[" + queryString + "]");
        return new SqlCreateView(pos.plus(pos3), name, query, queryString);
    }
}

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
    tableName = CompoundIdentifier()
    <LPAREN>
    columnDefs = ColumnDefList()
    (
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
        tableOptions = TableOptionList()
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
        return new SqlCreateTable(pos, tableName,
            SqlLiteral.createBoolean(ifNotExists, SqlParserPos.ZERO),
            columnDefs, pkConstraint, pkConstraintColumnDefs,
            tableOptions, splitKeys);
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

SqlNodeList TableOptionList() :
{
    SqlParserPos pos;
    SqlNode e;
    List<SqlNode> tableOptionList;
}
{
    { pos = getPos(); }
    e = TableOption() { tableOptionList = startList(e); }
    (
        <COMMA> e = TableOption() { tableOptionList.add(e); }
    ) *
    {
        return new SqlNodeList(tableOptionList, pos.plus(getPos()));
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
    boolean isNull = true;
    Integer maxLength = null;
    Integer scale = null;
    boolean isPk = false;
    SortOrder sortOrder = SortOrder.getDefault();
    boolean isArray = false;
    Integer arrSize = null;
    String expressionStr = null;
    boolean isRowTimestamp = false;
    SqlParserPos pos;
}
{
    columnName = CompoundIdentifier()
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
        <ARRAY>
		{isArray = true;}
		[
            <LBRACKET> 
                arrSize = UnsignedIntLiteral()		
            <RBRACKET>
        ]
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
    columnName = CompoundIdentifier()
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

SqlTableOptionNode TableOption() :
{
    SqlIdentifier key;
    SqlNode value;
    SqlParserPos pos;
}
{
    key = CompoundIdentifier()
    <EQ>
    value = Literal()
    {
        pos = key.getParserPosition().plus(getPos());
        return new SqlTableOptionNode(pos, key, (SqlLiteral) value);
    }
}
