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
	  SqlLiteral ifNotExists = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
      SqlNodeList columnDefs;
      SqlIdentifier pkConstraint = null;
      SqlNodeList pkConstraintColumnDefs = null;
      SqlNodeList tableOptionsKeyList = null;
      SqlNodeList tableOptionsValueList = null;
      SqlNodeList splitKeyList = null;
}
{
	
    <CREATE> { pos = getPos(); } <TABLE> tableName = CompoundIdentifier()
    (
    	<IF> <NOT> <EXISTS>
    	{
    		ifNotExists = SqlLiteral.createBoolean(false, getPos());
    	}
    )?
    {
    	List<SqlNode> coumnDefList = new ArrayList<SqlNode>();
    	List<SqlNode> pkConstraintList = new ArrayList<SqlNode>();
    	List<SqlNode> tableOptionKeys = new ArrayList<SqlNode>();
    	List<SqlNode> tableOptionValues = new ArrayList<SqlNode>();
    	List<SqlNode> splitKeys = new ArrayList<SqlNode>();
    	SqlNode item;
    }
    <LPAREN>
    	{
    		item = SqlColumnDefNode();
			coumnDefList.add(item);
		}
    	( 	
    		<COMMA> <CONSTRAINT> {pkConstraint = SimpleIdentifier();} <PRIMARY> <KEY>
    			<LPAREN>
    				{	item = SqlColumnDefInPkConstraintNode();pkConstraintList.add(item);}
    				( <COMMA> {item = SqlColumnDefInPkConstraintNode();pkConstraintList.add(item);} ) *
    			<RPAREN>
    		|
    		<COMMA>
    	  	{
    			item = SqlColumnDefNode();
    			coumnDefList.add(item);
    		}
    	
    	) *
    <RPAREN>
    (
		{	item = CompoundIdentifier();tableOptionKeys.add(item);} <EQ> {	item = StringLiteral();tableOptionValues.add(item);} 
		( <COMMA> {	item = CompoundIdentifier();tableOptionKeys.add(item);} <EQ> {	item = StringLiteral();tableOptionValues.add(item);} ) *
    )?
    (
    	<SPLIT> <ON>
    	<LPAREN>
			{item = StringLiteral();splitKeys.add(item);}
			( <COMMA> {item = StringLiteral();splitKeys.add(item);} ) *
    	<RPAREN>
    )?
    {
		columnDefs = new SqlNodeList(coumnDefList, pos.plusAll(coumnDefList));
		if(!pkConstraintList.isEmpty()) {
		    	pkConstraintColumnDefs =  new SqlNodeList(pkConstraintList, pos.plusAll(pkConstraintList));
		}
		if(!tableOptionKeys.isEmpty()) {
			tableOptionsKeyList = new SqlNodeList(tableOptionKeys, pos.plusAll(tableOptionKeys));
			tableOptionsValueList = new SqlNodeList(tableOptionValues, pos.plusAll(tableOptionValues));
		}
		if(!splitKeys.isEmpty()) {
			splitKeyList = new SqlNodeList(splitKeys, pos.plusAll(splitKeys));
		}
        return new SqlCreateTable(pos, tableName, ifNotExists, columnDefs, pkConstraint, pkConstraintColumnDefs, tableOptionsKeyList, tableOptionsValueList, splitKeyList);
    }
}

SqlColumnDefNode SqlColumnDefNode() :
{
    SqlIdentifier columnName;
    SqlIdentifier dataType;
    Boolean isNull = Boolean.TRUE;
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
    dataType = TypeName()
    (
    	<LPAREN>
    		{maxLength = (Integer)UnsignedNumericLiteral().getValue();}
	    	<COMMA>
        	{scale = (Integer)UnsignedNumericLiteral().getValue();}
    	<RPAREN>
    )?
    (
	    <ARRAY>
		{isArray = true;}
    	<LBRACKET> 
    		{arrSize = (Integer)UnsignedNumericLiteral().getValue();}		
	    <RBRACKET>
	)?
		(
        <NOT> <NULL>
        {isNull = false;}
        |
        <NULL>
        {isNull = true;}
	)?
	(
        <PRIMARY> <KEY>
        {isPk = true;}
	)?
	(
		<ASC>
        |
        <DESC>
        {sortOrder = SortOrder.DESC;}
	)?
	(
		<ROW_TIMESTAMP>
        {isRowTimestamp = true;}
		
	)?
    {
    	pos = columnName.getParserPosition().plus(getPos());
    	ColumnName name;    	
    	if(columnName.names.size()==2) {
    		name = new ColumnName(columnName.names.get(0), columnName.names.get(0));
    	} else{
    		name = new ColumnName(columnName.names.get(0));
    	}
    	ColumnDef cd = new ColumnDef(name, dataType.names.get(0), isArray, arrSize, isNull, maxLength, scale, isPk, sortOrder, null, isRowTimestamp);
    	return new SqlColumnDefNode(pos,cd);
    }
}

SqlColumnDefInPkConstraintNode SqlColumnDefInPkConstraintNode() :
{
    SqlIdentifier columnName;
    SortOrder sortOrder = SortOrder.getDefault();
    boolean isRowTimestamp = false;
    SqlParserPos pos;
}
{
    columnName = CompoundIdentifier()
	(
		<ASC>
        |
        <DESC>
        {sortOrder = SortOrder.DESC;}
	)?
	(
		<ROW_TIMESTAMP>
        {isRowTimestamp = true;}
		
	)?
    {
    	pos = columnName.getParserPosition().plus(getPos());
    	ColumnName name;    	
    	if(columnName.names.size()==2) {
    		name = new ColumnName(columnName.names.get(0), columnName.names.get(0));
    	} else{
    		name = new ColumnName(columnName.names.get(0));
    	}
    	ColumnDefInPkConstraint cd = new ColumnDefInPkConstraint(name, sortOrder, isRowTimestamp);
    	return new SqlColumnDefInPkConstraintNode(pos,cd);
    }
}
