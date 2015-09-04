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
