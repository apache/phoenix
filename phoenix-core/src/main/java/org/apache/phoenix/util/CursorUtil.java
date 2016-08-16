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

package org.apache.phoenix.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.compile.ColumnProjector;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.expression.function.FunctionArgumentType;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.parse.*;
import org.apache.phoenix.schema.types.*;
import org.apache.phoenix.schema.tuple.Tuple;

import java.sql.*;
import java.text.Format;
import java.util.*;

public final class CursorUtil {

    private static class CursorWrapper {
        private final int fetchSize = 1;
        private final List<String> listSelectColNames;
        private final String cursorName;
        private final String selectSQL;
        private final List<OrderByNode> orderBy;

        private boolean isOpen = false;
        //private Connection connection;
        private RowProjector rowProjector;
        //TODO: Make use of 'appendedColumnCount' to remove columns added to the query.
        //private int appendedColumnCount = 0;
        private String fetchSQL;
        //TODO: Consider refactoring these fields into List<String>
        private List<String> listRVCColNames = new ArrayList<String>();
        private Set<String> setDescRVCColNames = new HashSet<String>();
        private String strSelectColumnNames;
        private String orderByExpressionNext;
        private String orderByExpressionPrior;
        private String limitExpression;
        private String whereExpression;
        private String whereExpressionNext = "";
        private String whereExpressionPrior = "";
        private boolean selectHasPKCol = false;

        private CursorWrapper(String cursorName, String selectSQL, List<OrderByNode> orderBy){
            this.cursorName = cursorName;
            this.orderBy = orderBy;
            this.selectSQL = selectSQL;
            this.listSelectColNames = new ArrayList<String>(java.util.Arrays.asList(selectSQL
                    .substring(selectSQL.indexOf("SELECT") + 7, selectSQL.indexOf("FROM")).trim()
                    .replaceAll("[()]", "").toUpperCase().split(",")));
        }

        private synchronized void openCursor(Connection conn) throws SQLException {
            if(isOpen){
                return;
            }
            QueryPlan plan = conn.prepareStatement(selectSQL).unwrap(PhoenixPreparedStatement.class).compileQuery();
            List<String> listPKColNames = new ArrayList<String>(Arrays.asList(plan.getTableRef().getTable()
                    .getPKColumns().toString().replaceAll("[\\[ \\]]", "").toUpperCase().split(",")));
            StringBuilder whereBuilder = new StringBuilder(" WHERE (");
            StringBuilder orderByBuilder = new StringBuilder(" ORDER BY ");
            StringBuilder selectBuilder = new StringBuilder(listSelectColNames.toString().replaceAll("[\\[ \\]]", ""));
            boolean isPriorAsc = true;
            int lhsLength = 0;
            int colBinding = 0;
            for(OrderByNode node : orderBy){
                if(colBinding > 0) orderByBuilder.append(", ");
                List<String> nodeTokens = Arrays.asList(node.toString().split(" "));
                String nodeName = nodeTokens.get(0).toUpperCase();
                if(lhsLength > 0){
                    if(isPriorAsc == nodeTokens.contains("asc")){
                        whereBuilder.append(',');
                    } else {
                        //Add the RHS of the conditional
                        if(isPriorAsc){
                            whereBuilder.append(") ?> (");
                        } else{
                            whereBuilder.append(") ?< (");
                        }
                        while(lhsLength > 0){
                            whereBuilder.append(":").append(Integer.toString(colBinding-lhsLength));
                            if(lhsLength > 1){
                                whereBuilder.append(',');
                            } else{
                                whereBuilder.append(')');
                            }
                            --lhsLength;
                        }
                        whereBuilder.append(" AND (");
                    }
                }

                whereBuilder.append(nodeName);
                orderByBuilder.append(nodeName);
                ++colBinding;
                ++lhsLength;
                if(nodeTokens.contains("desc")){
                    setDescRVCColNames.add(nodeName);
                    orderByBuilder.append(" ?desc");
                    isPriorAsc = false;
                } else{
                    orderByBuilder.append(" ?asc");
                    isPriorAsc = true;
                }

                if(listPKColNames.remove(nodeName)){
                    selectHasPKCol = true;
                }
                listRVCColNames.add(nodeName);
                if(!listSelectColNames.contains("*") && !listSelectColNames.contains(nodeName)){
                    selectBuilder.append(",").append(nodeName);
                }
                //TODO: Ensure nulls last/first has no effect on RVC.

                //Address the last conditional
                if(colBinding == orderBy.size()) {
                    for (String colName : listPKColNames) {
                        whereBuilder.append(',');
                        orderByBuilder.append(',');
                        listRVCColNames.add(colName);
                        if (!listSelectColNames.contains("*") && !listSelectColNames.contains(colName)) {
                            selectBuilder.append(",").append(colName);
                        }
                        whereBuilder.append(colName);
                        orderByBuilder.append(colName);
                        if(isPriorAsc){
                            orderByBuilder.append(" ?asc");
                        } else{
                            orderByBuilder.append(" ?desc");
                        }
                        ++lhsLength;
                        ++colBinding;
                    }

                    listPKColNames.clear();

                    //Add the RHS of the conditional
                    if(isPriorAsc){
                        whereBuilder.append(") ?> (");
                    } else{
                        whereBuilder.append(") ?< (");
                    }
                    while(lhsLength > 0){
                        whereBuilder.append(":").append(Integer.toString(colBinding-lhsLength));
                        if(lhsLength > 1){
                            whereBuilder.append(',');
                        } else{
                            whereBuilder.append(')');
                            if(!selectHasPKCol && !listPKColNames.isEmpty()){
                                whereBuilder.append(" AND (");
                            } else{
                                listPKColNames.clear();
                            }
                        }
                        --lhsLength;
                    }
                }
            }

            for(String colName : listPKColNames){
                if(colBinding > 0){
                    orderByBuilder.append(',');
                }
                if(lhsLength > 0){
                    whereBuilder.append(',');
                }
                listRVCColNames.add(colName);
                if(!listSelectColNames.contains("*") && !listSelectColNames.contains(colName)){
                    selectBuilder.append(",").append(colName);
                }
                whereBuilder.append(colName);
                orderByBuilder.append(colName).append(" ?asc");
                ++lhsLength;
                ++colBinding;
            }
            if(lhsLength>0) whereBuilder.append(") ?> (");
            while(lhsLength>0){
                whereBuilder.append(":").append(colBinding-lhsLength);
                if(lhsLength > 1){
                    whereBuilder.append(',');
                } else{
                    whereBuilder.append(')');
                }
                --lhsLength;
            }
            strSelectColumnNames = selectBuilder.toString();
            whereExpression = whereBuilder.toString();
            orderByExpressionNext = orderByBuilder.toString().replaceAll("\\?asc", "asc").replaceAll("\\?desc","desc");
            orderByExpressionPrior = orderByBuilder.toString().replaceAll("\\?asc", "desc").replaceAll("\\?desc","asc");
            limitExpression = " LIMIT " + Integer.toString(fetchSize);
            fetchSQL = "SELECT * FROM ("+selectSQL.substring(0,selectSQL.indexOf(" "))+" "+strSelectColumnNames+" " + selectSQL.substring(selectSQL.indexOf("FROM"))+")";

            plan = conn.prepareStatement(fetchSQL).unwrap(PhoenixPreparedStatement.class).compileQuery();
            rowProjector = plan.getProjector();
            isOpen = true;
        }

        private void closeCursor() {
            isOpen = false;
            //TODO: Determine if the cursor should be removed from the HashMap at this point.
            //Semantically it makes sense that something which is 'Closed' one should be able to 'Open' again.
            mapCursorIDQuery.remove(this.cursorName);
        }

        private String getFetchSQL(boolean isNext) throws SQLException {
            if(!isOpen) throw new SQLException("Fetch call on closed cursor '"+this.cursorName+"'!");
            if(isNext){
                return fetchSQL + whereExpressionNext + orderByExpressionNext + limitExpression;
            } else {
                return fetchSQL + whereExpressionPrior + orderByExpressionPrior + limitExpression;
            }
        }

        private String getCursorName(){
            return this.cursorName;
        }

        private void updateSelectValues(Tuple currentRow) throws SQLException {
            if(currentRow != null){
                whereExpressionNext = whereExpression.replaceAll("\\?>", ">").replaceAll("\\?<=","<=").replaceAll("\\?<","<");
                whereExpressionPrior = whereExpression.replaceAll("\\?>", "<").replaceAll("\\?<=",">=").replaceAll("\\?<",">");
                //TODO: Evaluate if there is a better approach.
                int colBinding = 0;
                for(String colName : listRVCColNames){
                    int colIndex = rowProjector.getColumnIndex(colName);
                    ColumnProjector projector = rowProjector.getColumnProjector(colIndex);
                    PDataType type = projector.getExpression().getDataType();
                    Object value = projector.getValue(currentRow, type, new ImmutableBytesPtr());
                    if(value == null){
                        whereExpressionNext = whereExpressionNext.replaceFirst(":"+Integer.toString(colBinding),"NULL");
                        whereExpressionPrior = whereExpressionPrior.replaceFirst(":"+Integer.toString(colBinding),"NULL");
                    } else{
                        whereExpressionNext = whereExpressionNext.replaceFirst(":"+Integer.toString(colBinding),formatString(value, type));
                        whereExpressionPrior = whereExpressionPrior.replaceFirst(":"+Integer.toString(colBinding),formatString(value, type));
                    }
                    ++colBinding;
                }
                //strSelectColumnValues = strBuilder.toString();
            } else{
                //TODO: Decide if the right approach is to remove this cursor from the HashMap at this point.
                //The user could wish to step back using 'FETCH PRIOR <cursorName>'
                //this.closeCursor();
            }
        }

        private String formatString(Object value, PDataType type){
            Format format;
            String strValue;
            if(type.equals(PDate.INSTANCE) || type.equals(PUnsignedDate.INSTANCE)
                    ||  type.equals(PTime.INSTANCE) || type.equals(PUnsignedTime.INSTANCE)
                    ||  type.equals(PTimestamp.INSTANCE) || type.equals(PUnsignedTimestamp.INSTANCE)){
                format = DateUtil.getDateFormatter(DateUtil.DEFAULT_DATE_FORMAT);
                strValue = "TO_DATE('"+format.format(value)+"')";
            } else if(type.equals(PDecimal.INSTANCE) || type.equals(PLong.INSTANCE) || type.equals(PBinary.INSTANCE)
                    || type.equals(PDouble.INSTANCE) || type.equals(PTinyint.INSTANCE)){
                format = FunctionArgumentType.NUMERIC.getFormatter(NumberUtil.DEFAULT_NUMBER_FORMAT);
                strValue = "TO_NUMBER('"+format.format(value)+"')";
            }else if(type.equals(PChar.INSTANCE) || type.equals(PVarchar.INSTANCE)) {
                strValue = "'"+value.toString()+"'";
            }else {
                strValue = value.toString();
            }
            return strValue;
        }
    }

    private static Map<String, CursorWrapper> mapCursorIDQuery = new HashMap<String,CursorWrapper>();

    /**
     * Private constructor
     */
    private CursorUtil() {
    }

    /**
     *
     * @param stmt DeclareCursorStatement instance intending to declare a new cursor.
     * @return Returns true if the new cursor was successfully declared. False if a cursor with the same
     * identifier already exists.
     */
    public static boolean declareCursor(DeclareCursorStatement stmt) throws SQLException {
        if(mapCursorIDQuery.containsKey(stmt.getCursorName())){
            throw new SQLException("Can't declare cursor " + stmt.getCursorName() + ", cursor identifier already in use.");
        } else {
            mapCursorIDQuery.put(stmt.getCursorName(), new CursorWrapper(stmt.getCursorName(), stmt.getQuerySQL(), stmt.getSelectOrderBy()));
            return true;
        }
    }

    public static boolean openCursor(OpenStatement stmt, Connection conn) throws SQLException {
        if(mapCursorIDQuery.containsKey(stmt.getCursorName())){
            mapCursorIDQuery.get(stmt.getCursorName()).openCursor(conn);
            return true;
        } else{
            throw new SQLException("Cursor " + stmt.getCursorName() + " not declared.");
        }
    }

    public static void closeCursor(CloseStatement stmt) {
        if(mapCursorIDQuery.containsKey(stmt.getCursorName())){
            mapCursorIDQuery.get(stmt.getCursorName()).closeCursor();
        }
    }

    public static String getFetchSQL(String cursorName, boolean isNext) throws SQLException {
        if(mapCursorIDQuery.containsKey(cursorName)){
            return mapCursorIDQuery.get(cursorName).getFetchSQL(isNext);
        } else {
            throw new SQLException("Cursor " + cursorName + " not declared.");
        }
    }

    public static void updateCursor(String cursorName, Tuple rowValues) throws SQLException {
        mapCursorIDQuery.get(cursorName).updateSelectValues(rowValues);
    }

    public static boolean cursorDeclared(String cursorName){
        return mapCursorIDQuery.containsKey(cursorName);
    }
}