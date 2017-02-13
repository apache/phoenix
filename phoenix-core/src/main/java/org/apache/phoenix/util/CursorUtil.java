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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.CursorFetchPlan;
import org.apache.phoenix.iterate.CursorResultIterator;
import org.apache.phoenix.parse.CloseStatement;
import org.apache.phoenix.parse.DeclareCursorStatement;
import org.apache.phoenix.parse.OpenStatement;
import org.apache.phoenix.schema.tuple.Tuple;

public final class CursorUtil {

    private static class CursorWrapper {
        private final String cursorName;
        private final String selectSQL;
        private boolean isOpen = false;
        QueryPlan queryPlan;
        ImmutableBytesWritable row;
        ImmutableBytesWritable previousRow;
        private Scan scan;
        private boolean moreValues=true;
        private boolean isReversed;
        private boolean islastCallNext;
        private CursorFetchPlan fetchPlan;
        private int offset = -1;
        private boolean isAggregate;

        private CursorWrapper(String cursorName, String selectSQL, QueryPlan queryPlan){
            this.cursorName = cursorName;
            this.selectSQL = selectSQL;
            this.queryPlan = queryPlan;
            this.islastCallNext = true;
            this.fetchPlan = new CursorFetchPlan(queryPlan,cursorName);
            isAggregate = fetchPlan.isAggregate();
        }

        private synchronized void openCursor(Connection conn) throws SQLException {
            if(isOpen){
                return;
            }
            this.scan = this.queryPlan.getContext().getScan();
            isReversed=OrderBy.REV_ROW_KEY_ORDER_BY.equals(this.queryPlan.getOrderBy());
            isOpen = true;
        }

        private void closeCursor() throws SQLException {
            isOpen = false;
            ((CursorResultIterator) fetchPlan.iterator()).closeCursor();
            //TODO: Determine if the cursor should be removed from the HashMap at this point.
            //Semantically it makes sense that something which is 'Closed' one should be able to 'Open' again.
            mapCursorIDQuery.remove(this.cursorName);
        }

        private QueryPlan getFetchPlan(boolean isNext, int fetchSize) throws SQLException {
            if (!isOpen)
                throw new SQLException("Fetch call on closed cursor '" + this.cursorName + "'!");
            ((CursorResultIterator)fetchPlan.iterator()).setFetchSize(fetchSize);
            if (!isAggregate) { 
                if (row!=null){
                    scan.setStartRow(row.get());
                }
            }
            return this.fetchPlan;
        }

        public void updateLastScanRow(Tuple rowValues,Tuple nextRowValues) {
        	
            this.moreValues = !isReversed ? nextRowValues != null : rowValues != null;
            if(!moreValues()){
               return;
            }
            if (row == null) {
                row = new ImmutableBytesWritable();
            }
            if (previousRow == null) {
                previousRow = new ImmutableBytesWritable();
            }
            if (nextRowValues != null) {
                nextRowValues.getKey(row);
            } 
            if (rowValues != null) {
                rowValues.getKey(previousRow);
            }
            offset++;
        }

        public boolean moreValues() {
            return moreValues;
        }

        public String getFetchSQL() throws SQLException {
            if (!isOpen)
                throw new SQLException("Fetch call on closed cursor '" + this.cursorName + "'!");
            return selectSQL;
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
    public static boolean declareCursor(DeclareCursorStatement stmt, QueryPlan queryPlan) throws SQLException {
        if(mapCursorIDQuery.containsKey(stmt.getCursorName())){
            throw new SQLException("Can't declare cursor " + stmt.getCursorName() + ", cursor identifier already in use.");
        } else {
            mapCursorIDQuery.put(stmt.getCursorName(), new CursorWrapper(stmt.getCursorName(), stmt.getQuerySQL(), queryPlan));
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

    public static void closeCursor(CloseStatement stmt) throws SQLException {
        if(mapCursorIDQuery.containsKey(stmt.getCursorName())){
            mapCursorIDQuery.get(stmt.getCursorName()).closeCursor();
        }
    }

    public static QueryPlan getFetchPlan(String cursorName, boolean isNext, int fetchSize) throws SQLException {
        if(mapCursorIDQuery.containsKey(cursorName)){
            return mapCursorIDQuery.get(cursorName).getFetchPlan(isNext, fetchSize);
        } else {
            throw new SQLException("Cursor " + cursorName + " not declared.");
        }
    }
    
    public static String getFetchSQL(String cursorName) throws SQLException {
        if (mapCursorIDQuery.containsKey(cursorName)) {
            return mapCursorIDQuery.get(cursorName).getFetchSQL();
        } else {
            throw new SQLException("Cursor " + cursorName + " not declared.");
        }
    }

    public static void updateCursor(String cursorName, Tuple rowValues, Tuple nextRowValues) throws SQLException {
        mapCursorIDQuery.get(cursorName).updateLastScanRow(rowValues,nextRowValues);
    }

    public static boolean cursorDeclared(String cursorName){
        return mapCursorIDQuery.containsKey(cursorName);
    }

    public static boolean moreValues(String cursorName) {
        return mapCursorIDQuery.get(cursorName).moreValues();
    }
}
