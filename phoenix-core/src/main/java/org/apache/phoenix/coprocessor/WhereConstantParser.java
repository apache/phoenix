/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.coprocessor;

import static org.apache.phoenix.util.PhoenixRuntime.CONNECTIONLESS;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL;
import static org.apache.phoenix.util.PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.compile.ColumnResolver;
import org.apache.phoenix.compile.CreateTableCompiler;
import org.apache.phoenix.compile.ExpressionCompiler;
import org.apache.phoenix.compile.FromCompiler;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.compile.WhereCompiler;
import org.apache.phoenix.coprocessor.generated.MetaDataProtos.MutationCode;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.SQLParser;
import org.apache.phoenix.parse.SelectStatement;
import org.apache.phoenix.schema.ColumnNotFoundException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PColumnImpl;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableImpl;
import org.apache.phoenix.schema.TableRef;
import org.apache.phoenix.util.MetaDataUtil;

import com.google.common.collect.Lists;


public class WhereConstantParser {

    static Pair<PTable,MutationCode> addViewInfoToPColumnsIfNeeded(PTable view) throws SQLException {
    	boolean[] viewColumnConstantsMatched = new boolean[view.getColumns().size()];
        byte[][] viewColumnConstantsToBe = new byte[view.getColumns().size()][];
        if (view.getViewStatement() == null) {
        	return new Pair<PTable, MutationCode>(view, MutationCode.TABLE_ALREADY_EXISTS);
        }
        SelectStatement select = new SQLParser(view.getViewStatement()).parseQuery();
        ParseNode whereNode = select.getWhere();
        ColumnResolver resolver = FromCompiler.getResolver(new TableRef(view));
        StatementContext context = new StatementContext(new PhoenixStatement(getConnectionlessConnection()), resolver);
        Expression expression = null;
        try {
        	expression = WhereCompiler.compile(context, whereNode);
        }
        catch (ColumnNotFoundException e) {
        	// if we could not find a column used in the view statement (which means its was dropped)
        	// this view is not valid any more
        	return new Pair<PTable, MutationCode>(null, MutationCode.INVALID_VIEW);
        }
        CreateTableCompiler.ViewWhereExpressionVisitor visitor =
            new CreateTableCompiler.ViewWhereExpressionVisitor(view, viewColumnConstantsToBe);
        expression.accept(visitor);
        
        BitSet isViewColumnReferencedToBe = new BitSet(view.getColumns().size());
        // Used to track column references in a view
        ExpressionCompiler expressionCompiler = new CreateTableCompiler.ColumnTrackingExpressionCompiler(context, isViewColumnReferencedToBe);
        whereNode.accept(expressionCompiler);
        
        List<PColumn> result = Lists.newArrayList();
        for (PColumn column : PTableImpl.getColumnsToClone(view)) {
        	boolean isViewReferenced = isViewColumnReferencedToBe.get(column.getPosition());
        	if ( (visitor.isUpdatable() || view.getPKColumns().get(MetaDataUtil.getAutoPartitionColIndex(view)).equals(column)) 
        			&& viewColumnConstantsToBe[column.getPosition()] != null) {
				result.add(new PColumnImpl(column, viewColumnConstantsToBe[column.getPosition()], isViewReferenced));
				viewColumnConstantsMatched[column.getPosition()]=true;
            }
        	// If view is not updatable, viewColumnConstants should be empty. We will still
            // inherit our parent viewConstants, but we have no additional ones.
        	else if(isViewReferenced ){
        		result.add(new PColumnImpl(column, column.getViewConstant(), isViewReferenced));
        	}
        	else {
                result.add(column);
            }
        }
		// ensure that node of the columns in the view where statement were
		// dropped in any of this views ancestors
//		for (int i = 0; i < viewColumnConstantsMatched.length; ++i) {
//			if (viewColumnConstantsToBe[i] != null && !viewColumnConstantsMatched[i]) {
//				return null;
//			}
//		}
        view = PTableImpl.makePTable(view, result);
        return new Pair<PTable, MutationCode>(view, MutationCode.TABLE_ALREADY_EXISTS);
    }

    private static PhoenixConnection getConnectionlessConnection() throws SQLException {
        return DriverManager
            .getConnection(JDBC_PROTOCOL + JDBC_PROTOCOL_SEPARATOR + CONNECTIONLESS)
            .unwrap(PhoenixConnection.class);
    }

}
