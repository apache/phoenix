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
package org.apache.phoenix.join;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.ExpressionType;
import org.apache.phoenix.parse.JoinTableNode.JoinType;

public class HashJoinInfo {
    private static final String HASH_JOIN = "HashJoin";
    
    private ImmutableBytesPtr[] joinIds;
    private List<Expression>[] joinExpressions;
    private JoinType[] joinTypes;
    
    private HashJoinInfo(ImmutableBytesPtr[] joinIds, List<Expression>[] joinExpressions, JoinType[] joinTypes) {
        this.joinIds = joinIds;
        this.joinExpressions = joinExpressions;
        this.joinTypes = joinTypes;
    }
    
    public ImmutableBytesPtr[] getJoinIds() {
        return joinIds;
    }
    
    public List<Expression>[] getJoinExpressions() {
        return joinExpressions;
    }
    
    public JoinType[] getJoinTypes() {
        return joinTypes;
    }
    
    public static void serializeHashJoinIntoScan(Scan scan, HashJoinInfo joinInfo) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            DataOutputStream output = new DataOutputStream(stream);
            int count = joinInfo.joinIds.length;
            WritableUtils.writeVInt(output, count);
            for (int i = 0; i < count; i++) {
                joinInfo.joinIds[i].write(output);
                WritableUtils.writeVInt(output, joinInfo.joinExpressions[i].size());
                for (Expression expr : joinInfo.joinExpressions[i]) {
                    WritableUtils.writeVInt(output, ExpressionType.valueOf(expr).ordinal());
                    expr.write(output);
                }
                WritableUtils.writeVInt(output, joinInfo.joinTypes[i].ordinal());
            }
            scan.setAttribute(HASH_JOIN, stream.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    @SuppressWarnings("unchecked")
    public static HashJoinInfo deserializeHashJoinFromScan(Scan scan) {
        byte[] join = scan.getAttribute(HASH_JOIN);
        if (join == null) {
            return null;
        }
        ByteArrayInputStream stream = new ByteArrayInputStream(join);
        try {
            DataInputStream input = new DataInputStream(stream);
            int count = WritableUtils.readVInt(input);
            ImmutableBytesPtr[] joinIds = new ImmutableBytesPtr[count];
            List<Expression>[] joinExpressions = new List[count];
            JoinType[] joinTypes = new JoinType[count];
            for (int i = 0; i < count; i++) {
                joinIds[i] = new ImmutableBytesPtr();
                joinIds[i].readFields(input);
                int nExprs = WritableUtils.readVInt(input);
                joinExpressions[i] = new ArrayList<Expression>(nExprs);
                for (int j = 0; j < nExprs; j++) {
                    int expressionOrdinal = WritableUtils.readVInt(input);
                    Expression expression = ExpressionType.values()[expressionOrdinal].newInstance();
                    expression.readFields(input);
                    joinExpressions[i].add(expression);                    
                }
                int type = WritableUtils.readVInt(input);
                joinTypes[i] = JoinType.values()[type];
            }
            return new HashJoinInfo(joinIds, joinExpressions, joinTypes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
