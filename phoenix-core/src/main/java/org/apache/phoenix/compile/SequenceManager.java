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

import java.sql.SQLException;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.jdbc.PhoenixStatement;
import org.apache.phoenix.parse.SequenceValueParseNode;
import org.apache.phoenix.parse.SequenceValueParseNode.Op;
import org.apache.phoenix.parse.TableName;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.Sequence;
import org.apache.phoenix.schema.SequenceKey;
import org.apache.phoenix.schema.tuple.DelegateTuple;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class SequenceManager {
    private final PhoenixStatement statement;
    private int[] sequencePosition;
    private List<SequenceKey> nextSequences;
    private List<SequenceKey> currentSequences;
    private final Map<SequenceKey,SequenceValueExpression> sequenceMap = Maps.newHashMap();
    private final BitSet isNextSequence = new BitSet();
    
    public SequenceManager(PhoenixStatement statement) {
        this.statement = statement;
    }
    
    public int getSequenceCount() {
        return sequenceMap == null ? 0 : sequenceMap.size();
    }
    
    private void setSequenceValues(long[] srcSequenceValues, long[] dstSequenceValues, SQLException[] sqlExceptions) throws SQLException {
        SQLException eTop = null;
        for (int i = 0; i < sqlExceptions.length; i++) {
            SQLException e = sqlExceptions[i];
            if (e != null) {
                if (eTop == null) {
                    eTop = e;
                } else {
                    e.setNextException(eTop.getNextException());
                    eTop.setNextException(e);
                }
            } else {
                dstSequenceValues[sequencePosition[i]] = srcSequenceValues[i];
            }
        }
        if (eTop != null) {
            throw eTop;
        }
    }
    
    public Tuple newSequenceTuple(Tuple tuple) throws SQLException {
        return new SequenceTuple(tuple);
    }
    
    private class SequenceTuple extends DelegateTuple {
        private final long[] srcSequenceValues;
        private final long[] dstSequenceValues;
        private final SQLException[] sqlExceptions;
        
        public SequenceTuple(Tuple delegate) throws SQLException {
            super(delegate);
            int maxSize = sequenceMap.size();
            dstSequenceValues = new long[maxSize];
            srcSequenceValues = new long[nextSequences.size()];
            sqlExceptions = new SQLException[nextSequences.size()];
            incrementSequenceValues();
        }
        
        private void incrementSequenceValues() throws SQLException {
            if (sequenceMap == null) {
                return;
            }
            Long scn = statement.getConnection().getSCN();
            long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
            ConnectionQueryServices services = statement.getConnection().getQueryServices();
            services.incrementSequences(nextSequences, timestamp, srcSequenceValues, sqlExceptions);
            setSequenceValues(srcSequenceValues, dstSequenceValues, sqlExceptions);
            int offset = nextSequences.size();
            for (int i = 0; i < currentSequences.size(); i++) {
                dstSequenceValues[sequencePosition[offset+i]] = services.currentSequenceValue(currentSequences.get(i), timestamp);
            }
        }

        @Override
        public long getSequenceValue(int index) {
            return dstSequenceValues[index];
        }
    }

    public SequenceValueExpression newSequenceReference(SequenceValueParseNode node) {
        PName tenantName = statement.getConnection().getTenantId();
        String tenantId = tenantName == null ? null : tenantName.getString();
        TableName tableName = node.getTableName();
        int nSaltBuckets = statement.getConnection().getQueryServices().getSequenceSaltBuckets();
        SequenceKey key = new SequenceKey(tenantId, tableName.getSchemaName(), tableName.getTableName(), nSaltBuckets);
        SequenceValueExpression expression = sequenceMap.get(key);
        if (expression == null) {
            int index = sequenceMap.size();
            expression = new SequenceValueExpression(key, node.getOp(), index);
            sequenceMap.put(key, expression);
        } else if (expression.op != node.getOp()){
            expression = new SequenceValueExpression(key, node.getOp(), expression.getIndex());
        }
        // If we see a NEXT and a CURRENT, treat the CURRENT just like a NEXT
        if (node.getOp() == Op.NEXT_VALUE) {
            isNextSequence.set(expression.getIndex());
        }
           
        return expression;
    }
    
    public void validateSequences(Sequence.ValueOp action) throws SQLException {
        if (sequenceMap.isEmpty()) {
            return;
        }
        int maxSize = sequenceMap.size();
        long[] dstSequenceValues = new long[maxSize];
        sequencePosition = new int[maxSize];
        nextSequences = Lists.newArrayListWithExpectedSize(maxSize);
        currentSequences = Lists.newArrayListWithExpectedSize(maxSize);
        for (Map.Entry<SequenceKey, SequenceValueExpression> entry : sequenceMap.entrySet()) {
            if (isNextSequence.get(entry.getValue().getIndex())) {
                nextSequences.add(entry.getKey());
            } else {
                currentSequences.add(entry.getKey());
            }
        }
        long[] srcSequenceValues = new long[nextSequences.size()];
        SQLException[] sqlExceptions = new SQLException[nextSequences.size()];
        Collections.sort(nextSequences);
        // Create reverse indexes
        for (int i = 0; i < nextSequences.size(); i++) {
            sequencePosition[i] = sequenceMap.get(nextSequences.get(i)).getIndex();
        }
        int offset = nextSequences.size();
        for (int i = 0; i < currentSequences.size(); i++) {
            sequencePosition[i+offset] = sequenceMap.get(currentSequences.get(i)).getIndex();
        }
        ConnectionQueryServices services = this.statement.getConnection().getQueryServices();
        Long scn = statement.getConnection().getSCN();
        long timestamp = scn == null ? HConstants.LATEST_TIMESTAMP : scn;
        services.validateSequences(nextSequences, timestamp, srcSequenceValues, sqlExceptions, action);
        setSequenceValues(srcSequenceValues, dstSequenceValues, sqlExceptions);
    }
}
