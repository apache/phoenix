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
package org.apache.phoenix.execute;

import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.expression.BaseSingleExpression;
import org.apache.phoenix.expression.BaseTerminalExpression;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.visitor.ExpressionVisitor;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ParallelScanGrouper;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PArrayDataType;
import org.apache.phoenix.schema.types.PArrayDataTypeDecoder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;

public class UnnestArrayPlan extends DelegateQueryPlan {
    private final Expression arrayExpression;
    private final boolean withOrdinality;

    public UnnestArrayPlan(QueryPlan delegate, Expression arrayExpression, boolean withOrdinality) {
        super(delegate);
        this.arrayExpression = arrayExpression;
        this.withOrdinality = withOrdinality;
    }

    @Override
    public ResultIterator iterator(ParallelScanGrouper scanGrouper, Scan scan) throws SQLException {
        return new UnnestArrayResultIterator(delegate.iterator(scanGrouper, scan));
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> planSteps = delegate.getExplainPlan().getPlanSteps();
        planSteps.add("UNNEST");
        return new ExplainPlan(planSteps);
    }
    
    @Override
    public Integer getLimit() {
        return null;
    }

    public class UnnestArrayResultIterator extends DelegateResultIterator {
        private final UnnestArrayElemRefExpression elemRefExpression;
        private final UnnestArrayElemIndexExpression elemIndexExpression;
        private final TupleProjector projector;
        private Tuple current;
        private ImmutableBytesWritable arrayPtr;
        private int length;
        private int index;
        private boolean closed;

        public UnnestArrayResultIterator(ResultIterator iterator) {
            super(iterator);
            this.elemRefExpression = new UnnestArrayElemRefExpression(arrayExpression);
            this.elemIndexExpression = withOrdinality ? new UnnestArrayElemIndexExpression() : null;
            this.projector = new TupleProjector(withOrdinality ? new Expression[] {elemRefExpression, elemIndexExpression} : new Expression[] {elemRefExpression});
            this.arrayPtr = new ImmutableBytesWritable();
            this.length = 0;
            this.index = 0;
            this.closed = false;
        }

        @Override
        public Tuple next() throws SQLException {
            if (closed)
                return null;
            
            while (index >= length) {
                this.current = super.next();
                if (current == null) {
                    this.closed = true;
                    return null;
                }
                if (arrayExpression.evaluate(current, arrayPtr)) {
                    this.length = PArrayDataType.getArrayLength(arrayPtr, elemRefExpression.getDataType(), arrayExpression.getMaxLength());
                    this.index = 0;
                    this.elemRefExpression.setArrayPtr(arrayPtr);
                }
            }
            elemRefExpression.setIndex(index);
            if (elemIndexExpression != null) {
                elemIndexExpression.setIndex(index);
            }
            index++;
            return projector.projectResults(current);
        }

        @Override
        public void close() throws SQLException {
            super.close();
            closed = true;
        }
    }
    
    @SuppressWarnings("rawtypes")
    private static class UnnestArrayElemRefExpression extends BaseSingleExpression {
        private final PDataType type;
        private int index = 0;
        private ImmutableBytesWritable arrayPtr = new ImmutableBytesWritable();
        
        public UnnestArrayElemRefExpression(Expression arrayExpression) {
            super(arrayExpression);
            this.type = PDataType.fromTypeId(arrayExpression.getDataType().getSqlType() - PDataType.ARRAY_TYPE_BASE);
        }
        
        public void setIndex(int index) {
            this.index = index;
        }
        
        public void setArrayPtr(ImmutableBytesWritable arrayPtr) {
            this.arrayPtr.set(arrayPtr.get(), arrayPtr.getOffset(), arrayPtr.getLength());
        }
        
        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            ptr.set(arrayPtr.get(), arrayPtr.getOffset(), arrayPtr.getLength());
            PArrayDataTypeDecoder.positionAtArrayElement(ptr, index++, getDataType(), getMaxLength());
            return true;
        }

        @Override
        public <T> T accept(ExpressionVisitor<T> visitor) {
            // This Expression class is only used at runtime.
            return null;
        }

        @Override
        public PDataType getDataType() {
            return type;
        }
    }
    
    @SuppressWarnings("rawtypes")
    private static class UnnestArrayElemIndexExpression extends BaseTerminalExpression {
        private int index = 0;
        
        public void setIndex(int index) {
            this.index = index;
        }

        @Override
        public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
            byte[] lengthBuf = new byte[PInteger.INSTANCE.getByteSize()];
            PInteger.INSTANCE.getCodec().encodeInt(index + 1, lengthBuf, 0);
            ptr.set(lengthBuf);
            return true;
        }

        @Override
        public <T> T accept(ExpressionVisitor<T> visitor) {
            // This Expression class is only used at runtime.
            return null;
        }

        @Override
        public PDataType getDataType() {
            return PInteger.INSTANCE;
        }
    }
}
