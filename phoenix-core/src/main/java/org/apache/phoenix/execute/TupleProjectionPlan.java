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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.iterate.DelegateResultIterator;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.join.TupleProjector;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.collect.Lists;

public class TupleProjectionPlan extends DelegateQueryPlan {
    private final TupleProjector tupleProjector;
    private final Expression postFilter;

    public TupleProjectionPlan(QueryPlan plan, TupleProjector tupleProjector, Expression postFilter) {
        super(plan);
        if (tupleProjector == null) throw new IllegalArgumentException("tupleProjector is null");
        this.tupleProjector = tupleProjector;
        this.postFilter = postFilter;
    }

    @Override
    public ExplainPlan getExplainPlan() throws SQLException {
        List<String> planSteps = Lists.newArrayList(delegate.getExplainPlan().getPlanSteps());
        if (postFilter != null) {
            planSteps.add("CLIENT FILTER BY " + postFilter.toString());
        }
        
        return new ExplainPlan(planSteps);
    }

    @Override
    public ResultIterator iterator() throws SQLException {
        final ImmutableBytesWritable tempPtr = new ImmutableBytesWritable();

        return new DelegateResultIterator(delegate.iterator()) {
            
            @Override
            public Tuple next() throws SQLException {
                Tuple tuple = null;
                while (tuple == null) {
                    tuple = super.next();
                    if (tuple == null) {
                        break;
                    }
                    
                    tuple = tupleProjector.projectResults(tuple);
                    
                    if (postFilter != null) {
                        postFilter.reset();
                        try {
                            if (postFilter.evaluate(tuple, tempPtr)) {
                                Boolean b = (Boolean)postFilter.getDataType().toObject(tempPtr);
                                if (!b.booleanValue()) {
                                    tuple = null;
                                }            
                            } else {
                                tuple = null;
                            }
                        } catch (IllegalDataException e) {
                            tuple = null;
                        }
                    }
                }
                
                return tuple;
            }

            @Override
            public String toString() {
                return "TupleProjectionResultIterator [projector=" + tupleProjector + ", postFilter="
                        + postFilter + "]";
            }            
        };
    }
}
