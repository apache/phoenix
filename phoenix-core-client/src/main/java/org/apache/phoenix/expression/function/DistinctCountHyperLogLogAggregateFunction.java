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
package org.apache.phoenix.expression.function;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.BaseAggregator;
import org.apache.phoenix.expression.aggregator.DistinctCountClientAggregator;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.DistinctCountHyperLogLogAggregateParseNode;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PVarbinary;
import org.apache.phoenix.util.ByteUtil;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Built-in function for Distinct Count Aggregation 
 * function in approximation. 
 * This aggregator is implemented using HyperLogLog.
 * Please refer to PHOENIX-418
 * https://issues.apache.org/jira/browse/PHOENIX-418
 * 
 * 
 * 1, Accuracy input is not a customizeable. In HyperLogLog
 * accuracy is propertional to 1/sqrt(m), m is the size of
 * the hll hash. Also, this process is irrelavent to runtime
 * or space complexity.
 * 
 * 2, The two parameters that requires during HLL initialization. 
 * i.e., the precision value for the normal set and the precision 
 * value for the sparse set, is hard coded as static final 
 * variable. Any change of them requires re-deployment of the 
 * phoenix server coprocessors.
 * 
 */
@BuiltInFunction(name=DistinctCountHyperLogLogAggregateFunction.NAME, nodeClass=DistinctCountHyperLogLogAggregateParseNode.class, args= {@Argument()} )
public class DistinctCountHyperLogLogAggregateFunction extends DistinctCountAggregateFunction {
    public static final String NAME = "APPROX_COUNT_DISTINCT";
    public static final int NormalSetPrecision = 16;
    public static final int SparseSetPrecision = 25;
    
    public DistinctCountHyperLogLogAggregateFunction() {
    }
    
    public DistinctCountHyperLogLogAggregateFunction(List<Expression> childExpressions){
        super(childExpressions, null);
    }
    
    public DistinctCountHyperLogLogAggregateFunction(List<Expression> childExpressions, CountAggregateFunction delegate){
        super(childExpressions, delegate);
    }

    @Override
    public DistinctCountClientAggregator newClientAggregator() {
    	return new HyperLogLogClientAggregator(SortOrder.getDefault());
    }
    
    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        final Expression child = getAggregatorExpression();
        return new HyperLogLogServerAggregator(child.getSortOrder()){
			@Override
			protected PDataType getInputDataType() {
				return child.getDataType();
			}
        };
    }
    
    @Override
    public Aggregator newServerAggregator(Configuration conf, ImmutableBytesWritable ptr) {
        final Expression child = getAggregatorExpression();
        return new HyperLogLogServerAggregator(child.getSortOrder(), ptr) {
          @Override
          protected PDataType getInputDataType() {
            return child.getDataType();
          }
        };
    }
   
    @Override
    public String getName() {
        return NAME;
    }
}


/**
* ClientSide HyperLogLogAggregator
* It will be called when server side aggregator has finished
* Method aggregate is called for every new server aggregator returned
* Method evaluate is called when the aggregate is done.
* the return of evaluate will be send back to user as 
* counted result of expression.evaluate
*/
class HyperLogLogClientAggregator extends DistinctCountClientAggregator{
	private HyperLogLogPlus hll = new HyperLogLogPlus(DistinctCountHyperLogLogAggregateFunction.NormalSetPrecision, DistinctCountHyperLogLogAggregateFunction.SparseSetPrecision);

	public HyperLogLogClientAggregator(SortOrder sortOrder) {
		super(sortOrder);
	}
	
	@Override
	public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
		try {
			hll.addAll(HyperLogLogPlus.Builder.build(ByteUtil.copyKeyBytesIfNecessary(ptr)));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {	
		byte[] buffer = new byte[PLong.INSTANCE.getByteSize()];
		PLong.INSTANCE.getCodec().encodeLong(hll.cardinality(), buffer, 0);
		ptr.set(buffer);
		return true;
	}
}


/**
 * ServerSide HyperLogLogAggregator
 * It will be serialized and dispatched to region server
 * Method aggregate is called for every new row scanned
 * Method evaluate is called when this remote scan is over.
 * the return of evaluate will be send back to ClientSideAggregator.aggregate 
 */
abstract class HyperLogLogServerAggregator extends BaseAggregator{
	private HyperLogLogPlus hll = new HyperLogLogPlus(DistinctCountHyperLogLogAggregateFunction.NormalSetPrecision, DistinctCountHyperLogLogAggregateFunction.SparseSetPrecision);
	protected final ImmutableBytesWritable valueByteArray = new ImmutableBytesWritable(ByteUtil.EMPTY_BYTE_ARRAY);

	public HyperLogLogServerAggregator(SortOrder sortOrder) {
		super(sortOrder);
	}
	
	public HyperLogLogServerAggregator(SortOrder sortOrder, ImmutableBytesWritable ptr) {
		this(sortOrder);
		if(ptr !=null){
			hll.offer(ptr);
		}
	}

	@Override
	public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
		hll.offer(ptr);
	}

	@Override
	public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {	
		try {
			valueByteArray.set(hll.getBytes(), 0, hll.getBytes().length);
			ptr.set(ByteUtil.copyKeyBytesIfNecessary(valueByteArray));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return true;
	}

	@Override
	public final PDataType getDataType() {
		return PVarbinary.INSTANCE;
	}
	
	abstract protected PDataType getInputDataType();
}