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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.RowKeyColumnExpression;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.PartitionIdParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PChar;
import org.apache.phoenix.schema.types.PDataType;

import java.util.List;

/**
 * Function to return the partition id which is the encoded data table region name.
 * This function is used only with CDC Indexes
 */
@BuiltInFunction(name = PartitionIdFunction.NAME,
        nodeClass= PartitionIdParseNode.class,
        args = {})
public class PartitionIdFunction extends ScalarFunction {
    public static final String NAME = "PARTITION_ID";
    public static final int PARTITION_ID_LENGTH = 32;

    public PartitionIdFunction() {
    }

    /**
     *  @param children none
     *  {@link org.apache.phoenix.parse.PartitionIdParseNode#create create}
     *  will return the partition id of a given CDC index row.
     */
    public PartitionIdFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Since partition id is part of the row key, this method is not called when PARTITION_ID()
     * is used with an IN clause.
     */
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (tuple == null) {
            return false;
        }
        tuple.getKey(ptr);
        if (ptr.getLength() < PARTITION_ID_LENGTH) {
            return false;
        }
        RowKeyColumnExpression partitionIdColumnExpression =
                (RowKeyColumnExpression) children.get(0);
        return partitionIdColumnExpression.evaluate(tuple, ptr);
    }

    @Override
    public PDataType getDataType() {
        return PChar.INSTANCE;
    }

    @Override
    public Integer getMaxLength() {
        return PARTITION_ID_LENGTH;
    }

    @Override
    public boolean isStateless() {
        return false;
    }

    @Override
    public Determinism getDeterminism() {
        return Determinism.PER_ROW;
    }
}
