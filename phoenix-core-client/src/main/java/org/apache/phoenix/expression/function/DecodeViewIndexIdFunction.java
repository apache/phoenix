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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.parse.FunctionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.DecodeViewIndexIdParseNode;
import org.apache.phoenix.parse.PhoenixRowTimestampParseNode;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.types.PSmallint;

import java.sql.Types;
import java.util.List;

import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.NULL_DATA_TYPE_VALUE;
import static org.apache.phoenix.util.ViewIndexIdRetrieveUtil.VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN;

/**
 * Function to return the timestamp of the empty column which functions as the row timestamp. The
 * result returned can be used for debugging(eg. using HBase shell), logging etc.
 * Can also be used in sql predicates.
 */
@BuiltInFunction(name = DecodeViewIndexIdFunction.NAME,
        nodeClass= DecodeViewIndexIdParseNode.class,
        args = {@FunctionParseNode.Argument(allowedTypes = { PLong.class}),
                @FunctionParseNode.Argument(allowedTypes = { PInteger.class})
        })
public class DecodeViewIndexIdFunction extends ScalarFunction {

    public static final String NAME = "DECODE_VIEW_INDEX_ID";

    public DecodeViewIndexIdFunction() {
    }

    /**
     *  @param children An EMPTY_COLUMN key value expression injected thru
     *  {@link PhoenixRowTimestampParseNode#create create}
     *  will cause the empty column key value to be evaluated during scan filter processing.
     */
    public DecodeViewIndexIdFunction(List<Expression> children) {
        super(children);

        // It takes 2 parameters - VIEW_INDEX_ID, VIEW_INDEX_ID_DATA_TYPE.
        if ((children.size() != 2) || !children.get(0).getClass().isAssignableFrom(
                KeyValueColumnExpression.class) || !children.get(1).getClass().isAssignableFrom(
                KeyValueColumnExpression.class)) {
            throw new IllegalArgumentException(
                    "DecodeViewIndexIdFunction should only have a "
                            + "VIEW_INDEX_ID and a VIEW_INDEX_ID_DATA_TYPE key value expression."
            );
        }
        if (!(children.get(0).getDataType().equals(PLong.INSTANCE))) {
            throw new IllegalArgumentException(
                    "DecodeViewIndexIdFunction should have an "
                            + "VIEW_INDEX_ID key value expression of type PLong"
            );
        }

        if (!(children.get(1).getDataType().equals(PInteger.INSTANCE))) {
            throw new IllegalArgumentException(
                    "DecodeViewIndexIdFunction should have an "
                            + "VIEW_INDEX_ID_DATA_TYPE key value expression of type PLong"
            );
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (tuple == null) {
            return false;
        }

        byte[] viewIndexIdCF = ((KeyValueColumnExpression) children.get(0)).getColumnFamily();
        byte[] viewIndexIdCQ = ((KeyValueColumnExpression) children.get(0)).getColumnQualifier();
        byte[] viewIndexIdTypeCF = ((KeyValueColumnExpression) children.get(1)).getColumnFamily();
        byte[] viewIndexIdTypeCQ = ((KeyValueColumnExpression) children.get(1)).getColumnQualifier();

        Cell viewIndexIdCell = tuple.getValue(viewIndexIdCF, viewIndexIdCQ);
        Cell viewIndexIdDataTypeCell = tuple.getValue(viewIndexIdTypeCF, viewIndexIdTypeCQ);


        /*
        This is combination of diff client created view index looks like:
            client                  VIEW_INDEX_ID(Cell number of bytes)     VIEW_INDEX_ID_DATA_TYPE
        pre-4.15                        2 bytes                                     NULL
        post-4.15[config smallint]      2 bytes                                     5(smallint)
        post-4.15[config bigint]        8 bytes                                     -5(bigint)

        VIEW_INDEX_ID_DATA_TYPE,      VIEW_INDEX_ID(Cell representation of the data)
            NULL,                         SMALLINT         -> RETRIEVE AND CONVERT TO BIGINT
            SMALLINT,                     SMALLINT         -> RETRIEVE AND CONVERT TO BIGINT
            BIGINT,                       BIGINT           -> DO NOT CONVERT

         */

        if (viewIndexIdCell != null) {
            int type = NULL_DATA_TYPE_VALUE;
            if (viewIndexIdDataTypeCell != null) {
                type = (Integer) PInteger.INSTANCE.toObject(
                        viewIndexIdDataTypeCell.getValueArray(),
                        viewIndexIdDataTypeCell.getValueOffset(),
                        viewIndexIdDataTypeCell.getValueLength(),
                        PInteger.INSTANCE,
                        SortOrder.ASC);
            }

            System.out.println("DecodeViewIndexIdFunction: Type: " + type);
            ImmutableBytesWritable columnValue =
                    new ImmutableBytesWritable(CellUtil.cloneValue(viewIndexIdCell));
            if ((type == NULL_DATA_TYPE_VALUE || type == Types.SMALLINT) && (viewIndexIdCell.getValueLength() <
                    VIEW_INDEX_ID_BIGINT_TYPE_PTR_LEN)) {
                byte[] newBytes = PLong.INSTANCE.toBytes(PSmallint.INSTANCE.toObject(columnValue.get()));
                ptr.set(newBytes, 0, newBytes.length);
            } else {
                ptr.set(columnValue.get(), columnValue.getOffset(), columnValue.getLength());
            }
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PLong.INSTANCE;
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
