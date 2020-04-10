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
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.parse.PhoenixRowTimestampParseNode;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Function to return the timestamp of the empty column which functions as the row timestamp. The
 * result returned can be used for debugging(eg. using HBase shell), logging etc.
 * Can also be used in sql predicates.
 */
@BuiltInFunction(name = PhoenixRowTimestampFunction.NAME,
        nodeClass= PhoenixRowTimestampParseNode.class,
        args = {})
public class PhoenixRowTimestampFunction extends ScalarFunction {
    public static final String NAME = "PHOENIX_ROW_TIMESTAMP";
    private byte[] emptyCF;
    private byte[] emptyCQ;

    public PhoenixRowTimestampFunction() {
    }

    /**
     *  {@link org.apache.phoenix.parse.PhoenixRowTimestampParseNode#create}
     *  @param children An EMPTY_COLUMN key value expression injected thru create
     *  will cause the empty column key value to be evaluated during scan filter processing.
     *  @param emptyCF empty column family
     *  @param emptyCQ empty column column
     */
    public PhoenixRowTimestampFunction(List<Expression> children, byte[] emptyCF, byte[] emptyCQ) {
        super(children);
        this.emptyCF = emptyCF;
        this.emptyCQ = emptyCQ;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * The evaluate method is called under the following conditions -
     * 1. When PHOENIX_ROW_TIMESTAMP() is evaluated in the projection list.
     *    Since the EMPTY_COLUMN is not part of the table column list,
     *    emptyColumnKV will be null.
     *    PHOENIX-4179 ensures that the maxTS (which will be EMPTY_COLUMN ts)
     *    is returned for the tuple.
     *
     * 2. When PHOENIX_ROW_TIMESTAMP() is evaluated in the backend as part of the where clause.
     *    Here the emptyColumnKV will not be null, since we ensured that by adding it to
     *    scan column list in PhoenixRowTimestampParseNode.
     *    In this case the emptyColumnKV.getTimestamp() is used.
     */
    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {

        if (tuple == null || tuple.size() == 0) {
            return false;
        }

        long ts = tuple.getValue(0).getTimestamp();
        Cell emptyColumnKV = tuple.getValue(emptyCF, emptyCQ);
        if ((emptyColumnKV != null) && CellUtil.matchingColumn(emptyColumnKV, emptyCF, emptyCQ)) {
            ts = emptyColumnKV.getTimestamp();
        }
        Date rowTimestamp = new Date(ts);
        ptr.set(PDate.INSTANCE.toBytes(rowTimestamp));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDate.INSTANCE;
    }

    @Override
    public boolean isStateless() {
        return false;
    }

    @Override
    public Determinism getDeterminism() {
        return Determinism.PER_ROW;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        int emptyCFLength = WritableUtils.readVInt(input);
        int emptyCQLength = WritableUtils.readVInt(input);
        if (emptyCFLength > 0) {
            emptyCF = new byte[emptyCFLength];
            input.readFully(emptyCF, 0, emptyCFLength);
        }
        if (emptyCQLength > 0) {
            emptyCQ = new byte[emptyCQLength];
            input.readFully(emptyCQ, 0, emptyCQLength);
        }
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        int emptyCFLength = emptyCF.length;
        int emptyCQLength = emptyCQ.length;

        WritableUtils.writeVInt(output, emptyCFLength);
        WritableUtils.writeVInt(output, emptyCQLength);
        if (emptyCFLength > 0) {
            output.write(emptyCF, 0, emptyCFLength);
        }
        if (emptyCQLength > 0) {
            output.write(emptyCQ, 0, emptyCQLength);
        }
    }
}
