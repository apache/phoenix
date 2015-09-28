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
package org.apache.phoenix.filter;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.KeyValueColumnExpression;
import org.apache.phoenix.expression.visitor.StatelessTraverseAllExpressionVisitor;
import org.apache.phoenix.expression.visitor.TraverseAllExpressionVisitor;
import org.apache.phoenix.schema.tuple.SingleKeyValueTuple;



/**
 *
 * Modeled after {@link org.apache.hadoop.hbase.filter.SingleColumnValueFilter},
 * but for general expression evaluation in the case where only a single KeyValue
 * column is referenced in the expression.
 *
 * 
 * @since 0.1
 */
public abstract class SingleKeyValueComparisonFilter extends BooleanExpressionFilter {
    private final SingleKeyValueTuple inputTuple = new SingleKeyValueTuple();
    private boolean matchedColumn;
    protected byte[] cf;
    protected byte[] cq;

    public SingleKeyValueComparisonFilter() {
    }

    public SingleKeyValueComparisonFilter(Expression expression) {
        super(expression);
        init();
    }

    protected abstract int compare(byte[] cfBuf, int cfOffset, int cfLength, byte[] cqBuf, int cqOffset, int cqLength);

    private void init() {
        TraverseAllExpressionVisitor<Void> visitor = new StatelessTraverseAllExpressionVisitor<Void>() {
            @Override
            public Void visit(KeyValueColumnExpression expression) {
                cf = expression.getColumnFamily();
                cq = expression.getColumnName();
                return null;
            }
        };
        expression.accept(visitor);
    }

    private boolean foundColumn() {
        return inputTuple.size() > 0;
    }

    @Override
    public ReturnCode filterKeyValue(Cell keyValue) {
        if (this.matchedColumn) {
          // We already found and matched the single column, all keys now pass
          // TODO: why won't this cause earlier versions of a kv to be included?
          return ReturnCode.INCLUDE;
        }
        if (this.foundColumn()) {
          // We found all the columns, but did not match the expression, so skip to next row
          return ReturnCode.NEXT_ROW;
        }
        if (compare(keyValue.getFamilyArray(), keyValue.getFamilyOffset(), keyValue.getFamilyLength(),
                keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength()) != 0) {
            // Remember the key in case this is the only key value we see.
            // We'll need it if we have row key columns too.
            inputTuple.setKey(KeyValueUtil.ensureKeyValue(keyValue));
            // This is a key value we're not interested in
            // TODO: use NEXT_COL when bug fix comes through that includes the row still
            return ReturnCode.INCLUDE;
        }
        inputTuple.setKeyValue(KeyValueUtil.ensureKeyValue(keyValue));

        // We have the columns, so evaluate here
        if (!Boolean.TRUE.equals(evaluate(inputTuple))) {
            return ReturnCode.NEXT_ROW;
        }
        this.matchedColumn = true;
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRow() {
        // If column was found, return false if it was matched, true if it was not.
        if (foundColumn()) {
            return !this.matchedColumn;
        }
        // If column was not found, evaluate the expression here upon completion.
        // This is required with certain expressions, for example, with IS NULL
        // expressions where they'll evaluate to TRUE when the column being
        // tested wasn't found.
        // Since the filter is called also to position the scan initially, we have
        // to guard against this by checking whether or not we've filtered in
        // the key value (i.e. filterKeyValue was called and we found the keyValue
        // for which we're looking).
        if (inputTuple.hasKey() && expression.requiresFinalEvaluation()) {
            return !Boolean.TRUE.equals(evaluate(inputTuple));
        }
        // Finally, if we have no values, and we're not required to re-evaluate it
        // just filter the row
        return true;
    }

      @Override
    public void reset() {
        inputTuple.reset();
        matchedColumn = false;
        super.reset();
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        init();
    }

    @SuppressWarnings("all") // suppressing missing @Override since this doesn't exist for HBase 0.94.4
    public boolean isFamilyEssential(byte[] name) {
        // Only the column families involved in the expression are essential.
        // The others are for columns projected in the select expression
        return Bytes.compareTo(cf, name) == 0;
    }
}
