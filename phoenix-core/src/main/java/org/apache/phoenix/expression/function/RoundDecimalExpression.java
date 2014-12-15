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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.phoenix.expression.Determinism;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.LiteralExpression;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PLong;
import org.apache.phoenix.schema.tuple.Tuple;

import com.google.common.collect.Lists;
import java.util.Collections;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.PColumn;

/**
 *
 * Class encapsulating the process for rounding off a column/literal of type
 * {@link org.apache.phoenix.schema.types.PDecimal}
 *
 *
 * @since 3.0.0
 */
public class RoundDecimalExpression extends ScalarFunction {

    private int scale;

    /**
     * Creates a {@link RoundDecimalExpression} with rounding scale given by @param scale.
     *
     */
    public static Expression create(Expression expr, int scale) throws SQLException {
        if (expr.getDataType().isCoercibleTo(PLong.INSTANCE)) {
            return expr;
        }
        Expression scaleExpr = LiteralExpression.newConstant(scale, PInteger.INSTANCE, Determinism.ALWAYS);
        List<Expression> expressions = Lists.newArrayList(expr, scaleExpr);
        return new RoundDecimalExpression(expressions);
    }

    /**
     * Creates a {@link RoundDecimalExpression} with a default scale of 0 used for rounding.
     *
     */
    public static Expression create(Expression expr) throws SQLException {
        return create(expr, 0);
    }

    public static Expression create(List<Expression> exprs) throws SQLException {
        Expression expr = exprs.get(0);
        if (expr.getDataType().isCoercibleTo(PLong.INSTANCE)) {
            return expr;
        }
        if (exprs.size() == 1) {
            Expression scaleExpr = LiteralExpression.newConstant(0, PInteger.INSTANCE, Determinism.ALWAYS);
            exprs = Lists.newArrayList(expr, scaleExpr);
        }
        return new RoundDecimalExpression(exprs);
    }

    public RoundDecimalExpression() {}

    protected RoundDecimalExpression(List<Expression> children) {
        super(children);
        LiteralExpression scaleChild = (LiteralExpression)children.get(1);
        PDataType scaleType = scaleChild.getDataType();
        Object scaleValue = scaleChild.getValue();
        if(scaleValue != null) {
            if (scaleType.isCoercibleTo(PInteger.INSTANCE, scaleValue)) {
                int scale = (Integer) PInteger.INSTANCE.toObject(scaleValue, scaleType);
                if (scale >=0 && scale <= PDataType.MAX_PRECISION) {
                    this.scale = scale;
                    return;
                }
            }
            throw new IllegalDataException("Invalid second argument for scale: " + scaleValue + ". The scale must be between 0 and " + PDataType.MAX_PRECISION + " inclusive.");
        }
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        Expression childExpr = children.get(0);
        if(childExpr.evaluate(tuple, ptr)) {
            BigDecimal value = (BigDecimal) PDecimal.INSTANCE.toObject(ptr, childExpr.getDataType(), childExpr.getSortOrder());
            BigDecimal scaledValue = value.setScale(scale, getRoundingMode());
            ptr.set(PDecimal.INSTANCE.toBytes(scaledValue));
            return true;
        }
        return false;
    }

    @Override
    public PDataType getDataType() {
        return PDecimal.INSTANCE;
    }

    protected RoundingMode getRoundingMode() {
        return RoundingMode.HALF_UP;
    }

    protected final int getRoundingScale() {
        return scale;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        super.readFields(input);
        scale = WritableUtils.readVInt(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        super.write(output);
        WritableUtils.writeVInt(output, scale);
    }

    @Override
    public String getName() {
        return RoundFunction.NAME;
    }

    @Override
    public OrderPreserving preservesOrder() {
        return OrderPreserving.YES;
    }

    @Override
    public int getKeyFormationTraversalIndex() {
        return 0;
    }

    @Override
    public KeyPart newKeyPart(final KeyPart childPart) {
        return new KeyPart() {
            private final List<Expression> extractNodes = Collections.<Expression>singletonList(RoundDecimalExpression.this);

            @Override
            public PColumn getColumn() {
                return childPart.getColumn();
            }

            @Override
            public List<Expression> getExtractNodes() {
                return extractNodes;
            }

            @Override
            public KeyRange getKeyRange(CompareFilter.CompareOp op, Expression rhs) {
                final BigDecimal rhsDecimal = (BigDecimal) PDecimal.INSTANCE.toObject(evaluateExpression(rhs));
                
                // equality requires an exact match. if rounding would cut off more precision
                // than needed for a match, it's impossible for there to be any matches
                if(op == CompareFilter.CompareOp.EQUAL && !hasEnoughPrecisionToProduce(rhsDecimal)) {
                    return KeyRange.EMPTY_RANGE;
                }
                
                // if the decimal needs to be rounded, round it such that the given 
                // operator will still be valid
                BigDecimal roundedDecimal = roundAndPreserveOperator(rhsDecimal, op);
                
                // the range of big decimals that could be rounded to produce the rounded result
                // alternatively, the "rounding bucket" that this decimal falls into
                final KeyRange equalityRange = getInputRangeProducing(roundedDecimal);
                boolean lowerInclusive = equalityRange.isLowerInclusive();
                boolean upperInclusive = equalityRange.isUpperInclusive();
                byte[] lowerRange = KeyRange.UNBOUND;
                byte[] upperRange = KeyRange.UNBOUND;
                
                switch(op) {
                    case EQUAL:
                        return equalityRange;
                    case GREATER:
                        // from the equality range and up, NOT including the equality range
                        lowerRange = equalityRange.getUpperRange();
                        lowerInclusive = !equalityRange.isUpperInclusive();
                        break;
                    case GREATER_OR_EQUAL:
                        // from the equality range and up, including the equality range
                        lowerRange = equalityRange.getLowerRange();
                        break;
                    case LESS:
                        // from the equality range and down, NOT including the equality range
                        upperRange = equalityRange.getLowerRange();
                        upperInclusive = !equalityRange.isLowerInclusive();
                        break;
                    case LESS_OR_EQUAL:
                        // from the equality range and down, including the equality range
                        upperRange = equalityRange.getUpperRange();
                        break;
                    default:
                        throw new AssertionError("Invalid CompareOp: " + op);
                }

                return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
            }
            
            /**
             * Produces a the given decimal rounded to this rounding expression's scale. If the 
             * decimal requires more scale precision to produce than this expression has, as in
             * ROUND(?, 2) &gt; 2.0098974, it ensures that the decimal is rounded such that the
             * given operator will still produce correct results.
             * @param decimal  the decimal to round with this expression's scale
             * @param op  the operator to preserve comparison with in the event of lost precision
             * @return  the rounded decimal
             */
            private BigDecimal roundAndPreserveOperator(BigDecimal decimal, CompareFilter.CompareOp op) {
                final BigDecimal rounded = roundToScale(decimal);
                
                // if we lost information, make sure that the rounding didn't break the operator
                if(!hasEnoughPrecisionToProduce(decimal)) {
                    switch(op) {
                        case GREATER_OR_EQUAL:
                            // e.g. 'ROUND(dec, 2) >= 2.013' would be converted to 
                            // 'ROUND(dec, 2) >= 2.01' but should be 'ROUND(dec, 2) >= 2.02'
                            if(decimal.compareTo(rounded) > 0) {
                                return stepNextInScale(rounded);
                            }
                            break;
                        case GREATER:
                            // e.g. 'ROUND(dec, 2) > 2.017' would be converted to 
                            // 'ROUND(dec, 2) > 2.02' but should be 'ROUND(dec, 2) > 2.01'
                            if(decimal.compareTo(rounded) < 0) {
                                return stepPrevInScale(rounded);
                            }
                            break;
                        case LESS_OR_EQUAL:
                            // e.g. 'ROUND(dec, 2) < 2.017' would be converted to 
                            // 'ROUND(dec, 2) < 2.02' but should be 'ROUND(dec, 2) < 2.01'
                            if(decimal.compareTo(rounded) < 0) {
                                return stepPrevInScale(rounded);
                            }
                            break;
                        case LESS:
                            // e.g. 'ROUND(dec, 2) <= 2.013' would be converted to 
                            // 'ROUND(dec, 2) <= 2.01' but should be 'ROUND(dec, 2) <= 2.02'
                            if(decimal.compareTo(rounded) > 0) {
                                return stepNextInScale(rounded);
                            }
                            break;
                    }
                }
                
                // otherwise, rounding has not affected the operator, so return normally
                return rounded;
            }
        };
    }
    
    /**
     * Finds the Decimal KeyRange that will produce the given result when fed into this 
     * rounding expression. For example, a ROUND expression with scale 2 will produce the
     * result "2.05" with any decimal in the range [2.045, 2.0545). 
     * The result must be pre-rounded to within this rounding expression's scale. 
     * @param result  the result to find an input range for. Must be producable.
     * @return  a KeyRange of DECIMAL keys that can be rounded by this expression to produce result
     * @throws IllegalArgumentException  if the result has more scale than this expression can produce
     */
    protected KeyRange getInputRangeProducing(BigDecimal result) {
        if(!hasEnoughPrecisionToProduce(result)) {
            throw new IllegalArgumentException("Cannot produce input range for decimal " + result 
                + ", not enough precision with scale " + getRoundingScale());
        }
        byte[] lowerRange = PDecimal.INSTANCE.toBytes(halfStepPrevInScale(result));
        byte[] upperRange = PDecimal.INSTANCE.toBytes(halfStepNextInScale(result));
        // inclusiveness changes depending on sign
        // e.g. -0.5 rounds "up" to -1 even though it is the lower boundary
        boolean lowerInclusive = result.signum() > 0;
        boolean upperInclusive = result.signum() < 0;
        return KeyRange.getKeyRange(lowerRange, lowerInclusive, upperRange, upperInclusive);
    }
    
    /**
     * Determines whether this rounding expression's scale has enough precision to produce the
     * minimum precision for the input decimal. In other words, determines whether the given
     * decimal can be rounded to this scale without losing ordering information.
     * For example, an expression with a scale of 2 has enough precision to produce "2.3", "2.71"
     * and "2.100000", but does not have enough precision to produce "2.001"
     * @param result  the decimal to round
     * @return  true if the given decimal can be precisely matched by this rounding expression
     */
    protected final boolean hasEnoughPrecisionToProduce(BigDecimal result) {
        // use compareTo so that 2.0 and 2.00 are treated as "equal"
        return roundToScale(result).compareTo(result) == 0;
    }
    
    /**
     * Returns the given decimal rounded to this rounding expression's scale.
     * For example, with scale 2 the decimal "2.453" would be rounded to either 2.45 or 
     * 2.46 depending on the rounding mode, while "2.38" and "2.7" would be unchanged.  
     * @param decimal  the decimal to round
     * @return  the rounded result decimal
     */
    protected final BigDecimal roundToScale(BigDecimal decimal) {
        return decimal.setScale(getRoundingScale(), getRoundingMode());
    }
    
    /**
     * Produces a value half of a "step" back in this expression's rounding scale.
     * For example with a scale of 2, "2.5" would be stepped back to "2.495".
     */
    protected final BigDecimal halfStepPrevInScale(BigDecimal decimal) {
        BigDecimal step = BigDecimal.ONE.scaleByPowerOfTen(-getRoundingScale());
        BigDecimal halfStep = step.divide(BigDecimal.valueOf(2));
        return decimal.subtract(halfStep);
    }
    
    /**
     * Produces a value half of a "step" forward in this expression's rounding scale.
     * For example with a scale of 2, "2.5" would be stepped forward to "2.505".
     */
    protected final BigDecimal halfStepNextInScale(BigDecimal decimal) {
        BigDecimal step = BigDecimal.ONE.scaleByPowerOfTen(-getRoundingScale());
        BigDecimal halfStep = step.divide(BigDecimal.valueOf(2));
        return decimal.add(halfStep);
    }

    /**
     * Produces a value one "step" back in this expression's rounding scale.
     * For example with a scale of 2, "2.5" would be stepped back to "2.49".
     */
    protected final BigDecimal stepPrevInScale(BigDecimal decimal) {
        BigDecimal step = BigDecimal.ONE.scaleByPowerOfTen(-getRoundingScale());
        return decimal.subtract(step);
    }

    /**
     * Produces a value one "step" forward in this expression's rounding scale.
     * For example with a scale of 2, "2.5" would be stepped forward to "2.51".
     */
    protected final BigDecimal stepNextInScale(BigDecimal decimal) {
        BigDecimal step = BigDecimal.ONE.scaleByPowerOfTen(-getRoundingScale());
        return decimal.add(step);
    }

}
