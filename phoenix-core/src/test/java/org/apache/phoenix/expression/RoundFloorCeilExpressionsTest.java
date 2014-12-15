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
package org.apache.phoenix.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.function.CeilDateExpression;
import org.apache.phoenix.expression.function.CeilDecimalExpression;
import org.apache.phoenix.expression.function.FloorDateExpression;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.RoundDateExpression;
import org.apache.phoenix.expression.function.RoundDecimalExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.DateUtil;
import org.junit.Test;

/**
 *
 * Unit tests for {@link RoundDecimalExpression}, {@link FloorDecimalExpression}
 * and {@link CeilDecimalExpression}.
 *
 *
 * @since 3.0.0
 */
public class RoundFloorCeilExpressionsTest {

    // Decimal Expression Tests

    @Test
    public void testRoundDecimalExpression() throws Exception {
        LiteralExpression decimalLiteral = LiteralExpression.newConstant(1.23898, PDecimal.INSTANCE);
        Expression roundDecimalExpression = RoundDecimalExpression.create(decimalLiteral, 3);

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        roundDecimalExpression.evaluate(null, ptr);
        Object result = roundDecimalExpression.getDataType().toObject(ptr);

        assertTrue(result instanceof BigDecimal);
        BigDecimal resultDecimal = (BigDecimal)result;
        assertEquals(BigDecimal.valueOf(1.239), resultDecimal);
    }

    @Test
    public void testRoundDecimalExpressionNoop() throws Exception {
        LiteralExpression decimalLiteral = LiteralExpression.newConstant(5, PInteger.INSTANCE);
        Expression roundDecimalExpression = RoundDecimalExpression.create(decimalLiteral, 3);

        assertEquals(roundDecimalExpression, decimalLiteral);
    }

    @Test
    public void testFloorDecimalExpression() throws Exception {
        LiteralExpression decimalLiteral = LiteralExpression.newConstant(1.23898, PDecimal.INSTANCE);
        Expression floorDecimalExpression = FloorDecimalExpression.create(decimalLiteral, 3);

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        floorDecimalExpression.evaluate(null, ptr);
        Object result = floorDecimalExpression.getDataType().toObject(ptr);

        assertTrue(result instanceof BigDecimal);
        BigDecimal resultDecimal = (BigDecimal)result;
        assertEquals(BigDecimal.valueOf(1.238), resultDecimal);
    }

    @Test
    public void testFloorDecimalExpressionNoop() throws Exception {
        LiteralExpression decimalLiteral = LiteralExpression.newConstant(5, PInteger.INSTANCE);
        Expression floorDecimalExpression = FloorDecimalExpression.create(decimalLiteral, 3);

        assertEquals(floorDecimalExpression, decimalLiteral);
    }

    @Test
    public void testCeilDecimalExpression() throws Exception {
        LiteralExpression decimalLiteral = LiteralExpression.newConstant(1.23898, PDecimal.INSTANCE);
        Expression ceilDecimalExpression = CeilDecimalExpression.create(decimalLiteral, 3);

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ceilDecimalExpression.evaluate(null, ptr);
        Object result = ceilDecimalExpression.getDataType().toObject(ptr);

        assertTrue(result instanceof BigDecimal);
        BigDecimal resultDecimal = (BigDecimal)result;
        assertEquals(BigDecimal.valueOf(1.239), resultDecimal);
    }

    @Test
    public void testCeilDecimalExpressionNoop() throws Exception {
        LiteralExpression decimalLiteral = LiteralExpression.newConstant(5, PInteger.INSTANCE);
        Expression ceilDecimalExpression = CeilDecimalExpression.create(decimalLiteral, 3);

        assertEquals(ceilDecimalExpression, decimalLiteral);
    }

    @Test
    public void testRoundDecimalExpressionScaleParamValidation() throws Exception {
        LiteralExpression decimalLiteral = LiteralExpression.newConstant(1.23898, PDecimal.INSTANCE);
        LiteralExpression scale = LiteralExpression.newConstant("3", PVarchar.INSTANCE);

        List<Expression> childExpressions = new ArrayList<Expression>(2);
        childExpressions.add(decimalLiteral);
        childExpressions.add(scale);

        try {
            RoundDecimalExpression.create(childExpressions);
            fail("Evaluation should have failed because only an INTEGER is allowed for second param in a RoundDecimalExpression");
        } catch(IllegalDataException e) {

        }
    }

    // KeyRange explicit simple / sanity tests

    @Test
    public void testRoundDecimalExpressionKeyRangeSimple() throws Exception {
        ScalarFunction roundDecimalExpression = (ScalarFunction)RoundDecimalExpression.create(DUMMY_DECIMAL, 3);

        byte[] upperBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.2385"));
        byte[] lowerBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.2375"));
        KeyRange expectedKeyRange = KeyRange.getKeyRange(lowerBound, upperBound);

        KeyPart keyPart = roundDecimalExpression.newKeyPart(null);
        assertEquals(expectedKeyRange, keyPart.getKeyRange(CompareOp.EQUAL, LiteralExpression.newConstant(new BigDecimal("1.238"), PDecimal.INSTANCE)));
    }

    @Test
    public void testFloorDecimalExpressionKeyRangeSimple() throws Exception {
        ScalarFunction floorDecimalExpression = (ScalarFunction)FloorDecimalExpression.create(DUMMY_DECIMAL, 3);

        byte[] upperBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.239"));
        byte[] lowerBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.238"));
        KeyRange expectedKeyRange = KeyRange.getKeyRange(lowerBound, true, upperBound, false);

        KeyPart keyPart = floorDecimalExpression.newKeyPart(null);
        assertEquals(expectedKeyRange, keyPart.getKeyRange(CompareOp.EQUAL, LiteralExpression.newConstant(new BigDecimal("1.238"), PDecimal.INSTANCE)));
    }

    @Test
    public void testCeilDecimalExpressionKeyRangeSimple() throws Exception {
        ScalarFunction ceilDecimalExpression = (ScalarFunction)CeilDecimalExpression.create(DUMMY_DECIMAL, 3);

        byte[] upperBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.238"));
        byte[] lowerBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.237"));
        KeyRange expectedKeyRange = KeyRange.getKeyRange(lowerBound, false, upperBound, true);

        KeyPart keyPart = ceilDecimalExpression.newKeyPart(null);
        assertEquals(expectedKeyRange, keyPart.getKeyRange(CompareOp.EQUAL, LiteralExpression.newConstant(new BigDecimal("1.238"), PDecimal.INSTANCE)));
    }

    // KeyRange complex / generated tests

    @Test
    public void testRoundDecimalExpressionKeyRangeCoverage() throws Exception {
        for(int scale : SCALES) {
            ScalarFunction roundDecimalExpression = (ScalarFunction) RoundDecimalExpression.create(DUMMY_DECIMAL, scale);
            KeyPart keyPart = roundDecimalExpression.newKeyPart(null);
            verifyKeyPart(RoundingType.ROUND, scale, keyPart);
        }
    }

    @Test
    public void testFloorDecimalExpressionKeyRangeCoverage() throws Exception {
        for(int scale : SCALES) {
            ScalarFunction floorDecimalExpression = (ScalarFunction) FloorDecimalExpression.create(DUMMY_DECIMAL, scale);
            KeyPart keyPart = floorDecimalExpression.newKeyPart(null);
            verifyKeyPart(RoundingType.FLOOR, scale, keyPart);
        }
    }

    @Test
    public void testCeilDecimalExpressionKeyRangeCoverage() throws Exception {
        for(int scale : SCALES) {
            ScalarFunction ceilDecimalExpression = (ScalarFunction) CeilDecimalExpression.create(DUMMY_DECIMAL, scale);
            KeyPart keyPart = ceilDecimalExpression.newKeyPart(null);
            verifyKeyPart(RoundingType.CEIL, scale, keyPart);
        }
    }

    /**
     * Represents the three different types of rounding expression and produces
     * expressions of their type when given a Decimal key and scale.
     */
    private static enum RoundingType {
        ROUND("ROUND"),
        FLOOR("FLOOR"),
        CEIL("CEIL");

        public final String name;

        RoundingType(String name) {
            this.name = name;
        }

        /**
         * Returns a rounding expression of this type that will round the given decimal key at the
         * given scale.
         * @param key  the byte key for the Decimal to round
         * @param scale  the scale to round the decimal to
         * @return  the expression containing the above parameters
         */
        public Expression getExpression(byte[] key, int scale) throws SQLException {
            LiteralExpression decimalLiteral = LiteralExpression.newConstant(PDecimal.INSTANCE.toObject(key), PDecimal.INSTANCE);
            switch(this) {
                case ROUND:
                    return RoundDecimalExpression.create(decimalLiteral, scale);
                case FLOOR:
                    return FloorDecimalExpression.create(decimalLiteral, scale);
                case CEIL:
                    return CeilDecimalExpression.create(decimalLiteral, scale);
                default:
                    throw new AssertionError("Unknown RoundingType");
            }
        }
    }

    /**
     * Represents a possible relational operator used in rounding expression where clauses.
     * Includes information not kept by CompareFilter.CompareOp, including a string symbol
     * representation and a method for actually comparing comparables.
     */
    private static enum Relation {
        EQUAL(CompareOp.EQUAL, "="),
        GREATER(CompareOp.GREATER, ">"),
        GREATER_OR_EQUAL(CompareOp.GREATER_OR_EQUAL, ">="),
        LESS(CompareOp.LESS, "<"),
        LESS_OR_EQUAL(CompareOp.LESS_OR_EQUAL, "<=");

        public final CompareOp compareOp;
        public final String symbol;

        Relation(CompareOp compareOp, String symbol) {
            this.compareOp = compareOp;
            this.symbol = symbol;
        }

        public <E extends Comparable<? super E>> boolean compare(E lhs, E rhs) {
            int comparison = lhs.compareTo(rhs);
            switch(this) {
                case EQUAL:
                    return comparison == 0;
                case GREATER_OR_EQUAL:
                    return comparison >= 0;
                case GREATER:
                    return comparison > 0;
                case LESS_OR_EQUAL:
                    return comparison <= 0;
                case LESS:
                    return comparison < 0;
                default:
                    throw new AssertionError("Unknown RelationType");
            }
        }
    }

    /**
     * Produces a string error message containing the given information, formatted like a where
     * clause. <br>
     * Example Output: <br>
     * 'where ROUND(?, 2) &lt;= 2.55' (produced range: [2.545, 2.555) )
     * @param exprType
     * @param scale
     * @param relation
     * @param rhs
     * @param range
     * @return
     */
    private static String getMessage(RoundingType exprType, int scale, Relation relation, BigDecimal rhs, KeyRange range) {
        String where = exprType.name + "(?, " + scale + ") " + relation.symbol + " " + rhs;
        return "'where " + where + "' (produced range: " + formatDecimalKeyRange(range) + " )";
    }

    /**
     * Interpreting the KeyRange as a range of decimal, produces a nicely formatted string
     * representation.
     * @param range  the KeyRange to format
     * @return  the string representation, e.g. [2.45, 2.55)
     */
    private static String formatDecimalKeyRange(KeyRange range) {
        return (range.isLowerInclusive() ? "[" : "(")
            + (range.lowerUnbound() ? "*" : PDecimal.INSTANCE.toObject(range.getLowerRange()))
            + ", "
            + (range.upperUnbound() ? "*" : PDecimal.INSTANCE.toObject(range.getUpperRange()))
            + (range.isUpperInclusive() ? "]" : ")");
    }

    // create methods need a dummy expression that is not coercible to to a long
    // value doesn't matter because we only use those expressions to produce a keypart
    private static final LiteralExpression DUMMY_DECIMAL = LiteralExpression.newConstant(new BigDecimal("2.5"));

    private static final List<BigDecimal> DECIMALS = Collections.unmodifiableList(
        Arrays.asList(
            BigDecimal.valueOf(Long.MIN_VALUE * 17L - 13L, 9),
            BigDecimal.valueOf(Long.MIN_VALUE, 8),
            new BigDecimal("-200300"),
            new BigDecimal("-8.44"),
            new BigDecimal("-2.00"),
            new BigDecimal("-0.6"),
            new BigDecimal("-0.00032"),
            BigDecimal.ZERO,
            BigDecimal.ONE,
            new BigDecimal("0.00000984"),
            new BigDecimal("0.74"),
            new BigDecimal("2.00"),
            new BigDecimal("7.09"),
            new BigDecimal("84900800"),
            BigDecimal.valueOf(Long.MAX_VALUE, 8),
            BigDecimal.valueOf(Long.MAX_VALUE * 31L + 17L, 7)
        ));

    private static final List<Integer> SCALES = Collections.unmodifiableList(Arrays.asList(0, 1, 2, 3, 8));

    /**
     * Checks that a given KeyPart produces the right key ranges for each relational operator and
     * a variety of right-hand-side decimals.
     * @param exprType  the rounding expression type used to create this KeyPart
     * @param scale  the scale used to create this KeyPart
     * @param keyPart  the KeyPart to test
     */
    private void verifyKeyPart(RoundingType exprType, int scale, KeyPart keyPart) throws SQLException {
        for(BigDecimal rhsDecimal : DECIMALS) {
            LiteralExpression rhsExpression = LiteralExpression.newConstant(rhsDecimal, PDecimal.INSTANCE);
            for(Relation relation : Relation.values()) {
                KeyRange keyRange = keyPart.getKeyRange(relation.compareOp, rhsExpression);
                verifyKeyRange(exprType, scale, relation, rhsDecimal, keyRange);
            }
        }
    }

    /**
     * Checks that a given KeyRange's boundaries match with the given rounding expression type,
     * rounding scale, relational operator, and right hand side decimal.
     * Does so by checking the decimal values immediately on either side of the KeyRange border and
     * verifying that they either match or do not match the "where clause" formed by the
     * rounding type, scale, relation, and rhs decimal. If a relation should produce an unbounded
     * upper or lower range, verifies that that end of the range is unbounded. Finally, if the
     * range is empty, verifies that the rhs decimal required more precision than could be
     * produced by the rounding expression.
     * @param exprType  the rounding expression type used to create this KeyRange
     * @param scale  the rounding scale used to create this KeyRange
     * @param relation  the relational operator used to create this KeyRange
     * @param rhs  the right hand side decimal used to create this KeyRange
     * @param range  the KeyRange to test
     */
    private void verifyKeyRange(RoundingType exprType, int scale, Relation relation, BigDecimal rhs, KeyRange range) throws SQLException {
        // dump of values for debugging
        final String dump = getMessage(exprType, scale, relation, rhs, range);

        ImmutableBytesPtr rhsPtr = new ImmutableBytesPtr();
        LiteralExpression.newConstant(rhs, PDecimal.INSTANCE).evaluate(null, rhsPtr);

        ImmutableBytesPtr lhsPtr = new ImmutableBytesPtr();

        // we should only get an empty range if we can verify that precision makes a match impossible
        if(range == KeyRange.EMPTY_RANGE) {
            assertTrue("should only get empty key range for unmatchable rhs precision (" + dump + ")", rhs.scale() > scale);
            assertEquals("should only get empty key range for equals checks (" + dump + ")", Relation.EQUAL, relation);
            return;
        }

        // if it should have an upper bound
        if(relation != Relation.GREATER && relation != Relation.GREATER_OR_EQUAL) {
            // figure out what the upper bound is
            byte[] highestHighIncluded;
            byte[] lowestHighExcluded;
            if(range.isUpperInclusive()) {
                highestHighIncluded = range.getUpperRange();
                lowestHighExcluded = nextDecimalKey(range.getUpperRange());
            }
            else {
                highestHighIncluded = prevDecimalKey(range.getUpperRange());
                lowestHighExcluded = range.getUpperRange();
            }

            // check on either side of the boundary to validate that it is in fact the boundary
            exprType.getExpression(highestHighIncluded, scale).evaluate(null, lhsPtr);
            assertTrue("incorrectly excluding " + PDecimal.INSTANCE.toObject(highestHighIncluded)
                + " in upper bound for " + dump, relation.compare(lhsPtr, rhsPtr));
            exprType.getExpression(lowestHighExcluded, scale).evaluate(null, lhsPtr);
            assertFalse("incorrectly including " + PDecimal.INSTANCE.toObject(lowestHighExcluded)
                + " in upper bound for " + dump, relation.compare(lhsPtr, rhsPtr));
        }
        else {
            // otherwise verify that it does not have an upper bound
            assertTrue("should not have a upper bound for " + dump, range.upperUnbound());
        }

        // if it should have a lower bound
        if(relation != Relation.LESS && relation != Relation.LESS_OR_EQUAL) {
            // figure out what the lower bound is
            byte[] lowestLowIncluded;
            byte[] highestLowExcluded;
            if(range.isLowerInclusive()) {
                lowestLowIncluded = range.getLowerRange();
                highestLowExcluded = prevDecimalKey(range.getLowerRange());
            }
            else {
                lowestLowIncluded = nextDecimalKey(range.getLowerRange());
                highestLowExcluded = range.getLowerRange();
            }

            // check on either side of the boundary to validate that it is in fact the boundary
            exprType.getExpression(lowestLowIncluded, scale).evaluate(null, lhsPtr);
            assertTrue("incorrectly excluding " + PDecimal.INSTANCE.toObject(lowestLowIncluded)
                + " in lower bound for " + dump, relation.compare(lhsPtr, rhsPtr));
            exprType.getExpression(highestLowExcluded, scale).evaluate(null, lhsPtr);
            assertFalse("incorrectly including " + PDecimal.INSTANCE.toObject(highestLowExcluded)
                + " in lower bound for " + dump, relation.compare(lhsPtr, rhsPtr));
        }
        else {
            // otherwise verify that it does not have a lower bound
            assertTrue("should not have a lower bound for " + dump, range.lowerUnbound());
        }
    }

    /**
     * Produces the previous Decimal key relative to the given key. The new key will differ from
     * the old key in as small a unit as possible while still maintaining accurate serialization.
     * @param key  bytes for the old Decimal key
     * @return  bytes for the new Decimal key, a single unit previous to the old one
     */
    private static byte[] prevDecimalKey(byte[] key) {
        BigDecimal decimal = (BigDecimal) PDecimal.INSTANCE.toObject(key);
        BigDecimal prev = decimal.subtract(getSmallestUnit(decimal));
        return PDecimal.INSTANCE.toBytes(prev);
    }

    /**
     * Produces the next Decimal key relative to the given key. The new key will differ from the
     * old key in as small a unit as possible while still maintaining accurate serialization.
     * @param key  bytes for the old Decimal key
     * @return  bytes for the new Decimal key, a single unit next from the old one
     */
    private static byte[] nextDecimalKey(byte[] key) {
        BigDecimal decimal = (BigDecimal) PDecimal.INSTANCE.toObject(key);
        BigDecimal next = decimal.add(getSmallestUnit(decimal));
        return PDecimal.INSTANCE.toBytes(next);
    }

    /**
     * Produces the smallest unit of difference possible for the given decimal that will still
     * be serialized accurately. For example, if the MAXIMUM_RELIABLE_PRECISION were 4, then
     * getSmallestUnit(2.3) would produce 0.001, as 2.301 could be serialized accurately but
     * 2.3001 could not.
     * @param decimal  the decimal to find the smallest unit in relation to
     * @return  the smallest BigDecimal unit possible to add to decimal while still maintaining
     *          accurate serialization
     * @throws IllegalArgumentException if decimal requires more than the maximum reliable precision
     */
    private static BigDecimal getSmallestUnit(BigDecimal decimal) {
        if (decimal.precision() > PDataType.MAX_PRECISION) {
            throw new IllegalArgumentException("rounding errors mean that we cannot reliably test " + decimal);
        }
        int minScale = decimal.scale() + (PDataType.MAX_PRECISION - decimal.precision());
        return BigDecimal.valueOf(1, minScale);
    }
    
    // Date Expression Tests
    
    @Test
    public void testRoundDateExpression() throws Exception {
        LiteralExpression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDate.INSTANCE);
        Expression roundDateExpression = RoundDateExpression.create(dateLiteral, TimeUnit.DAY);
        
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        roundDateExpression.evaluate(null, ptr);
        Object result = roundDateExpression.getDataType().toObject(ptr);
        
        assertTrue(result instanceof Date);
        Date resultDate = (Date)result;
        assertEquals(DateUtil.parseDate("2012-01-02 00:00:00"), resultDate);
    }
    
    @Test
    public void testRoundDateExpressionWithMultiplier() throws Exception {
        Expression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDate.INSTANCE);
        Expression roundDateExpression = RoundDateExpression.create(dateLiteral, TimeUnit.MINUTE, 10);
        
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        roundDateExpression.evaluate(null, ptr);
        Object result = roundDateExpression.getDataType().toObject(ptr);
        
        assertTrue(result instanceof Date);
        Date resultDate = (Date)result;
        assertEquals(DateUtil.parseDate("2012-01-01 14:30:00"), resultDate);
    }
    
    @Test
    public void testFloorDateExpression() throws Exception {
        LiteralExpression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDate.INSTANCE);
        Expression floorDateExpression = FloorDateExpression.create(dateLiteral, TimeUnit.DAY);
        
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        floorDateExpression.evaluate(null, ptr);
        Object result = floorDateExpression.getDataType().toObject(ptr);
        
        assertTrue(result instanceof Date);
        Date resultDate = (Date)result;
        assertEquals(DateUtil.parseDate("2012-01-01 00:00:00"), resultDate);
    }
    
    @Test
    public void testFloorDateExpressionWithMultiplier() throws Exception {
        Expression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDate.INSTANCE);
        Expression floorDateExpression = FloorDateExpression.create(dateLiteral, TimeUnit.SECOND, 10);
        
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        floorDateExpression.evaluate(null, ptr);
        Object result = floorDateExpression.getDataType().toObject(ptr);
        
        assertTrue(result instanceof Date);
        Date resultDate = (Date)result;
        assertEquals(DateUtil.parseDate("2012-01-01 14:25:20"), resultDate);
    }
    
    @Test
    public void testCeilDateExpression() throws Exception {
        LiteralExpression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDate.INSTANCE);
        Expression ceilDateExpression = CeilDateExpression.create(dateLiteral, TimeUnit.DAY);
        
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ceilDateExpression.evaluate(null, ptr);
        Object result = ceilDateExpression.getDataType().toObject(ptr);
        
        assertTrue(result instanceof Date);
        Date resultDate = (Date)result;
        assertEquals(DateUtil.parseDate("2012-01-02 00:00:00"), resultDate);
    }
    
    @Test
    public void testCeilDateExpressionWithMultiplier() throws Exception {
        Expression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDate.INSTANCE);
        Expression ceilDateExpression = CeilDateExpression.create(dateLiteral, TimeUnit.SECOND, 10);
        
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ceilDateExpression.evaluate(null, ptr);
        Object result = ceilDateExpression.getDataType().toObject(ptr);
        
        assertTrue(result instanceof Date);
        Date resultDate = (Date)result;
        assertEquals(DateUtil.parseDate("2012-01-01 14:25:30"), resultDate);
    }
    
    /**
     * Tests {@link RoundDateExpression} constructor check which only allows number of arguments between 2 and 3.
     */
    @Test
    public void testRoundDateExpressionValidation_1() throws Exception {
        LiteralExpression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDate.INSTANCE);
        
        List<Expression> childExpressions = new ArrayList<Expression>(1);
        childExpressions.add(dateLiteral);
        
        try {
            RoundDateExpression.create(childExpressions);
            fail("Instantiating a RoundDateExpression with only one argument should have failed.");
        } catch(IllegalArgumentException e) {

        }
    }
    
    /**
     * Tests {@link RoundDateExpression} constructor for a valid value of time unit.
     */
    @Test
    public void testRoundDateExpressionValidation_2() throws Exception {
        LiteralExpression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDate.INSTANCE);
        LiteralExpression timeUnitLiteral = LiteralExpression.newConstant("millis", PVarchar.INSTANCE);
        
        List<Expression> childExpressions = new ArrayList<Expression>(1);
        childExpressions.add(dateLiteral);
        childExpressions.add(timeUnitLiteral);
        
        try {
            RoundDateExpression.create(childExpressions);
            fail("Only a valid time unit represented by TimeUnit enum is allowed and millis is invalid.");
        } catch(IllegalArgumentException e) {

        }
    }

}
