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

import static org.apache.phoenix.util.TestUtil.TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.compile.KeyPart;
import org.apache.phoenix.expression.function.CeilDateExpression;
import org.apache.phoenix.expression.function.CeilDecimalExpression;
import org.apache.phoenix.expression.function.CeilMonthExpression;
import org.apache.phoenix.expression.function.CeilWeekExpression;
import org.apache.phoenix.expression.function.CeilYearExpression;
import org.apache.phoenix.expression.function.FloorDateExpression;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.FloorMonthExpression;
import org.apache.phoenix.expression.function.FloorWeekExpression;
import org.apache.phoenix.expression.function.FloorYearExpression;
import org.apache.phoenix.expression.function.RoundDateExpression;
import org.apache.phoenix.expression.function.RoundDecimalExpression;
import org.apache.phoenix.expression.function.RoundMonthExpression;
import org.apache.phoenix.expression.function.RoundWeekExpression;
import org.apache.phoenix.expression.function.RoundYearExpression;
import org.apache.phoenix.expression.function.ScalarFunction;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.hbase.index.util.ImmutableBytesPtr;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.BaseConnectionlessQueryTest;
import org.apache.phoenix.query.KeyRange;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDate;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PInteger;
import org.apache.phoenix.schema.types.PVarchar;
import org.apache.phoenix.util.DateUtil;
import org.apache.phoenix.util.PropertiesUtil;
import org.joda.time.Chronology;
import org.joda.time.chrono.GJChronology;
import org.junit.Test;

/**
 *
 * Unit tests for {@link RoundDecimalExpression}, {@link FloorDecimalExpression}
 * and {@link CeilDecimalExpression}.
 *
 *
 * @since 3.0.0
 */
public class RoundFloorCeilExpressionsTest extends BaseConnectionlessQueryTest {


    private static long HALF_SEC = 500;
    private static long SEC = 2 * HALF_SEC;

    private static long HALF_MIN = 30 * 1000;
    private static long MIN = 2 * HALF_MIN;

    private static long HALF_HOUR = 30 * 60 * 1000;
    private static long HOUR = 2 * HALF_HOUR;

    private static long HALF_DAY = 12 * 60 * 60 * 1000;
    private static long DAY = 2 * HALF_DAY;

    private static long HALF_WEEK = 7 * 12 * 60 * 60 * 1000;
    private static long WEEK = 2 * HALF_WEEK;

    // Note that without the "l" the integer arithmetic below would overflow
    private static long HALF_YEAR = 365l * 12 * 60 * 60 * 1000;
    private static long YEAR = 2l * HALF_YEAR;

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
    public void testRoundNegativePrecisionDecimalExpression() throws Exception {
        LiteralExpression decimalLiteral = LiteralExpression.newConstant(444.44, PDecimal.INSTANCE);
        Expression roundDecimalExpression = RoundDecimalExpression.create(decimalLiteral, -2);

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        roundDecimalExpression.evaluate(null, ptr);
        Object result = roundDecimalExpression.getDataType().toObject(ptr);

        assertTrue(result instanceof BigDecimal);
        BigDecimal resultDecimal = (BigDecimal)result;
        assertEquals(0, BigDecimal.valueOf(400).compareTo(resultDecimal));
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
        KeyPart baseKeyPart = getDecimalKeyPart();
        ScalarFunction roundDecimalExpression = (ScalarFunction)RoundDecimalExpression.create(DUMMY_DECIMAL, 3);

        byte[] upperBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.2385"));
        byte[] lowerBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.2375"));
        KeyRange expectedKeyRange = KeyRange.getKeyRange(lowerBound, upperBound);

        KeyPart keyPart = roundDecimalExpression.newKeyPart(baseKeyPart);
        assertEquals(expectedKeyRange, keyPart.getKeyRange(CompareOp.EQUAL, LiteralExpression.newConstant(new BigDecimal("1.238"), PDecimal.INSTANCE)));
    }

    @Test
    public void testFloorDecimalExpressionKeyRangeSimple() throws Exception {
        KeyPart baseKeyPart = getDecimalKeyPart();
        ScalarFunction floorDecimalExpression = (ScalarFunction)FloorDecimalExpression.create(DUMMY_DECIMAL, 3);

        byte[] upperBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.239"));
        byte[] lowerBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.238"));
        KeyRange expectedKeyRange = KeyRange.getKeyRange(lowerBound, true, upperBound, false);

        KeyPart keyPart = floorDecimalExpression.newKeyPart(baseKeyPart);
        assertEquals(expectedKeyRange, keyPart.getKeyRange(CompareOp.EQUAL, LiteralExpression.newConstant(new BigDecimal("1.238"), PDecimal.INSTANCE)));
    }

    @Test
    public void testCeilDecimalExpressionKeyRangeSimple() throws Exception {
        KeyPart baseKeyPart = getDecimalKeyPart();
        ScalarFunction ceilDecimalExpression = (ScalarFunction)CeilDecimalExpression.create(DUMMY_DECIMAL, 3);

        byte[] upperBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.238"));
        byte[] lowerBound = PDecimal.INSTANCE.toBytes(new BigDecimal("1.237"));
        KeyRange expectedKeyRange = KeyRange.getKeyRange(lowerBound, false, upperBound, true);

        KeyPart keyPart = ceilDecimalExpression.newKeyPart(baseKeyPart);
        assertEquals(expectedKeyRange, keyPart.getKeyRange(CompareOp.EQUAL, LiteralExpression.newConstant(new BigDecimal("1.238"), PDecimal.INSTANCE)));
    }

    // KeyRange complex / generated tests

    @Test
    public void testRoundDecimalExpressionKeyRangeCoverage() throws Exception {
        KeyPart baseKeyPart = getDecimalKeyPart();
        for(int scale : SCALES) {
            ScalarFunction roundDecimalExpression = (ScalarFunction) RoundDecimalExpression.create(DUMMY_DECIMAL, scale);
            KeyPart keyPart = roundDecimalExpression.newKeyPart(baseKeyPart);
            verifyKeyPart(RoundingType.ROUND, scale, keyPart);
        }
    }

    private static KeyPart getDecimalKeyPart() throws SQLException {
        String tableName = generateUniqueName();
        try (PhoenixConnection pconn = DriverManager.getConnection(getUrl(), PropertiesUtil.deepCopy(TEST_PROPERTIES)).unwrap(PhoenixConnection.class)) {
            pconn.createStatement().execute("CREATE TABLE " + tableName + " (k DECIMAL PRIMARY KEY)");
            final PTable table = pconn.getMetaDataCache().getTableRef(new PTableKey(null, tableName)).getTable();
            KeyPart baseKeyPart = new KeyPart() {
    
                @Override
                public KeyRange getKeyRange(CompareOp op, Expression rhs) {
                    return KeyRange.EVERYTHING_RANGE;
                }
    
                @Override
                public Set<Expression> getExtractNodes() {
                    return Collections.emptySet();
                }
    
                @Override
                public PColumn getColumn() {
                    return table.getPKColumns().get(0);
                }
    
                @Override
                public PTable getTable() {
                    return table;
                }
            };
            return baseKeyPart;
        }
    }
    
    @Test
    public void testFloorDecimalExpressionKeyRangeCoverage() throws Exception {
        KeyPart baseKeyPart = getDecimalKeyPart();
        for(int scale : SCALES) {
            ScalarFunction floorDecimalExpression = (ScalarFunction) FloorDecimalExpression.create(DUMMY_DECIMAL, scale);
            KeyPart keyPart = floorDecimalExpression.newKeyPart(baseKeyPart);
            verifyKeyPart(RoundingType.FLOOR, scale, keyPart);
        }
    }

    @Test
    public void testCeilDecimalExpressionKeyRangeCoverage() throws Exception {
        KeyPart baseKeyPart = getDecimalKeyPart();
        for(int scale : SCALES) {
            ScalarFunction ceilDecimalExpression = (ScalarFunction) CeilDecimalExpression.create(DUMMY_DECIMAL, scale);
            KeyPart keyPart = ceilDecimalExpression.newKeyPart(baseKeyPart);
            verifyKeyPart(RoundingType.CEIL, scale, keyPart);
        }
    }

    /**
     * Represents the three different types of rounding expression and produces
     * expressions of their type when given a Decimal key and scale.
     */
    private enum RoundingType {
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
    private enum Relation {
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
    
    @Test
    public void testFloorDateExpressionForWeek() throws Exception {
        Expression dateLiteral = LiteralExpression.newConstant(DateUtil.parseDate("2016-01-07 08:17:28"), PDate.INSTANCE);
        Expression floorDateExpression = FloorDateExpression.create(dateLiteral, TimeUnit.WEEK);
        
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        floorDateExpression.evaluate(null, ptr);
        Object result = floorDateExpression.getDataType().toObject(ptr);
        
        assertTrue(result instanceof Date);
        Date resultDate = (Date)result;
        assertEquals(DateUtil.parseDate("2016-01-04 00:00:00"), resultDate);
    }

    private RoundDateExpression getRoundMsExpression(String s, TimeUnit u, int m) throws SQLException {
        return (RoundDateExpression)RoundDateExpression.create(LiteralExpression.newConstant(s), u, m );
    }

    // The three tests below are backported from PHOENIX-5066.
    // When PHOENIX-5066 lands, these can be removed as redundant.

    @Test
    public void testRoundingGMT() throws SQLException {
        // We operate on Instants for time units up to Days, simply counting millis

        RoundDateExpression oddWholeSecondExp =
                getRoundMsExpression("2022-11-11 11:11:11", TimeUnit.SECOND, 1);
        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime() - HALF_SEC;
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime() + HALF_SEC - 1;
        assertEquals(lowerBoundaryOddWholeSecond,
            oddWholeSecondExp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecond,
            oddWholeSecondExp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(lowerBoundaryOddWholeSecond)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(lowerBoundaryOddWholeSecond - 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(upperBoundaryOddWholeSecond)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(upperBoundaryOddWholeSecond + 1)));

        // 10 sec range
        RoundDateExpression oddWholeSecondRound10Exp =
                getRoundMsExpression("2022-11-11 11:11:10", TimeUnit.SECOND, 10);
        java.sql.Timestamp oddWholeSecondRound10 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:10").getTime());
        long lowerBoundaryOddWholeSecondRound10 = oddWholeSecondRound10.getTime() - 5 * SEC;
        long upperBoundaryOddWholeSecondRound10 = oddWholeSecondRound10.getTime() + 5 * SEC - 1;
        assertEquals(lowerBoundaryOddWholeSecondRound10,
            oddWholeSecondRound10Exp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecondRound10,
            oddWholeSecondRound10Exp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecondRound10, new java.sql.Timestamp(
                oddWholeSecondRound10Exp.roundTime(lowerBoundaryOddWholeSecondRound10)));
        assertNotEquals(oddWholeSecondRound10, new java.sql.Timestamp(
                oddWholeSecondRound10Exp.roundTime(lowerBoundaryOddWholeSecondRound10 - 1)));
        assertEquals(oddWholeSecondRound10, new java.sql.Timestamp(
                oddWholeSecondRound10Exp.roundTime(upperBoundaryOddWholeSecondRound10)));
        assertNotEquals(oddWholeSecondRound10, new java.sql.Timestamp(
                oddWholeSecondRound10Exp.roundTime(upperBoundaryOddWholeSecondRound10 + 1)));

        // 15 sec range
        RoundDateExpression oddWholeSecondRound15Exp =
                getRoundMsExpression("2022-11-11 11:11:15", TimeUnit.SECOND, 15);
        java.sql.Timestamp oddWholeSecondRound15 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:15").getTime());
        long lowerBoundaryOddWholeSecondRound15 = oddWholeSecondRound15.getTime() - 15 * HALF_SEC;
        long upperBoundaryOddWholeSecondRound15 =
                oddWholeSecondRound15.getTime() + 15 * HALF_SEC - 1;
        assertEquals(lowerBoundaryOddWholeSecondRound15,
            oddWholeSecondRound15Exp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecondRound15,
            oddWholeSecondRound15Exp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecondRound15, new java.sql.Timestamp(
                oddWholeSecondRound15Exp.roundTime(lowerBoundaryOddWholeSecondRound15)));
        assertNotEquals(oddWholeSecondRound15, new java.sql.Timestamp(
                oddWholeSecondRound15Exp.roundTime(lowerBoundaryOddWholeSecondRound15 - 1)));
        assertEquals(oddWholeSecondRound15, new java.sql.Timestamp(
                oddWholeSecondRound15Exp.roundTime(upperBoundaryOddWholeSecondRound15)));
        assertNotEquals(oddWholeSecondRound15, new java.sql.Timestamp(
                oddWholeSecondRound15Exp.roundTime(upperBoundaryOddWholeSecondRound15 + 1)));

        RoundDateExpression evenWholeSecondExp =
                getRoundMsExpression("2022-11-11 11:11:12", TimeUnit.SECOND, 1);
        java.sql.Timestamp evenWholeSecond =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:12").getTime());
        long lowerBoundaryEvenWholeSecond = evenWholeSecond.getTime() - HALF_SEC;
        long upperBoundaryEvenWholeSecond = evenWholeSecond.getTime() + HALF_SEC - 1;
        assertEquals(lowerBoundaryEvenWholeSecond,
            evenWholeSecondExp.rangeLower(evenWholeSecond.getTime()));
        assertEquals(upperBoundaryEvenWholeSecond,
            evenWholeSecondExp.rangeUpper(evenWholeSecond.getTime()));
        assertEquals(evenWholeSecond,
            new java.sql.Timestamp(evenWholeSecondExp.roundTime(lowerBoundaryEvenWholeSecond)));
        assertNotEquals(evenWholeSecond,
            new java.sql.Timestamp(evenWholeSecondExp.roundTime(lowerBoundaryEvenWholeSecond - 1)));
        assertEquals(evenWholeSecond,
            new java.sql.Timestamp(evenWholeSecondExp.roundTime(upperBoundaryEvenWholeSecond)));
        assertNotEquals(evenWholeSecond,
            new java.sql.Timestamp(evenWholeSecondExp.roundTime(upperBoundaryEvenWholeSecond + 1)));

        RoundDateExpression oddWholeMinuteExp =
                getRoundMsExpression("2022-11-11 11:11:0", TimeUnit.MINUTE, 1);
        java.sql.Timestamp oddWholeMinute =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:0").getTime());
        long lowerBoundaryOddWholeMinute = oddWholeMinute.getTime() - HALF_MIN;
        long upperBoundaryOddWholeMinute = oddWholeMinute.getTime() + HALF_MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute,
            oddWholeMinuteExp.rangeLower(oddWholeMinute.getTime()));
        assertEquals(upperBoundaryOddWholeMinute,
            oddWholeMinuteExp.rangeUpper(oddWholeMinute.getTime()));
        assertEquals(oddWholeMinute,
            new java.sql.Timestamp(oddWholeMinuteExp.roundTime(lowerBoundaryOddWholeMinute)));
        assertNotEquals(oddWholeMinute,
            new java.sql.Timestamp(oddWholeMinuteExp.roundTime(lowerBoundaryOddWholeMinute - 1)));
        assertEquals(oddWholeMinute,
            new java.sql.Timestamp(oddWholeMinuteExp.roundTime(upperBoundaryOddWholeMinute)));
        assertNotEquals(oddWholeMinute,
            new java.sql.Timestamp(oddWholeMinuteExp.roundTime(upperBoundaryOddWholeMinute + 1)));

        RoundDateExpression oddWholeMinuteRound20Exp =
                getRoundMsExpression("2022-11-11 11:20:0", TimeUnit.MINUTE, 20);
        java.sql.Timestamp oddWholeMinuteRound20 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:20:0").getTime());
        long lowerBoundaryOddWholeMinute20 = oddWholeMinuteRound20.getTime() - 10 * MIN;
        long upperBoundaryOddWholeMinute20 = oddWholeMinuteRound20.getTime() + 10 * MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute20,
            oddWholeMinuteRound20Exp.rangeLower(oddWholeMinute.getTime()));
        assertEquals(upperBoundaryOddWholeMinute20,
            oddWholeMinuteRound20Exp.rangeUpper(oddWholeMinute.getTime()));
        assertEquals(oddWholeMinuteRound20, new java.sql.Timestamp(
                oddWholeMinuteRound20Exp.roundTime(lowerBoundaryOddWholeMinute20)));
        assertNotEquals(oddWholeMinuteRound20, new java.sql.Timestamp(
                oddWholeMinuteRound20Exp.roundTime(lowerBoundaryOddWholeMinute20 - 1)));
        assertEquals(oddWholeMinuteRound20, new java.sql.Timestamp(
                oddWholeMinuteRound20Exp.roundTime(upperBoundaryOddWholeMinute20)));
        assertNotEquals(oddWholeMinuteRound20, new java.sql.Timestamp(
                oddWholeMinuteRound20Exp.roundTime(upperBoundaryOddWholeMinute20 + 1)));

        // Minutes since epoch, don't expect the rounded value to be "round"

        RoundDateExpression oddWholeMinuteRound17Exp =
                getRoundMsExpression("2022-11-11 11:12:0", TimeUnit.MINUTE, 17);
        java.sql.Timestamp oddWholeMinuteRound17 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryOddWholeMinute17 = oddWholeMinuteRound17.getTime() - 17 * HALF_MIN;
        long upperBoundaryOddWholeMinute17 = oddWholeMinuteRound17.getTime() + 17 * HALF_MIN - 1;
        assertEquals(lowerBoundaryOddWholeMinute17,
            oddWholeMinuteRound17Exp.rangeLower(oddWholeMinute.getTime()));
        assertEquals(upperBoundaryOddWholeMinute17,
            oddWholeMinuteRound17Exp.rangeUpper(oddWholeMinute.getTime()));
        assertEquals(oddWholeMinuteRound17, new java.sql.Timestamp(
                oddWholeMinuteRound17Exp.roundTime(lowerBoundaryOddWholeMinute17)));
        assertNotEquals(oddWholeMinuteRound17, new java.sql.Timestamp(
                oddWholeMinuteRound17Exp.roundTime(lowerBoundaryOddWholeMinute17 - 1)));
        assertEquals(oddWholeMinuteRound17, new java.sql.Timestamp(
                oddWholeMinuteRound17Exp.roundTime(upperBoundaryOddWholeMinute17)));
        assertNotEquals(oddWholeMinuteRound17, new java.sql.Timestamp(
                oddWholeMinuteRound17Exp.roundTime(upperBoundaryOddWholeMinute17 + 1)));

        RoundDateExpression evenWholeMinuteExp =
                getRoundMsExpression("2022-11-11 11:12:0", TimeUnit.MINUTE, 1);
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime() - HALF_MIN;
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime() + HALF_MIN - 1;
        assertEquals(lowerBoundaryEvenWholeMinute,
            evenWholeMinuteExp.rangeLower(evenWholeMinute.getTime()));
        assertEquals(upperBoundaryEvenWholeMinute,
            evenWholeMinuteExp.rangeUpper(evenWholeMinute.getTime()));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(lowerBoundaryEvenWholeMinute)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(lowerBoundaryEvenWholeMinute - 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(upperBoundaryEvenWholeMinute)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(upperBoundaryEvenWholeMinute + 1)));

        RoundDateExpression oddWholeHourExp =
                getRoundMsExpression("2022-11-11 11:0:0", TimeUnit.HOUR, 1);
        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime() - HALF_HOUR;
        long upperBoundaryOddWholeHour = oddWholeHour.getTime() + HALF_HOUR - 1;
        assertEquals(lowerBoundaryOddWholeHour, oddWholeHourExp.rangeLower(oddWholeHour.getTime()));
        assertEquals(upperBoundaryOddWholeHour, oddWholeHourExp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(lowerBoundaryOddWholeHour)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(lowerBoundaryOddWholeHour - 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(upperBoundaryOddWholeHour)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(upperBoundaryOddWholeHour + 1)));

        // Not rounding to hourOfDay
        RoundDateExpression oddWholeHour10Exp =
                getRoundMsExpression("2022-11-11 12:0:0", TimeUnit.HOUR, 10);
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime() - HALF_HOUR * 10;
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime() + HALF_HOUR * 10 - 1;
        assertEquals(lowerBoundaryOddWholeHour10,
            oddWholeHour10Exp.rangeLower(oddWholeHour.getTime()));
        assertEquals(upperBoundaryOddWholeHour10,
            oddWholeHour10Exp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(lowerBoundaryOddWholeHour10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(lowerBoundaryOddWholeHour10 - 1)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(upperBoundaryOddWholeHour10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(upperBoundaryOddWholeHour10 + 1)));

        // Not rounding to hourOfDay
        RoundDateExpression oddWholeHour11Exp =
                getRoundMsExpression("2022-11-11 07:0:0", TimeUnit.HOUR, 11);
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 07:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime() - HALF_HOUR * 11;
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime() + HALF_HOUR * 11 - 1;
        assertEquals(lowerBoundaryOddWholeHour11,
            oddWholeHour11Exp.rangeLower(oddWholeHour.getTime()));
        assertEquals(upperBoundaryOddWholeHour11,
            oddWholeHour11Exp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(lowerBoundaryOddWholeHour11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(lowerBoundaryOddWholeHour11 - 1)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(upperBoundaryOddWholeHour11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(upperBoundaryOddWholeHour11 + 1)));

        RoundDateExpression evenwholeHourExp =
                getRoundMsExpression("2022-11-11 12:0:0", TimeUnit.HOUR, 1);
        java.sql.Timestamp evenwholeHour =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryEvenWholeHour = evenwholeHour.getTime() - HALF_HOUR;
        long upperBoundaryEvenWholeHour = evenwholeHour.getTime() + HALF_HOUR - 1;
        assertEquals(lowerBoundaryEvenWholeHour,
            evenwholeHourExp.rangeLower(evenwholeHour.getTime()));
        assertEquals(upperBoundaryEvenWholeHour,
            evenwholeHourExp.rangeUpper(evenwholeHour.getTime()));
        assertEquals(evenwholeHour,
            new java.sql.Timestamp(evenwholeHourExp.roundTime(lowerBoundaryEvenWholeHour)));
        assertNotEquals(evenwholeHour,
            new java.sql.Timestamp(evenwholeHourExp.roundTime(lowerBoundaryEvenWholeHour - 1)));
        assertEquals(evenwholeHour,
            new java.sql.Timestamp(evenwholeHourExp.roundTime(upperBoundaryEvenWholeHour)));
        assertNotEquals(evenwholeHour,
            new java.sql.Timestamp(evenwholeHourExp.roundTime(upperBoundaryEvenWholeHour + 1)));

        // No DST switchover
        RoundDateExpression oddWholeDayExp =
                getRoundMsExpression("2022-11-11 0:0:0", TimeUnit.DAY, 1);
        java.sql.Timestamp oddWholeDay =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 0:0:0").getTime());
        long lowerBoundaryOddWholeDay = oddWholeDay.getTime() - HALF_DAY;
        long upperBoundaryOddWholeDay = oddWholeDay.getTime() + HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay, oddWholeDayExp.rangeLower(oddWholeDay.getTime()));
        assertEquals(upperBoundaryOddWholeDay, oddWholeDayExp.rangeUpper(oddWholeDay.getTime()));
        assertEquals(oddWholeDay,
            new java.sql.Timestamp(oddWholeDayExp.roundTime(lowerBoundaryOddWholeDay)));
        assertNotEquals(oddWholeDay,
            new java.sql.Timestamp(oddWholeDayExp.roundTime(lowerBoundaryOddWholeDay - 1)));
        assertEquals(oddWholeDay,
            new java.sql.Timestamp(oddWholeDayExp.roundTime(upperBoundaryOddWholeDay)));
        assertNotEquals(oddWholeDay,
            new java.sql.Timestamp(oddWholeDayExp.roundTime(upperBoundaryOddWholeDay + 1)));

        RoundDateExpression oddWholeDay10Exp =
                getRoundMsExpression("2022-11-14 0:0:0", TimeUnit.DAY, 10);
        java.sql.Timestamp oddWholeDay10 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-14 0:0:0").getTime());
        long lowerBoundaryOddWholeDay10 = oddWholeDay10.getTime() - 10 * HALF_DAY;
        long upperBoundaryOddWholeDay10 = oddWholeDay10.getTime() + 10 * HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay10,
            oddWholeDay10Exp.rangeLower(oddWholeDay.getTime()));
        assertEquals(upperBoundaryOddWholeDay10,
            oddWholeDay10Exp.rangeUpper(oddWholeDay.getTime()));
        assertEquals(oddWholeDay10,
            new java.sql.Timestamp(oddWholeDay10Exp.roundTime(lowerBoundaryOddWholeDay10)));
        assertNotEquals(oddWholeDay10,
            new java.sql.Timestamp(oddWholeDay10Exp.roundTime(lowerBoundaryOddWholeDay10 - 1)));
        assertEquals(oddWholeDay10,
            new java.sql.Timestamp(oddWholeDay10Exp.roundTime(upperBoundaryOddWholeDay10)));
        assertNotEquals(oddWholeDay10,
            new java.sql.Timestamp(oddWholeDay10Exp.roundTime(upperBoundaryOddWholeDay10 + 1)));

        RoundDateExpression oddWholeDay3Exp =
                getRoundMsExpression("2022-11-12 0:0:0", TimeUnit.DAY, 3);
        java.sql.Timestamp oddWholeDay3 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryOddWholeDay3 = oddWholeDay3.getTime() - 3 * HALF_DAY;
        long upperBoundaryOddWholeDay3 = oddWholeDay3.getTime() + 3 * HALF_DAY - 1;
        assertEquals(lowerBoundaryOddWholeDay3, oddWholeDay3Exp.rangeLower(oddWholeDay.getTime()));
        assertEquals(upperBoundaryOddWholeDay3, oddWholeDay3Exp.rangeUpper(oddWholeDay.getTime()));
        assertEquals(oddWholeDay3,
            new java.sql.Timestamp(oddWholeDay3Exp.roundTime(lowerBoundaryOddWholeDay3)));
        assertNotEquals(oddWholeDay3,
            new java.sql.Timestamp(oddWholeDay3Exp.roundTime(lowerBoundaryOddWholeDay3 - 1)));
        assertEquals(oddWholeDay3,
            new java.sql.Timestamp(oddWholeDay3Exp.roundTime(upperBoundaryOddWholeDay3)));
        assertNotEquals(oddWholeDay3,
            new java.sql.Timestamp(oddWholeDay3Exp.roundTime(upperBoundaryOddWholeDay3 + 1)));

        RoundDateExpression evenWholeDayExp =
                getRoundMsExpression("2022-11-12 0:0:0", TimeUnit.DAY, 1);
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime() - HALF_DAY;
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime() + HALF_DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay, evenWholeDayExp.rangeLower(evenWholeDay.getTime()));
        assertEquals(upperBoundaryEvenWholeDay, evenWholeDayExp.rangeUpper(evenWholeDay.getTime()));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(lowerBoundaryEvenWholeDay)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(lowerBoundaryEvenWholeDay - 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(upperBoundaryEvenWholeDay)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(upperBoundaryEvenWholeDay + 1)));

        // Stateless, we can reuse it for every week test
        RoundWeekExpression roundWeekExpression = new RoundWeekExpression();
        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(DateUtil.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime() - (HALF_WEEK - 1);
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime() + HALF_WEEK - 1;
        assertEquals(lowerBoundaryWholeWeekOdd,
            roundWeekExpression.rangeLower(wholeWeekOdd.getTime()));
        assertEquals(upperBoundaryWholeWeekOdd,
            roundWeekExpression.rangeUpper(wholeWeekOdd.getTime()));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(roundWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeWeekOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(roundWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeWeekOdd - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(roundWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeWeekOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(roundWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeWeekOdd + 1, GJChronology.getInstanceUTC()))));

        java.sql.Timestamp wholeWeekEven =
                new java.sql.Timestamp(DateUtil.parseDate("2022-10-17 0:0:0").getTime());
        long lowerBoundaryWholeWeekEven = wholeWeekEven.getTime() - HALF_WEEK;
        long upperBoundaryWholeWeekEven = wholeWeekEven.getTime() + HALF_WEEK;
        assertEquals(lowerBoundaryWholeWeekEven,
            roundWeekExpression.rangeLower(wholeWeekEven.getTime()));
        assertEquals(upperBoundaryWholeWeekEven,
            roundWeekExpression.rangeUpper(wholeWeekEven.getTime()));
        assertEquals(wholeWeekEven,
            new java.sql.Timestamp(roundWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeWeekEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeWeekEven,
            new java.sql.Timestamp(roundWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeWeekEven - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeWeekEven,
            new java.sql.Timestamp(roundWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeWeekEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeWeekEven,
            new java.sql.Timestamp(roundWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeWeekEven + 1, GJChronology.getInstanceUTC()))));

        RoundMonthExpression roundMonthExpression = new RoundMonthExpression();
        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeMonthEven =
                new java.sql.Timestamp(DateUtil.parseDate("2022-06-1 0:0:0").getTime());
        // May is 31 days
        long lowerBoundaryWholeMonthEven = wholeMonthEven.getTime() - 31 * HALF_DAY;
        // June is 30 days
        long upperBoundaryWholeMonthEven = wholeMonthEven.getTime() + 30 * HALF_DAY;
        assertEquals(lowerBoundaryWholeMonthEven,
            roundMonthExpression.rangeLower(wholeMonthEven.getTime()));
        assertEquals(upperBoundaryWholeMonthEven,
            roundMonthExpression.rangeUpper(wholeMonthEven.getTime()));
        assertEquals(wholeMonthEven,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthEven,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthEven - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeMonthEven,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthEven,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthEven + 1, GJChronology.getInstanceUTC()))));

        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(DateUtil.parseDate("2022-07-1 0:0:0").getTime());
        // June is 30 days
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime() - 30 * HALF_DAY + 1;
        // July is 31 days
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime() + 31 * HALF_DAY - 1;
        assertEquals(lowerBoundaryWholeMonthOdd,
            roundMonthExpression.rangeLower(wholeMonthOdd.getTime()));
        assertEquals(upperBoundaryWholeMonthOdd,
            roundMonthExpression.rangeUpper(wholeMonthOdd.getTime()));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthOdd - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthOdd + 1, GJChronology.getInstanceUTC()))));

        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(DateUtil.parseDate("2024-02-1 0:0:0").getTime());
        // January is 31 days
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime() - 31 * HALF_DAY;
        // February is 29 days
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime() + 29 * HALF_DAY;
        assertEquals(lowerBoundaryWholeMonthLeap,
            roundMonthExpression.rangeLower(wholeMonthLeap.getTime()));
        assertEquals(upperBoundaryWholeMonthLeap,
            roundMonthExpression.rangeUpper(wholeMonthLeap.getTime()));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthLeap, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthLeap - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthLeap, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(roundMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthLeap + 1, GJChronology.getInstanceUTC()))));

        // We're still using roundHalfEven here for backwards compatibility
        RoundYearExpression roundYearExpression = new RoundYearExpression();
        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(DateUtil.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime() - HALF_YEAR;
        long upperBoundaryWholeYearEven = wholeYearEven.getTime() + HALF_YEAR;
        assertEquals(lowerBoundaryWholeYearEven,
            roundYearExpression.rangeLower(wholeYearEven.getTime()));
        assertEquals(upperBoundaryWholeYearEven,
            roundYearExpression.rangeUpper(wholeYearEven.getTime()));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearEven - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearEven + 1, GJChronology.getInstanceUTC()))));

        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeYearOdd =
                new java.sql.Timestamp(DateUtil.parseDate("2023-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearOdd = wholeYearOdd.getTime() - HALF_YEAR + 1;
        long upperBoundaryWholeYearOdd = wholeYearOdd.getTime() + HALF_YEAR - 1;
        assertEquals(lowerBoundaryWholeYearOdd,
            roundYearExpression.rangeLower(wholeYearOdd.getTime()));
        assertEquals(upperBoundaryWholeYearOdd,
            roundYearExpression.rangeUpper(wholeYearOdd.getTime()));
        assertEquals(wholeYearOdd,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearOdd,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearOdd - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeYearOdd,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearOdd,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearOdd + 1, GJChronology.getInstanceUTC()))));

        // We're still using roundHalfEven here for backwards compatibility
        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(DateUtil.parseDate("2024-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() - HALF_YEAR;
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() + HALF_YEAR + HALF_DAY;
        assertEquals(lowerBoundaryWholeYearLeapEven,
            roundYearExpression.rangeLower(wholeYearLeapEven.getTime()));
        assertEquals(upperBoundaryWholeYearLeapEven,
            roundYearExpression.rangeUpper(wholeYearLeapEven.getTime()));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearLeapEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearLeapEven - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearLeapEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(roundYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearLeapEven + 1, GJChronology.getInstanceUTC()))));
    }

    private FloorDateExpression getFloorMsExpression(String s, TimeUnit u, int m)
            throws SQLException {
        return (FloorDateExpression) FloorDateExpression.create(LiteralExpression.newConstant(s), u,
            m);
    }

    @Test
    public void testFloorGMT() throws SQLException {

        // No need to repeat odd / even cases
        // The logic for upper and lower scan ranges is always
        // [floor(ts), ceil(ts+1)-1]

        RoundDateExpression oddWholeSecondExp =
                getFloorMsExpression("2022-11-11 11:11:11", TimeUnit.SECOND, 1);
        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime();
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime() + SEC - 1;
        assertEquals(lowerBoundaryOddWholeSecond,
            oddWholeSecondExp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecond,
            oddWholeSecondExp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(lowerBoundaryOddWholeSecond)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(lowerBoundaryOddWholeSecond - 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(upperBoundaryOddWholeSecond)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(upperBoundaryOddWholeSecond + 1)));

        // 10 sec range
        RoundDateExpression oddWholeSecondFloor10Exp =
                getFloorMsExpression("2022-11-11 11:11:10", TimeUnit.SECOND, 10);
        java.sql.Timestamp oddWholeSecondFloor10 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:10").getTime());
        long lowerBoundaryOddWholeSecondFloor10 = oddWholeSecondFloor10.getTime();
        long upperBoundaryOddWholeSecondFloor10 = oddWholeSecondFloor10.getTime() + 10 * SEC - 1;
        assertEquals(lowerBoundaryOddWholeSecondFloor10,
            oddWholeSecondFloor10Exp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecondFloor10,
            oddWholeSecondFloor10Exp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecondFloor10, new java.sql.Timestamp(
                oddWholeSecondFloor10Exp.roundTime(lowerBoundaryOddWholeSecondFloor10)));
        assertNotEquals(oddWholeSecondFloor10, new java.sql.Timestamp(
                oddWholeSecondFloor10Exp.roundTime(lowerBoundaryOddWholeSecondFloor10 - 1)));
        assertEquals(oddWholeSecondFloor10, new java.sql.Timestamp(
                oddWholeSecondFloor10Exp.roundTime(upperBoundaryOddWholeSecondFloor10)));
        assertNotEquals(oddWholeSecondFloor10, new java.sql.Timestamp(
                oddWholeSecondFloor10Exp.roundTime(upperBoundaryOddWholeSecondFloor10 + 1)));

        // 15 sec range
        RoundDateExpression oddWholeSecondFloor15Exp =
                getFloorMsExpression("2022-11-11 11:11:0", TimeUnit.SECOND, 15);
        java.sql.Timestamp oddWholeSecondFloor15 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:0").getTime());
        long lowerBoundaryOddWholeSecondFloor15 = oddWholeSecondFloor15.getTime();
        long upperBoundaryOddWholeSecondFloor15 = oddWholeSecondFloor15.getTime() + 15 * SEC - 1;
        assertEquals(lowerBoundaryOddWholeSecondFloor15,
            oddWholeSecondFloor15Exp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecondFloor15,
            oddWholeSecondFloor15Exp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecondFloor15, new java.sql.Timestamp(
                oddWholeSecondFloor15Exp.roundTime(lowerBoundaryOddWholeSecondFloor15)));
        assertNotEquals(oddWholeSecondFloor15, new java.sql.Timestamp(
                oddWholeSecondFloor15Exp.roundTime(lowerBoundaryOddWholeSecondFloor15 - 1)));
        assertEquals(oddWholeSecondFloor15, new java.sql.Timestamp(
                oddWholeSecondFloor15Exp.roundTime(upperBoundaryOddWholeSecondFloor15)));
        assertNotEquals(oddWholeSecondFloor15, new java.sql.Timestamp(
                oddWholeSecondFloor15Exp.roundTime(upperBoundaryOddWholeSecondFloor15 + 1)));

        RoundDateExpression evenWholeMinuteExp =
                getFloorMsExpression("2022-11-11 11:12:0", TimeUnit.MINUTE, 1);
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime();
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime() + MIN - 1;
        assertEquals(lowerBoundaryEvenWholeMinute,
            evenWholeMinuteExp.rangeLower(evenWholeMinute.getTime()));
        assertEquals(upperBoundaryEvenWholeMinute,
            evenWholeMinuteExp.rangeUpper(evenWholeMinute.getTime()));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(lowerBoundaryEvenWholeMinute)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(lowerBoundaryEvenWholeMinute - 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(upperBoundaryEvenWholeMinute)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(upperBoundaryEvenWholeMinute + 1)));

        RoundDateExpression evenWholeMinuteFloor20Exp =
                getFloorMsExpression("2022-11-11 11:00:0", TimeUnit.MINUTE, 20);
        java.sql.Timestamp evenWholeMinuteFloor20 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:00:0").getTime());
        long lowerBoundaryEvenWholeMinuteFloor20 = evenWholeMinuteFloor20.getTime();
        long upperBoundaryEvenWholeMinuteFloor20 = evenWholeMinuteFloor20.getTime() + 20 * MIN - 1;
        assertEquals(lowerBoundaryEvenWholeMinuteFloor20,
            evenWholeMinuteFloor20Exp.rangeLower(evenWholeMinute.getTime()));
        assertEquals(upperBoundaryEvenWholeMinuteFloor20,
            evenWholeMinuteFloor20Exp.rangeUpper(evenWholeMinute.getTime()));
        assertEquals(evenWholeMinuteFloor20, new java.sql.Timestamp(
                evenWholeMinuteFloor20Exp.roundTime(lowerBoundaryEvenWholeMinuteFloor20)));
        assertNotEquals(evenWholeMinuteFloor20, new java.sql.Timestamp(
                evenWholeMinuteFloor20Exp.roundTime(lowerBoundaryEvenWholeMinuteFloor20 - 1)));
        assertEquals(evenWholeMinuteFloor20, new java.sql.Timestamp(
                evenWholeMinuteFloor20Exp.roundTime(upperBoundaryEvenWholeMinuteFloor20)));
        assertNotEquals(evenWholeMinuteFloor20, new java.sql.Timestamp(
                evenWholeMinuteFloor20Exp.roundTime(upperBoundaryEvenWholeMinuteFloor20 + 1)));

        // Minutes since epoch, don't expect the rounded value to be "round"
        RoundDateExpression evenWholeMinuteFloor17Exp =
                getFloorMsExpression("2022-11-11 11:12:00", TimeUnit.MINUTE, 17);
        java.sql.Timestamp evenWholeMinuteFloor17 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryEvenWholeMinute17 = evenWholeMinuteFloor17.getTime();
        long upperBoundaryEvenWholeMinute17 = evenWholeMinuteFloor17.getTime() + 17 * MIN - 1;
        assertEquals(lowerBoundaryEvenWholeMinute17,
            evenWholeMinuteFloor17Exp.rangeLower(evenWholeMinute.getTime()));
        assertEquals(upperBoundaryEvenWholeMinute17,
            evenWholeMinuteFloor17Exp.rangeUpper(evenWholeMinute.getTime()));
        assertEquals(evenWholeMinuteFloor17, new java.sql.Timestamp(
                evenWholeMinuteFloor17Exp.roundTime(lowerBoundaryEvenWholeMinute17)));
        assertNotEquals(evenWholeMinuteFloor17, new java.sql.Timestamp(
                evenWholeMinuteFloor17Exp.roundTime(lowerBoundaryEvenWholeMinute17 - 1)));
        assertEquals(evenWholeMinuteFloor17, new java.sql.Timestamp(
                evenWholeMinuteFloor17Exp.roundTime(upperBoundaryEvenWholeMinute17)));
        assertNotEquals(evenWholeMinuteFloor17, new java.sql.Timestamp(
                evenWholeMinuteFloor17Exp.roundTime(upperBoundaryEvenWholeMinute17 + 1)));

        RoundDateExpression oddWholeHourExp =
                getFloorMsExpression("2022-11-11 11:0:0", TimeUnit.HOUR, 1);
        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime();
        long upperBoundaryOddWholeHour = oddWholeHour.getTime() + HOUR - 1;
        assertEquals(lowerBoundaryOddWholeHour, oddWholeHourExp.rangeLower(oddWholeHour.getTime()));
        assertEquals(upperBoundaryOddWholeHour, oddWholeHourExp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(lowerBoundaryOddWholeHour)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(lowerBoundaryOddWholeHour - 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(upperBoundaryOddWholeHour)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(upperBoundaryOddWholeHour + 1)));

        // Not rounding to hourOfDay
        RoundDateExpression oddWholeHour10Exp =
                getFloorMsExpression("2022-11-11 02:0:0", TimeUnit.HOUR, 10);
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 02:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime();
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime() + HOUR * 10 - 1;
        assertEquals(lowerBoundaryOddWholeHour10,
            oddWholeHour10Exp.rangeLower(oddWholeHour.getTime()));
        assertEquals(upperBoundaryOddWholeHour10,
            oddWholeHour10Exp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(lowerBoundaryOddWholeHour10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(lowerBoundaryOddWholeHour10 - 1)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(upperBoundaryOddWholeHour10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(upperBoundaryOddWholeHour10 + 1)));

        // Not rounding to hourOfDay
        RoundDateExpression oddWholeHour11Exp =
                getFloorMsExpression("2022-11-11 07:0:0", TimeUnit.HOUR, 11);
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 07:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime();
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime() + HOUR * 11 - 1;
        assertEquals(lowerBoundaryOddWholeHour11,
            oddWholeHour11Exp.rangeLower(oddWholeHour.getTime()));
        assertEquals(upperBoundaryOddWholeHour11,
            oddWholeHour11Exp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(lowerBoundaryOddWholeHour11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(lowerBoundaryOddWholeHour11 - 1)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(upperBoundaryOddWholeHour11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(upperBoundaryOddWholeHour11 + 1)));

        // No DST switchover
        RoundDateExpression evenWholeDayExp =
                getFloorMsExpression("2022-11-12 0:0:0", TimeUnit.DAY, 1);
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime();
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime() + DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay, evenWholeDayExp.rangeLower(evenWholeDay.getTime()));
        assertEquals(upperBoundaryEvenWholeDay, evenWholeDayExp.rangeUpper(evenWholeDay.getTime()));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(lowerBoundaryEvenWholeDay)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(lowerBoundaryEvenWholeDay - 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(upperBoundaryEvenWholeDay)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(upperBoundaryEvenWholeDay + 1)));

        RoundDateExpression evenWholeDay2Exp =
                getFloorMsExpression("2022-11-12 0:0:0", TimeUnit.DAY, 2);
        java.sql.Timestamp evenWholeDay2 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay2 = evenWholeDay2.getTime();
        long upperBoundaryEvenWholeDay2 = evenWholeDay2.getTime() + 2 * DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay2,
            evenWholeDay2Exp.rangeLower(evenWholeDay.getTime()));
        assertEquals(upperBoundaryEvenWholeDay2,
            evenWholeDay2Exp.rangeUpper(evenWholeDay.getTime()));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(evenWholeDay2Exp.roundTime(lowerBoundaryEvenWholeDay2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(evenWholeDay2Exp.roundTime(lowerBoundaryEvenWholeDay2 - 1)));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(evenWholeDay2Exp.roundTime(upperBoundaryEvenWholeDay2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(evenWholeDay2Exp.roundTime(upperBoundaryEvenWholeDay2 + 1)));

        RoundDateExpression evenWholeDay3Exp =
                getFloorMsExpression("2022-11-12 0:0:0", TimeUnit.DAY, 3);
        java.sql.Timestamp evenWholeDay3 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay3 = evenWholeDay3.getTime();
        long upperBoundaryEvenWholeDay3 = evenWholeDay3.getTime() + 3 * DAY - 1;
        assertEquals(lowerBoundaryEvenWholeDay3,
            evenWholeDay3Exp.rangeLower(evenWholeDay.getTime()));
        assertEquals(upperBoundaryEvenWholeDay3,
            evenWholeDay3Exp.rangeUpper(evenWholeDay.getTime()));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(evenWholeDay3Exp.roundTime(lowerBoundaryEvenWholeDay3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(evenWholeDay3Exp.roundTime(lowerBoundaryEvenWholeDay3 - 1)));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(evenWholeDay3Exp.roundTime(upperBoundaryEvenWholeDay3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(evenWholeDay3Exp.roundTime(upperBoundaryEvenWholeDay3 + 1)));

        FloorWeekExpression floorWeekExpression = new FloorWeekExpression();
        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(DateUtil.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime();
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime() + WEEK - 1;
        assertEquals(lowerBoundaryWholeWeekOdd,
            floorWeekExpression.rangeLower(wholeWeekOdd.getTime()));
        assertEquals(upperBoundaryWholeWeekOdd,
            floorWeekExpression.rangeUpper(wholeWeekOdd.getTime()));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(floorWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeWeekOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(floorWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeWeekOdd - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeWeekOdd,
            new java.sql.Timestamp(floorWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeWeekOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeWeekOdd,
            new java.sql.Timestamp(floorWeekExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeWeekOdd + 1, GJChronology.getInstanceUTC()))));

        FloorMonthExpression floorMonthExpression = new FloorMonthExpression();
        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(DateUtil.parseDate("2022-07-1 0:0:0").getTime());
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime();
        // July is 31 days
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime() + 31 * DAY - 1;
        assertEquals(lowerBoundaryWholeMonthOdd,
            floorMonthExpression.rangeLower(wholeMonthOdd.getTime()));
        assertEquals(upperBoundaryWholeMonthOdd,
            floorMonthExpression.rangeUpper(wholeMonthOdd.getTime()));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(floorMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(floorMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthOdd - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeMonthOdd,
            new java.sql.Timestamp(floorMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthOdd, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(floorMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthOdd + 1, GJChronology.getInstanceUTC()))));

        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(DateUtil.parseDate("2024-02-1 0:0:0").getTime());
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime();
        // February is 29 days
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime() + 29 * DAY - 1;
        assertEquals(lowerBoundaryWholeMonthLeap,
            floorMonthExpression.rangeLower(wholeMonthLeap.getTime()));
        assertEquals(upperBoundaryWholeMonthLeap,
            floorMonthExpression.rangeUpper(wholeMonthLeap.getTime()));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(floorMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthLeap, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(floorMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthLeap - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeMonthLeap,
            new java.sql.Timestamp(floorMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthLeap, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(floorMonthExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthLeap + 1, GJChronology.getInstanceUTC()))));

        FloorYearExpression floorYearExpression = new FloorYearExpression();
        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(DateUtil.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime();
        long upperBoundaryWholeYearEven = wholeYearEven.getTime() + YEAR - 1;
        assertEquals(lowerBoundaryWholeYearEven,
            floorYearExpression.rangeLower(wholeYearEven.getTime()));
        assertEquals(upperBoundaryWholeYearEven,
            floorYearExpression.rangeUpper(wholeYearEven.getTime()));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(floorYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(floorYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearEven - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeYearEven,
            new java.sql.Timestamp(floorYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearEven,
            new java.sql.Timestamp(floorYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearEven + 1, GJChronology.getInstanceUTC()))));

        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(DateUtil.parseDate("2024-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime();
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() + YEAR + DAY - 1;
        assertEquals(lowerBoundaryWholeYearLeapEven,
            floorYearExpression.rangeLower(wholeYearLeapEven.getTime()));
        assertEquals(upperBoundaryWholeYearLeapEven,
            floorYearExpression.rangeUpper(wholeYearLeapEven.getTime()));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(floorYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearLeapEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(floorYearExpression.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearLeapEven - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeYearLeapEven,
            new java.sql.Timestamp(floorYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearLeapEven, GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(floorYearExpression.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearLeapEven + 1, GJChronology.getInstanceUTC()))));
    }

    private CeilDateExpression getCeilMsExpression(String s, TimeUnit u, int m)
            throws SQLException {
        return (CeilDateExpression) CeilDateExpression.create(LiteralExpression.newConstant(s), u,
            m);
    }

    @Test
    public void testCeilGMT() throws SQLException {

        // No need to repeat odd / even cases
        // The logic for upper and lower scan ranges is always
        // [floor(ts-1)+1, ceil(ts)]

        RoundDateExpression oddWholeSecondExp =
                getCeilMsExpression("2022-11-11 11:11:11", TimeUnit.SECOND, 1);
        java.sql.Timestamp oddWholeSecond =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:11").getTime());
        long lowerBoundaryOddWholeSecond = oddWholeSecond.getTime() - SEC + 1;
        long upperBoundaryOddWholeSecond = oddWholeSecond.getTime();
        assertEquals(lowerBoundaryOddWholeSecond,
            oddWholeSecondExp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecond,
            oddWholeSecondExp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(lowerBoundaryOddWholeSecond)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(lowerBoundaryOddWholeSecond - 1)));
        assertEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(upperBoundaryOddWholeSecond)));
        assertNotEquals(oddWholeSecond,
            new java.sql.Timestamp(oddWholeSecondExp.roundTime(upperBoundaryOddWholeSecond + 1)));

        // 10 sec range
        RoundDateExpression oddWholeSecondCeil10Exp =
                getCeilMsExpression("2022-11-11 11:11:20", TimeUnit.SECOND, 10);
        java.sql.Timestamp oddWholeSecondCeil10 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:20").getTime());
        long lowerBoundaryOddWholeSecondCeil10 = oddWholeSecondCeil10.getTime() - 10 * SEC + 1;
        long upperBoundaryOddWholeSecondCeil10 = oddWholeSecondCeil10.getTime();
        assertEquals(lowerBoundaryOddWholeSecondCeil10,
            oddWholeSecondCeil10Exp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecondCeil10,
            oddWholeSecondCeil10Exp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecondCeil10, new java.sql.Timestamp(
                oddWholeSecondCeil10Exp.roundTime(lowerBoundaryOddWholeSecondCeil10)));
        assertNotEquals(oddWholeSecondCeil10, new java.sql.Timestamp(
                oddWholeSecondCeil10Exp.roundTime(lowerBoundaryOddWholeSecondCeil10 - 1)));
        assertEquals(oddWholeSecondCeil10, new java.sql.Timestamp(
                oddWholeSecondCeil10Exp.roundTime(upperBoundaryOddWholeSecondCeil10)));
        assertNotEquals(oddWholeSecondCeil10, new java.sql.Timestamp(
                oddWholeSecondCeil10Exp.roundTime(upperBoundaryOddWholeSecondCeil10 + 1)));

        // 15 sec range
        RoundDateExpression oddWholeSecondCeil15Exp =
                getCeilMsExpression("2022-11-11 11:11:15", TimeUnit.SECOND, 15);
        java.sql.Timestamp oddWholeSecondCeil15 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:11:15").getTime());
        long lowerBoundaryOddWholeSecondFloor15 = oddWholeSecondCeil15.getTime() - 15 * SEC + 1;
        long upperBoundaryOddWholeSecondFloor15 = oddWholeSecondCeil15.getTime();
        assertEquals(lowerBoundaryOddWholeSecondFloor15,
            oddWholeSecondCeil15Exp.rangeLower(oddWholeSecond.getTime()));
        assertEquals(upperBoundaryOddWholeSecondFloor15,
            oddWholeSecondCeil15Exp.rangeUpper(oddWholeSecond.getTime()));
        assertEquals(oddWholeSecondCeil15, new java.sql.Timestamp(
                oddWholeSecondCeil15Exp.roundTime(lowerBoundaryOddWholeSecondFloor15)));
        assertNotEquals(oddWholeSecondCeil15, new java.sql.Timestamp(
                oddWholeSecondCeil15Exp.roundTime(lowerBoundaryOddWholeSecondFloor15 - 1)));
        assertEquals(oddWholeSecondCeil15, new java.sql.Timestamp(
                oddWholeSecondCeil15Exp.roundTime(upperBoundaryOddWholeSecondFloor15)));
        assertNotEquals(oddWholeSecondCeil15, new java.sql.Timestamp(
                oddWholeSecondCeil15Exp.roundTime(upperBoundaryOddWholeSecondFloor15 + 1)));

        RoundDateExpression evenWholeMinuteExp =
                getCeilMsExpression("2022-11-11 11:12:0", TimeUnit.MINUTE, 1);
        java.sql.Timestamp evenWholeMinute =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:12:0").getTime());
        long lowerBoundaryEvenWholeMinute = evenWholeMinute.getTime() - MIN + 1;
        long upperBoundaryEvenWholeMinute = evenWholeMinute.getTime();
        assertEquals(lowerBoundaryEvenWholeMinute,
            evenWholeMinuteExp.rangeLower(evenWholeMinute.getTime()));
        assertEquals(upperBoundaryEvenWholeMinute,
            evenWholeMinuteExp.rangeUpper(evenWholeMinute.getTime()));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(lowerBoundaryEvenWholeMinute)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(lowerBoundaryEvenWholeMinute - 1)));
        assertEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(upperBoundaryEvenWholeMinute)));
        assertNotEquals(evenWholeMinute,
            new java.sql.Timestamp(evenWholeMinuteExp.roundTime(upperBoundaryEvenWholeMinute + 1)));

        RoundDateExpression evenWholeMinuteCeil20Exp =
                getCeilMsExpression("2022-11-11 11:20:0", TimeUnit.MINUTE, 20);
        java.sql.Timestamp evenWholeMinuteCeil20 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:20:0").getTime());
        long lowerBoundaryEvenWholeMinuteCeil20 = evenWholeMinuteCeil20.getTime() - 20 * MIN + 1;
        long upperBoundaryEvenWholeMinuteCeil20 = evenWholeMinuteCeil20.getTime();
        assertEquals(lowerBoundaryEvenWholeMinuteCeil20,
            evenWholeMinuteCeil20Exp.rangeLower(evenWholeMinute.getTime()));
        assertEquals(upperBoundaryEvenWholeMinuteCeil20,
            evenWholeMinuteCeil20Exp.rangeUpper(evenWholeMinute.getTime()));
        assertEquals(evenWholeMinuteCeil20, new java.sql.Timestamp(
                evenWholeMinuteCeil20Exp.roundTime(lowerBoundaryEvenWholeMinuteCeil20)));
        assertNotEquals(evenWholeMinuteCeil20, new java.sql.Timestamp(
                evenWholeMinuteCeil20Exp.roundTime(lowerBoundaryEvenWholeMinuteCeil20 - 1)));
        assertEquals(evenWholeMinuteCeil20, new java.sql.Timestamp(
                evenWholeMinuteCeil20Exp.roundTime(upperBoundaryEvenWholeMinuteCeil20)));
        assertNotEquals(evenWholeMinuteCeil20, new java.sql.Timestamp(
                evenWholeMinuteCeil20Exp.roundTime(upperBoundaryEvenWholeMinuteCeil20 + 1)));

        // Minutes since epoch, don't expect the rounded value to be "round"
        RoundDateExpression evenWholeMinuteCeil17Exp =
                getCeilMsExpression("2022-11-11 11:12:00", TimeUnit.MINUTE, 17);
        java.sql.Timestamp evenWholeMinuteCeil17 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:12:00").getTime());
        long lowerBoundaryEvenWholeMinute17 = evenWholeMinuteCeil17.getTime() - 17 * MIN + 1;
        long upperBoundaryEvenWholeMinute17 = evenWholeMinuteCeil17.getTime();
        assertEquals(lowerBoundaryEvenWholeMinute17,
            evenWholeMinuteCeil17Exp.rangeLower(evenWholeMinute.getTime()));
        assertEquals(upperBoundaryEvenWholeMinute17,
            evenWholeMinuteCeil17Exp.rangeUpper(evenWholeMinute.getTime()));
        assertEquals(evenWholeMinuteCeil17, new java.sql.Timestamp(
                evenWholeMinuteCeil17Exp.roundTime(lowerBoundaryEvenWholeMinute17)));
        assertNotEquals(evenWholeMinuteCeil17, new java.sql.Timestamp(
                evenWholeMinuteCeil17Exp.roundTime(lowerBoundaryEvenWholeMinute17 - 1)));
        assertEquals(evenWholeMinuteCeil17, new java.sql.Timestamp(
                evenWholeMinuteCeil17Exp.roundTime(upperBoundaryEvenWholeMinute17)));
        assertNotEquals(evenWholeMinuteCeil17, new java.sql.Timestamp(
                evenWholeMinuteCeil17Exp.roundTime(upperBoundaryEvenWholeMinute17 + 1)));

        RoundDateExpression oddWholeHourExp =
                getCeilMsExpression("2022-11-11 11:0:0", TimeUnit.HOUR, 1);
        java.sql.Timestamp oddWholeHour =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 11:0:0").getTime());
        long lowerBoundaryOddWholeHour = oddWholeHour.getTime() - HOUR + 1;
        long upperBoundaryOddWholeHour = oddWholeHour.getTime();
        assertEquals(lowerBoundaryOddWholeHour,
            oddWholeHourExp.rangeLower(oddWholeHour.getTime() - 1));
        assertEquals(upperBoundaryOddWholeHour, oddWholeHourExp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(lowerBoundaryOddWholeHour)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(lowerBoundaryOddWholeHour - 1)));
        assertEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(upperBoundaryOddWholeHour)));
        assertNotEquals(oddWholeHour,
            new java.sql.Timestamp(oddWholeHourExp.roundTime(upperBoundaryOddWholeHour + 1)));

        // Not rounding to hourOfDay
        RoundDateExpression oddWholeHour10Exp =
                getCeilMsExpression("2022-11-11 12:0:0", TimeUnit.HOUR, 10);
        java.sql.Timestamp oddWholeHour10 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 12:0:0").getTime());
        long lowerBoundaryOddWholeHour10 = oddWholeHour10.getTime() - 10 * HOUR + 1;
        long upperBoundaryOddWholeHour10 = oddWholeHour10.getTime();
        assertEquals(lowerBoundaryOddWholeHour10,
            oddWholeHour10Exp.rangeLower(oddWholeHour.getTime()));
        assertEquals(upperBoundaryOddWholeHour10,
            oddWholeHour10Exp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(lowerBoundaryOddWholeHour10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(lowerBoundaryOddWholeHour10 - 1)));
        assertEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(upperBoundaryOddWholeHour10)));
        assertNotEquals(oddWholeHour10,
            new java.sql.Timestamp(oddWholeHour10Exp.roundTime(upperBoundaryOddWholeHour10 + 1)));

        // Not rounding to hourOfDay
        RoundDateExpression oddWholeHour11Exp =
                getCeilMsExpression("2022-11-11 12:0:0", TimeUnit.HOUR, 11);
        java.sql.Timestamp oddWholeHour11 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-11 18:0:0").getTime());
        long lowerBoundaryOddWholeHour11 = oddWholeHour11.getTime() - 11 * HOUR + 1;
        long upperBoundaryOddWholeHour11 = oddWholeHour11.getTime();
        assertEquals(lowerBoundaryOddWholeHour11,
            oddWholeHour11Exp.rangeLower(oddWholeHour.getTime()));
        assertEquals(upperBoundaryOddWholeHour11,
            oddWholeHour11Exp.rangeUpper(oddWholeHour.getTime()));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(lowerBoundaryOddWholeHour11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(lowerBoundaryOddWholeHour11 - 1)));
        assertEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(upperBoundaryOddWholeHour11)));
        assertNotEquals(oddWholeHour11,
            new java.sql.Timestamp(oddWholeHour11Exp.roundTime(upperBoundaryOddWholeHour11 + 1)));

        RoundDateExpression evenWholeDayExp =
                getCeilMsExpression("2022-11-12 0:0:0", TimeUnit.DAY, 1);
        java.sql.Timestamp evenWholeDay =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay = evenWholeDay.getTime() - DAY + 1;
        long upperBoundaryEvenWholeDay = evenWholeDay.getTime();
        assertEquals(lowerBoundaryEvenWholeDay, evenWholeDayExp.rangeLower(evenWholeDay.getTime()));
        assertEquals(upperBoundaryEvenWholeDay, evenWholeDayExp.rangeUpper(evenWholeDay.getTime()));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(lowerBoundaryEvenWholeDay)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(lowerBoundaryEvenWholeDay - 1)));
        assertEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(upperBoundaryEvenWholeDay)));
        assertNotEquals(evenWholeDay,
            new java.sql.Timestamp(evenWholeDayExp.roundTime(upperBoundaryEvenWholeDay + 1)));

        RoundDateExpression evenWholeDay2Exp =
                getCeilMsExpression("2022-11-12 0:0:0", TimeUnit.DAY, 2);
        java.sql.Timestamp evenWholeDay2 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay2 = evenWholeDay2.getTime() - 2 * DAY + 1;
        long upperBoundaryEvenWholeDay2 = evenWholeDay2.getTime();
        assertEquals(lowerBoundaryEvenWholeDay2,
            evenWholeDay2Exp.rangeLower(evenWholeDay.getTime()));
        assertEquals(upperBoundaryEvenWholeDay2,
            evenWholeDay2Exp.rangeUpper(evenWholeDay.getTime()));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(evenWholeDay2Exp.roundTime(lowerBoundaryEvenWholeDay2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(evenWholeDay2Exp.roundTime(lowerBoundaryEvenWholeDay2 - 1)));
        assertEquals(evenWholeDay2,
            new java.sql.Timestamp(evenWholeDay2Exp.roundTime(upperBoundaryEvenWholeDay2)));
        assertNotEquals(evenWholeDay2,
            new java.sql.Timestamp(evenWholeDay2Exp.roundTime(upperBoundaryEvenWholeDay2 + 1)));

        RoundDateExpression evenWholeDay3Exp =
                getCeilMsExpression("2022-11-12 0:0:0", TimeUnit.DAY, 3);
        java.sql.Timestamp evenWholeDay3 =
                new java.sql.Timestamp(DateUtil.parseDate("2022-11-12 0:0:0").getTime());
        long lowerBoundaryEvenWholeDay3 = evenWholeDay3.getTime() - 3 * DAY + 1;
        long upperBoundaryEvenWholeDay3 = evenWholeDay3.getTime();
        assertEquals(lowerBoundaryEvenWholeDay3,
            evenWholeDay3Exp.rangeLower(evenWholeDay.getTime()));
        assertEquals(upperBoundaryEvenWholeDay3,
            evenWholeDay3Exp.rangeUpper(evenWholeDay.getTime()));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(evenWholeDay3Exp.roundTime(lowerBoundaryEvenWholeDay3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(evenWholeDay3Exp.roundTime(lowerBoundaryEvenWholeDay3 - 1)));
        assertEquals(evenWholeDay3,
            new java.sql.Timestamp(evenWholeDay3Exp.roundTime(upperBoundaryEvenWholeDay3)));
        assertNotEquals(evenWholeDay3,
            new java.sql.Timestamp(evenWholeDay3Exp.roundTime(upperBoundaryEvenWholeDay3 + 1)));

        CeilWeekExpression ceilWeekExp = new CeilWeekExpression();
        java.sql.Timestamp wholeWeekOdd =
                new java.sql.Timestamp(DateUtil.parseDate("2022-10-10 0:0:0").getTime());
        long lowerBoundaryWholeWeekOdd = wholeWeekOdd.getTime() - WEEK + 1;
        long upperBoundaryWholeWeekOdd = wholeWeekOdd.getTime();
        assertEquals(lowerBoundaryWholeWeekOdd, ceilWeekExp.rangeLower(wholeWeekOdd.getTime()));
        assertEquals(upperBoundaryWholeWeekOdd, ceilWeekExp.rangeUpper(wholeWeekOdd.getTime()));
        assertEquals(wholeWeekOdd, new java.sql.Timestamp(
                ceilWeekExp.roundDateTime(new org.joda.time.DateTime(lowerBoundaryWholeWeekOdd,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeWeekOdd, new java.sql.Timestamp(
                ceilWeekExp.roundDateTime(new org.joda.time.DateTime(lowerBoundaryWholeWeekOdd - 1,
                        GJChronology.getInstanceUTC()))));
        assertEquals(wholeWeekOdd, new java.sql.Timestamp(
                ceilWeekExp.roundDateTime(new org.joda.time.DateTime(upperBoundaryWholeWeekOdd,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeWeekOdd, new java.sql.Timestamp(
                ceilWeekExp.roundDateTime(new org.joda.time.DateTime(upperBoundaryWholeWeekOdd + 1,
                        GJChronology.getInstanceUTC()))));

        CeilMonthExpression ceilMonthExp = new CeilMonthExpression();
        java.sql.Timestamp wholeMonthOdd =
                new java.sql.Timestamp(DateUtil.parseDate("2022-08-1 0:0:0").getTime());
        // July is 31 days
        long lowerBoundaryWholeMonthOdd = wholeMonthOdd.getTime() - 31 * DAY + 1;
        long upperBoundaryWholeMonthOdd = wholeMonthOdd.getTime();
        assertEquals(lowerBoundaryWholeMonthOdd, ceilMonthExp.rangeLower(wholeMonthOdd.getTime()));
        assertEquals(upperBoundaryWholeMonthOdd, ceilMonthExp.rangeUpper(wholeMonthOdd.getTime()));
        assertEquals(wholeMonthOdd, new java.sql.Timestamp(
                ceilMonthExp.roundDateTime(new org.joda.time.DateTime(lowerBoundaryWholeMonthOdd,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(ceilMonthExp.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthOdd - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeMonthOdd, new java.sql.Timestamp(
                ceilMonthExp.roundDateTime(new org.joda.time.DateTime(upperBoundaryWholeMonthOdd,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthOdd,
            new java.sql.Timestamp(ceilMonthExp.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthOdd + 1, GJChronology.getInstanceUTC()))));

        java.sql.Timestamp wholeMonthLeap =
                new java.sql.Timestamp(DateUtil.parseDate("2024-03-1 0:0:0").getTime());
        // February is 29 days
        long lowerBoundaryWholeMonthLeap = wholeMonthLeap.getTime() - 29 * DAY + 1;
        long upperBoundaryWholeMonthLeap = wholeMonthLeap.getTime();
        assertEquals(lowerBoundaryWholeMonthLeap,
            ceilMonthExp.rangeLower(wholeMonthLeap.getTime()));
        assertEquals(upperBoundaryWholeMonthLeap,
            ceilMonthExp.rangeUpper(wholeMonthLeap.getTime()));
        assertEquals(wholeMonthLeap, new java.sql.Timestamp(
                ceilMonthExp.roundDateTime(new org.joda.time.DateTime(lowerBoundaryWholeMonthLeap,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(ceilMonthExp.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeMonthLeap - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeMonthLeap, new java.sql.Timestamp(
                ceilMonthExp.roundDateTime(new org.joda.time.DateTime(upperBoundaryWholeMonthLeap,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeMonthLeap,
            new java.sql.Timestamp(ceilMonthExp.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeMonthLeap + 1, GJChronology.getInstanceUTC()))));

        CeilYearExpression ceilYearExp = new CeilYearExpression();
        java.sql.Timestamp wholeYearEven =
                new java.sql.Timestamp(DateUtil.parseDate("2022-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearEven = wholeYearEven.getTime() - YEAR + 1;
        long upperBoundaryWholeYearEven = wholeYearEven.getTime();
        assertEquals(lowerBoundaryWholeYearEven, ceilYearExp.rangeLower(wholeYearEven.getTime()));
        assertEquals(upperBoundaryWholeYearEven, ceilYearExp.rangeUpper(wholeYearEven.getTime()));
        assertEquals(wholeYearEven, new java.sql.Timestamp(
                ceilYearExp.roundDateTime(new org.joda.time.DateTime(lowerBoundaryWholeYearEven,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearEven, new java.sql.Timestamp(
                ceilYearExp.roundDateTime(new org.joda.time.DateTime(lowerBoundaryWholeYearEven - 1,
                        GJChronology.getInstanceUTC()))));
        assertEquals(wholeYearEven, new java.sql.Timestamp(
                ceilYearExp.roundDateTime(new org.joda.time.DateTime(upperBoundaryWholeYearEven,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearEven, new java.sql.Timestamp(
                ceilYearExp.roundDateTime(new org.joda.time.DateTime(upperBoundaryWholeYearEven + 1,
                        GJChronology.getInstanceUTC()))));

        java.sql.Timestamp wholeYearLeapEven =
                new java.sql.Timestamp(DateUtil.parseDate("2025-1-1 0:0:0").getTime());
        long lowerBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime() - (YEAR + DAY) + 1;
        long upperBoundaryWholeYearLeapEven = wholeYearLeapEven.getTime();
        assertEquals(lowerBoundaryWholeYearLeapEven,
            ceilYearExp.rangeLower(wholeYearLeapEven.getTime()));
        assertEquals(upperBoundaryWholeYearLeapEven,
            ceilYearExp.rangeUpper(wholeYearLeapEven.getTime()));
        assertEquals(wholeYearLeapEven, new java.sql.Timestamp(
                ceilYearExp.roundDateTime(new org.joda.time.DateTime(lowerBoundaryWholeYearLeapEven,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(ceilYearExp.roundDateTime(new org.joda.time.DateTime(
                    lowerBoundaryWholeYearLeapEven - 1, GJChronology.getInstanceUTC()))));
        assertEquals(wholeYearLeapEven, new java.sql.Timestamp(
                ceilYearExp.roundDateTime(new org.joda.time.DateTime(upperBoundaryWholeYearLeapEven,
                        GJChronology.getInstanceUTC()))));
        assertNotEquals(wholeYearLeapEven,
            new java.sql.Timestamp(ceilYearExp.roundDateTime(new org.joda.time.DateTime(
                    upperBoundaryWholeYearLeapEven + 1, GJChronology.getInstanceUTC()))));
    }

}
