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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import org.apache.phoenix.expression.function.CeilDateExpression;
import org.apache.phoenix.expression.function.CeilDecimalExpression;
import org.apache.phoenix.expression.function.FloorDateExpression;
import org.apache.phoenix.expression.function.FloorDecimalExpression;
import org.apache.phoenix.expression.function.RoundDateExpression;
import org.apache.phoenix.expression.function.RoundDecimalExpression;
import org.apache.phoenix.expression.function.TimeUnit;
import org.apache.phoenix.schema.IllegalDataException;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.util.DateUtil;

/**
 * 
 * Unit tests for {@link RoundDecimalExpression}, {@link FloorDecimalExpression}
 * and {@link CeilDecimalExpression}.
 *
 * 
 * @since 3.0.0
 */
public class RoundFloorCeilExpressionsUnitTests {

    @Test
    public void testRoundDecimalExpression() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(1.23898, PDataType.DECIMAL);
        Expression rde = RoundDecimalExpression.create(bd, 3);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof BigDecimal);
        BigDecimal value = (BigDecimal)obj;
        assertEquals(BigDecimal.valueOf(1.239), value);
    }
    
    @Test
    public void testCeilDecimalExpression() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(1.23898, PDataType.DECIMAL);
        Expression rde = CeilDecimalExpression.create(bd, 3);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof BigDecimal);
        BigDecimal value = (BigDecimal)obj;
        assertEquals(BigDecimal.valueOf(1.239), value);
    }
    
    @Test
    public void testCeilDecimalExpressionNoop() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(5, PDataType.INTEGER);
        Expression rde = CeilDecimalExpression.create(bd, 3);
        assertEquals(rde, bd);
    }
    
    @Test
    public void testFloorDecimalExpressionNoop() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(5, PDataType.INTEGER);
        Expression rde = FloorDecimalExpression.create(bd, 3);
        assertEquals(rde, bd);
    }
    
    @Test
    public void testRoundDecimalExpressionNoop() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(5, PDataType.INTEGER);
        Expression rde = RoundDecimalExpression.create(bd, 3);
        assertEquals(rde, bd);
    }
    
    @Test
    public void testFloorDecimalExpression() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(1.23898, PDataType.DECIMAL);
        Expression rde = FloorDecimalExpression.create(bd, 3);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof BigDecimal);
        BigDecimal value = (BigDecimal)obj;
        assertEquals(BigDecimal.valueOf(1.238), value);
    }
    
    @Test
    public void testRoundDecimalExpressionScaleParamValidation() throws Exception {
        LiteralExpression bd = LiteralExpression.newConstant(1.23898, PDataType.DECIMAL);
        LiteralExpression scale = LiteralExpression.newConstant("3", PDataType.VARCHAR);
        List<Expression> exprs = new ArrayList<Expression>(2);
        exprs.add(bd);
        exprs.add(scale);
        try {
            new RoundDecimalExpression(exprs);
            fail("Evaluation should have failed because only an INTEGER is allowed for second param in a RoundDecimalExpression");
        } catch(IllegalDataException e) {

        }
    }
    
    @Test
    public void testRoundDateExpression() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        Expression rde = RoundDateExpression.create(date, TimeUnit.DAY);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-02 00:00:00"), value);
    }
    
    @Test
    public void testRoundDateExpressionWithMultiplier() throws Exception {
        Expression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        Expression rde = RoundDateExpression.create(date, TimeUnit.MINUTE, 10);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-01 14:30:00"), value);
    }
    
    @Test
    public void testCeilDateExpression() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        Expression rde = CeilDateExpression.create(date, TimeUnit.DAY);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-02 00:00:00"), value);
    }
    
    @Test
    public void testCeilDateExpressionWithMultiplier() throws Exception {
        Expression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        Expression rde = CeilDateExpression.create(date, TimeUnit.SECOND, 10);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-01 14:25:30"), value);
    }
    
    @Test
    public void testFloorDateExpression() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        Expression rde = FloorDateExpression.create(date, TimeUnit.DAY);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-01 00:00:00"), value);
    }
    
    @Test
    public void testFloorDateExpressionWithMultiplier() throws Exception {
        Expression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        Expression rde = FloorDateExpression.create(date, TimeUnit.SECOND, 10);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        rde.evaluate(null, ptr);
        Object obj = rde.getDataType().toObject(ptr);
        assertTrue(obj instanceof Date);
        Date value = (Date)obj;
        assertEquals(DateUtil.parseDate("2012-01-01 14:25:20"), value);
    }
    
    /**
     * Tests {@link RoundDateExpression} constructor check which only allows number of arguments between 2 and 3.
     */
    @Test
    public void testRoundDateExpressionValidation_1() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        List<Expression> exprs = new ArrayList<Expression>(1);
        exprs.add(date);
        try {
            RoundDateExpression.create(exprs);
            fail("Instantiating a RoundDateExpression with only one argument should have failed.");
        } catch(IllegalArgumentException e) {

        }
    }
    
    /**
     * Tests {@link RoundDateExpression} constructor for a valid value of time unit.
     */
    @Test
    public void testRoundDateExpressionValidation_2() throws Exception {
        LiteralExpression date = LiteralExpression.newConstant(DateUtil.parseDate("2012-01-01 14:25:28"), PDataType.DATE);
        LiteralExpression timeUnit = LiteralExpression.newConstant("millis", PDataType.VARCHAR);
        List<Expression> exprs = new ArrayList<Expression>(1);
        exprs.add(date);
        exprs.add(timeUnit);
        try {
            RoundDateExpression.create(exprs);
            fail("Only a valid time unit represented by TimeUnit enum is allowed and millis is invalid.");
        } catch(IllegalArgumentException e) {

        }
    }

}
